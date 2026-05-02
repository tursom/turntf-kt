package io.github.tursom.turntf.kotlin

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentHashMap

/**
 * 基于 Client 的 relay 连接管理器。
 *
 * 负责入站连接分发和出站连接创建。通过 [TurntfClient.relay] 访问。
 *
 * @property client 关联的 turntf 客户端
 */
class Relay(internal val client: TurntfClient) {
    private val lock = Any()
    private val conns = ConcurrentHashMap<String, RelayConnection>()
    private var onConnHandler: ((RelayConnection) -> Unit)? = null

    /**
     * 注册入站 relay 连接的处理器。每个新入站连接会调用 handler。
     *
     * @param handler 入站连接处理器
     */
    fun onConnection(handler: (RelayConnection) -> Unit) {
        synchronized(lock) {
            onConnHandler = handler
        }
    }

    /**
     * 向目标用户发起 relay 连接。
     *
     * 自动解析目标用户的在线会话并选择支持瞬时消息的会话。
     *
     * @param target 目标用户
     * @param config 连接配置，null 时使用默认配置 [RelayConfig]
     * @return 已建立的 relay 连接
     * @throws RelayError 如果目标用户不在线或连接超时
     */
    suspend fun connect(target: UserRef, config: RelayConfig? = null): RelayConnection {
        // 1. 解析目标用户会话
        val sessions = client.resolveUserSessions(target)
        val resolvedSession = sessions.sessions.firstOrNull { it.transientCapable }
            ?: throw RelayError(
                RELAY_ERROR_NOT_CONNECTED,
                "no transient-capable session found for target user"
            )

        val cfg = config ?: RelayConfig()
        val relayId = newRelayId()
        val mySession = client.loginState.value?.sessionRef
            ?: throw IllegalStateException("client not logged in")

        // 2. 创建连接（OPENING 状态）
        val conn = RelayConnection(
            relay = this,
            relayId = relayId,
            config = cfg,
            remotePeer = target,
            remoteSession = resolvedSession.session,
            mySession = mySession,
            initialState = RelayState.OPENING
        )

        conns[relayId] = conn

        // 3. 发送 OPEN 帧
        val openEnv = RelayEnvelope(
            relayId = relayId,
            kind = RelayKind.OPEN,
            senderSession = mySession,
            targetSession = resolvedSession.session,
            sentAtMs = currentTimeMillis()
        )
        try {
            conn.sendRelayEnvelope(openEnv)
        } catch (e: Exception) {
            conns.remove(relayId)
            throw RelayError(RELAY_ERROR_NOT_CONNECTED, "send OPEN failed: ${e.message}")
        }

        // 4. 启动发送循环
        conn.startSendAndRetransmit()

        // 5. 等待 OPEN_ACK（带超时）
        // TimeoutCancellationException 是 CancellationException 的子类，必须优先捕获
        try {
            withTimeout(cfg.openTimeoutMs) {
                conn.awaitOpen()
            }
        } catch (e: TimeoutCancellationException) {
            conn.handleClose(RelayError(RELAY_ERROR_OPEN_TIMEOUT, "OPEN timeout waiting for OPEN_ACK"))
            throw RelayError(RELAY_ERROR_OPEN_TIMEOUT, "OPEN timeout waiting for OPEN_ACK")
        } catch (e: CancellationException) {
            conn.handleClose(RelayError(RELAY_ERROR_OPEN_TIMEOUT, "OPEN cancelled"))
            throw e
        }

        return conn
    }

    /**
     * 处理入站 PACKET_PUSHED 事件。
     *
     * 尝试将 packet body 解码为 relay 帧。如果解码成功则分发到对应连接。
     *
     * @param packet 入站数据包
     * @return true 表示该数据包已被 relay 处理（非用户数据），false 表示无关帧
     */
    fun handlePacket(packet: Packet): Boolean {
        val env = decodeRelayEnvelope(packet.body) ?: return false

        val conn = conns[env.relayId]

        when (env.kind) {
            RelayKind.OPEN -> {
                if (conn == null) {
                    acceptIncoming(env)
                }
                return true
            }

            RelayKind.OPEN_ACK -> {
                if (conn != null && conn.state.value == RelayState.OPENING) {
                    conn.markOpen()
                }
                return true
            }

            RelayKind.CLOSE -> {
                if (conn != null) {
                    conn.handleClose(RelayError(RELAY_ERROR_REMOTE_CLOSE, "remote peer closed connection"))
                }
                return true
            }

            RelayKind.ERROR -> {
                if (conn != null) {
                    val msg = env.payload.decodeToString()
                    conn.handleClose(RelayError(RELAY_ERROR_PROTOCOL, "remote peer error: $msg"))
                }
                return true
            }

            else -> {
                if (conn != null) {
                    conn.handleEnvelope(env)
                }
                return true
            }
        }
    }

    /**
     * 处理入站 OPEN 帧：创建新连接并通知用户处理器。
     */
    private fun acceptIncoming(env: RelayEnvelope) {
        val cfg = RelayConfig()
        val mySession = client.loginState.value?.sessionRef
            ?: return // 未登录，忽略

        val conn = RelayConnection(
            relay = this,
            relayId = env.relayId,
            config = cfg,
            remotePeer = UserRef(0, 0), // OPEN 帧不含 sender userRef，通过 sender session 推断
            remoteSession = env.senderSession,
            mySession = mySession,
            initialState = RelayState.OPENING
        )

        // 并发 OPEN 处理：relay_id 字典序小的保留
        synchronized(lock) {
            val existing = conns[env.relayId]
            if (existing != null) {
                // 已有连接，比较 relay_id
                if (env.relayId < existing.relayId) {
                    // 对端的 relay_id 更小，保留对端连接
                    existing.handleClose(
                        RelayError(RELAY_ERROR_DUPLICATE_OPEN, "concurrent OPEN, keeping lower relay_id")
                    )
                } else {
                    // 本端的 relay_id 更小或相等，保留本端连接
                    conn.handleClose(
                        RelayError(RELAY_ERROR_DUPLICATE_OPEN, "concurrent OPEN, keeping lower relay_id")
                    )
                    return
                }
            }
            conns[env.relayId] = conn
        }

        // 置为 OPEN 并启动发送循环
        conn.markOpen()

        // 在连接作用域内异步回复 OPEN_ACK
        val openAckEnv = RelayEnvelope(
            relayId = env.relayId,
            kind = RelayKind.OPEN_ACK,
            senderSession = conn.mySession,
            targetSession = conn.remoteSession,
            sentAtMs = currentTimeMillis()
        )
        conn.connectionScope.launch {
            try {
                conn.sendRelayEnvelope(openAckEnv)
            } catch (_: Exception) {
                // OPEN_ACK 发送失败忽略
            }
        }

        // 通知用户处理器
        val handler = synchronized(lock) { onConnHandler }
        handler?.invoke(conn)
    }

    /**
     * 从管理器中移除连接。
     */
    internal fun removeConnection(relayId: String) {
        conns.remove(relayId)
    }

    /**
     * 获取指定 relay_id 的连接。
     */
    internal fun getConnection(relayId: String): RelayConnection? = conns[relayId]
}
