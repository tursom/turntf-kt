package io.github.tursom.turntf.kotlin

import io.github.tursom.turntf.kotlin.internal.sessionRefFromProto
import io.github.tursom.turntf.kotlin.internal.sessionRefToProto
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import notifier.relay.v1.RelayProto
import java.security.SecureRandom

// ============================================================
// 类型定义
// ============================================================

/**
 * RelayConnection 的可靠性等级。
 */
enum class Reliability(val wireValue: String) {
    /** 无 ACK，无重传，无去重，无排序。延迟最低，适合实时音视频帧。 */
    BEST_EFFORT("best_effort"),
    /** ACK + 重传，不保证去重和排序。适合幂等指令。 */
    AT_LEAST_ONCE("at_least_once"),
    /** ACK + 重传 + 去重 + 严格有序。适合文件传输和聊天消息。 */
    RELIABLE_ORDERED("reliable_ordered")
}

/**
 * RelayConnection 的当前状态。
 */
enum class RelayState {
    /** 初始状态或已关闭。 */
    CLOSED,
    /** 已发送 OPEN，等待 OPEN_ACK。 */
    OPENING,
    /** 连接已建立，可收发数据。 */
    OPEN,
    /** 已发送 CLOSE，等待确认。 */
    CLOSING
}

/**
 * Relay 协议帧的类型枚举，对应 proto RelayKind。
 */
enum class RelayKind {
    UNSPECIFIED,
    OPEN,
    OPEN_ACK,
    DATA,
    ACK,
    CLOSE,
    PING,
    ERROR
}

/**
 * RelayConnection 的配置。
 *
 * @param reliability 可靠性等级，默认 [Reliability.RELIABLE_ORDERED]
 * @param windowSize 发送窗口大小（在途未确认帧数上限），范围 1-256，默认 16
 * @param openTimeoutMs OPEN 等待 OPEN_ACK 超时毫秒数，默认 10000
 * @param closeTimeoutMs CLOSE 等待确认超时毫秒数，默认 5000
 * @param ackTimeoutMs DATA 等待 ACK 超时毫秒数，默认 3000
 * @param maxRetransmits 最大重传次数，默认 5
 * @param sendTimeoutMs Send 操作超时毫秒数（缓冲区满时等待上限），0 表示不超时，默认 0
 * @param receiveTimeoutMs Receive 操作超时毫秒数（无数据等待上限），0 表示不超时，默认 0
 * @param sendBufferSize 发送缓冲区字节数，默认 65536
 * @param deliveryMode Packet 投递模式，默认 [DeliveryMode.ROUTE_RETRY]
 */
data class RelayConfig(
    val reliability: Reliability = Reliability.RELIABLE_ORDERED,
    val windowSize: Int = 16,
    val openTimeoutMs: Long = 10000,
    val closeTimeoutMs: Long = 5000,
    val ackTimeoutMs: Long = 3000,
    val maxRetransmits: Int = 5,
    val sendTimeoutMs: Long = 0,
    val receiveTimeoutMs: Long = 0,
    val sendBufferSize: Int = 65536,
    val deliveryMode: DeliveryMode = DeliveryMode.ROUTE_RETRY
)

/**
 * Relay 协议帧类型，与 proto RelayEnvelope 对应。
 */
data class RelayEnvelope(
    val relayId: String,
    val kind: RelayKind,
    val senderSession: SessionRef,
    val targetSession: SessionRef,
    val seq: Long = 0,
    val ackSeq: Long = 0,
    val payload: ByteArray = byteArrayOf(),
    val sentAtMs: Long = 0
)

/**
 * Relay 层错误码。
 */
const val RELAY_ERROR_OPEN_TIMEOUT = "open_timeout"
const val RELAY_ERROR_MAX_RETRANSMIT = "max_retransmit"
const val RELAY_ERROR_REMOTE_CLOSE = "remote_close"
const val RELAY_ERROR_CLIENT_CLOSED = "client_closed"
const val RELAY_ERROR_PROTOCOL = "protocol_error"
const val RELAY_ERROR_DUPLICATE_OPEN = "duplicate_open"
const val RELAY_ERROR_NOT_CONNECTED = "not_connected"
const val RELAY_ERROR_SEND_TIMEOUT = "send_timeout"
const val RELAY_ERROR_RECEIVE_TIMEOUT = "receive_timeout"

/**
 * Relay 层的错误。
 */
class RelayError(code: String, message: String) : TurntfException("relay: $code: $message")

/**
 * relay_id 生成器。
 */
private val relayIdRandom = SecureRandom()

/**
 * 生成一个唯一的 relay 连接 ID。
 */
internal fun newRelayId(): String {
    val bytes = ByteArray(16)
    relayIdRandom.nextBytes(bytes)
    return bytes.joinToString("") { "%02x".format(it) }
}

/**
 * 当前时间毫秒数。
 */
internal fun currentTimeMillis(): Long = System.currentTimeMillis()

// ============================================================
// 未确认帧记录
// ============================================================

private data class UnackedFrame(
    val data: ByteArray,
    var retransmitCount: Int = 0
)

// ============================================================
// RelayConnection
// ============================================================

/**
 * 表示一条 relay 点对点连接，提供可靠或尽力而为的数据传输。
 *
 * 连接通过 [Relay.connect] 创建（出站）或由 [Relay.onConnection] 接收（入站）。
 *
 * @property relayId 连接的唯一标识
 * @property remotePeer 对端用户引用
 * @property remoteSession 对端会话引用
 * @property state 连接当前状态
 * @property recvFlow 接收数据流，从中读取对端发送的数据
 */
class RelayConnection internal constructor(
    internal val relay: Relay,
    val relayId: String,
    val config: RelayConfig,
    val remotePeer: UserRef,
    val remoteSession: SessionRef,
    internal val mySession: SessionRef,
    initialState: RelayState = RelayState.CLOSED
) {
    private val lock = Any()

    // ---- 状态 ----
    private val _state = MutableStateFlow(initialState)

    /** 连接当前状态。 */
    val state: StateFlow<RelayState> = _state.asStateFlow()

    // ---- 发送窗口 ----
    private var sendBase = 0L
    private var nextSeq = 1L
    private val unacked = mutableMapOf<Long, UnackedFrame>()
    private var retransCnt = 0

    // ---- 接收窗口（ReliableOrdered） ----
    private var expectedSeq = 1L
    private val recvBuf = mutableMapOf<Long, ByteArray>()

    // ---- 通道 ----
    private val sendCh: Channel<ByteArray> = Channel(maxOf(1, config.sendBufferSize / 1024))
    private val recvCh: Channel<ByteArray> = Channel(64)

    /** 接收数据流。 */
    val recvFlow: Flow<ByteArray> = recvCh.receiveAsFlow()

    // ---- 生命周期信号 ----
    private val openDeferred = CompletableDeferred<Unit>()

    // ---- 协程管理 ----
    @PublishedApi
    internal val connectionScope = CoroutineScope(
        SupervisorJob() + Dispatchers.IO + CoroutineName("relay-conn-$relayId")
    )
    private var sendJob: Job? = null
    private var retransmitJob: Job? = null

    // ---- 回调 ----
    private val onCloseCallbacks = mutableListOf<(Throwable?) -> Unit>()

    // ---- 初始化 ----
    init {
        if (initialState == RelayState.OPEN) {
            // 入站连接初始即为 OPEN 状态
            openDeferred.complete(Unit)
        }
    }

    /**
     * 等待连接建立（收到 OPEN_ACK）。
     */
    suspend fun awaitOpen() {
        openDeferred.await()
    }

    /**
     * 发送数据。行为取决于配置的可靠性等级。
     *
     * @param data 要发送的数据
     * @throws RelayError 如果连接未建立或已关闭
     */
    suspend fun send(data: ByteArray) {
        require(data.isNotEmpty()) { "data must not be empty" }
        if (_state.value != RelayState.OPEN) {
            throw RelayError(RELAY_ERROR_NOT_CONNECTED, "connection is not open, state=${_state.value}")
        }
        if (config.sendTimeoutMs > 0) {
            try {
                withTimeout(config.sendTimeoutMs) {
                    sendCh.send(data)
                }
            } catch (e: TimeoutCancellationException) {
                throw RelayError(RELAY_ERROR_SEND_TIMEOUT, "send timeout after ${config.sendTimeoutMs}ms")
            }
        } else {
            sendCh.send(data)
        }
    }

    /**
     * 从连接接收数据，支持超时。
     *
     * @param timeoutMs 超时毫秒数，0 表示使用 [config.receiveTimeoutMs]，两者均为 0 则无限等待
     * @return 接收到的数据
     * @throws RelayError(RELAY_ERROR_RECEIVE_TIMEOUT) 超时
     */
    suspend fun receiveTimeout(timeoutMs: Long = 0): ByteArray {
        val t = if (timeoutMs > 0) timeoutMs else config.receiveTimeoutMs
        return if (t > 0) {
            try {
                withTimeout(t) {
                    recvCh.receive()
                }
            } catch (e: TimeoutCancellationException) {
                throw RelayError(RELAY_ERROR_RECEIVE_TIMEOUT, "receive timeout after ${t}ms")
            }
        } else {
            recvCh.receive()
        }
    }

    /**
     * 注册连接关闭回调。
     */
    fun onClose(handler: (Throwable?) -> Unit) {
        synchronized(lock) {
            onCloseCallbacks.add(handler)
        }
    }

    /**
     * 优雅关闭连接，发送 CLOSE 帧。
     */
    suspend fun close() {
        synchronized(lock) {
            if (_state.value != RelayState.OPEN) return
            _state.value = RelayState.CLOSING
        }
        val closeEnv = RelayEnvelope(
            relayId = relayId,
            kind = RelayKind.CLOSE,
            senderSession = mySession,
            targetSession = remoteSession,
            sentAtMs = currentTimeMillis()
        )
        try {
            sendRelayEnvelope(closeEnv)
        } catch (_: Exception) {
            // CLOSE 发送失败忽略
        }
        handleClose(null)
    }

    /**
     * 强制关闭连接，不等待确认。
     */
    fun abort(reason: Throwable) {
        handleClose(reason)
    }

    // ============================================================
    // 内部控制方法
    // ============================================================

    /**
     * 启动发送循环和重传定时器。
     */
    internal fun startSendAndRetransmit() {
        if (sendJob?.isActive == true) return
        sendJob = connectionScope.launch {
            try {
                if (config.reliability == Reliability.BEST_EFFORT) {
                    bestEffortSendLoop()
                } else {
                    reliableSendLoop()
                }
            } catch (_: CancellationException) {
                // 正常取消
            } catch (e: Exception) {
                handleClose(e)
            }
        }
        // 重传定时器（仅在可靠模式下）
        if (config.reliability != Reliability.BEST_EFFORT) {
            retransmitJob = connectionScope.launch {
                try {
                    while (isActive) {
                        kotlinx.coroutines.delay(config.ackTimeoutMs)
                        retransmit()
                    }
                } catch (_: CancellationException) {
                    // 正常取消
                }
            }
        }
    }

    /**
     * 尽力而为模式的发送循环。
     */
    private suspend fun bestEffortSendLoop() {
        for (data in sendCh) {
            val env = RelayEnvelope(
                relayId = relayId,
                kind = RelayKind.DATA,
                senderSession = mySession,
                targetSession = remoteSession,
                payload = data,
                sentAtMs = currentTimeMillis()
            )
            sendRelayEnvelope(env)
        }
    }

    /**
     * 可靠模式的发送循环（AtLeastOnce / ReliableOrdered）。
     */
    private suspend fun reliableSendLoop() {
        for (data in sendCh) {
            // 等待发送窗口有空位
            waitForWindowSpace()

            val seq: Long
            synchronized(lock) {
                seq = nextSeq++
                unacked[seq] = UnackedFrame(data = data)
                if (sendBase == 0L) {
                    sendBase = seq
                }
            }

            val env = RelayEnvelope(
                relayId = relayId,
                kind = RelayKind.DATA,
                senderSession = mySession,
                targetSession = remoteSession,
                seq = seq,
                payload = data,
                sentAtMs = currentTimeMillis()
            )
            sendRelayEnvelope(env)
        }
    }

    /**
     * 等待发送窗口有空闲位置。
     * 当窗口满时，触发重传并等待后重试。
     */
    private suspend fun waitForWindowSpace() {
        while (true) {
            synchronized(lock) {
                if (nextSeq - sendBase < config.windowSize) {
                    return // 窗口有空位
                }
            }
            // 窗口满，触发重传并等待
            retransmit()
            kotlinx.coroutines.delay(config.ackTimeoutMs)
        }
    }

    /**
     * 发送 relay 信封到对端。
     */
    internal suspend fun sendRelayEnvelope(env: RelayEnvelope) {
        val body = encodeRelayEnvelope(env)
        val mode = config.deliveryMode
        relay.client.sendPacket(
            SendPacketInput(
                target = remotePeer,
                body = body,
                deliveryMode = mode,
                targetSession = remoteSession
            )
        )
    }

    /**
     * 处理收到的 relay 信封。
     * 此方法在 [Relay.handlePacket] 中调用，运行在 orderedScope 内。
     */
    internal fun handleEnvelope(env: RelayEnvelope) {
        when (env.kind) {
            RelayKind.DATA -> handleData(env)
            RelayKind.ACK -> handleAck(env)
            RelayKind.PING -> handlePing(env)
            else -> { /* 忽略未知类型 */ }
        }
    }

    /**
     * 处理 DATA 帧。
     */
    private fun handleData(env: RelayEnvelope) {
        when (config.reliability) {
            Reliability.BEST_EFFORT -> {
                recvCh.trySend(env.payload)
            }

            Reliability.AT_LEAST_ONCE -> {
                sendAckAsync(env.seq)
                recvCh.trySend(env.payload)
            }

            Reliability.RELIABLE_ORDERED -> {
                val toDeliver = mutableListOf<ByteArray>()
                synchronized(lock) {
                    if (env.seq == expectedSeq) {
                        toDeliver.add(env.payload)
                        expectedSeq++
                        while (true) {
                            val buffered = recvBuf.remove(expectedSeq) ?: break
                            toDeliver.add(buffered)
                            expectedSeq++
                        }
                    } else if (env.seq > expectedSeq) {
                        if (env.seq - expectedSeq < config.windowSize) {
                            recvBuf[env.seq] = env.payload
                        }
                    }
                }
                for (data in toDeliver) {
                    recvCh.trySend(data)
                }
                sendAckAsync(env.seq)
            }
        }
    }

    /**
     * 处理 ACK 帧。
     */
    private fun handleAck(env: RelayEnvelope) {
        if (config.reliability == Reliability.BEST_EFFORT) return
        synchronized(lock) {
            if (env.ackSeq >= sendBase) {
                for (seq in sendBase..env.ackSeq) {
                    unacked.remove(seq)
                }
                sendBase = env.ackSeq + 1
                retransCnt = 0
            }
        }
    }

    /**
     * 处理 PING 帧：回复 ERROR 帧表示不支持 PING。
     */
    private fun handlePing(env: RelayEnvelope) {
        connectionScope.launch {
            val errEnv = RelayEnvelope(
                relayId = relayId,
                kind = RelayKind.ERROR,
                senderSession = mySession,
                targetSession = remoteSession,
                payload = byteArrayOf(),
                sentAtMs = currentTimeMillis()
            )
            try {
                sendRelayEnvelope(errEnv)
            } catch (_: Exception) {
                // 忽略
            }
        }
    }

    /**
     * 异步发送 ACK 帧。
     */
    private fun sendAckAsync(ackSeq: Long) {
        connectionScope.launch {
            val ackEnv = RelayEnvelope(
                relayId = relayId,
                kind = RelayKind.ACK,
                senderSession = mySession,
                targetSession = remoteSession,
                ackSeq = ackSeq,
                sentAtMs = currentTimeMillis()
            )
            try {
                sendRelayEnvelope(ackEnv)
            } catch (_: Exception) {
                // ACK 发送失败忽略
            }
        }
    }

    /**
     * 重传所有未确认的帧。
     */
    internal fun retransmit() {
        synchronized(lock) {
            if (unacked.isEmpty()) return

            retransCnt++
            if (retransCnt > config.maxRetransmits) {
                // 超出最大重传次数，断开连接
                val err = RelayError(RELAY_ERROR_MAX_RETRANSMIT, "max retransmits exceeded")
                synchronized(lock) {
                    if (_state.value == RelayState.CLOSED) return
                    _state.value = RelayState.CLOSED
                }
                notifyClose(err)
                return
            }

            for ((seq, frame) in unacked) {
                frame.retransmitCount++
                connectionScope.launch {
                    val env = RelayEnvelope(
                        relayId = relayId,
                        kind = RelayKind.DATA,
                        senderSession = mySession,
                        targetSession = remoteSession,
                        seq = seq,
                        payload = frame.data,
                        sentAtMs = currentTimeMillis()
                    )
                    try {
                        sendRelayEnvelope(env)
                    } catch (_: Exception) {
                        // 重传发送失败忽略
                    }
                }
            }
        }
    }

    /**
     * 内部通知关闭，不经过 scope 取消链。
     */
    private fun notifyClose(reason: Throwable?) {
        sendJob?.cancel()
        retransmitJob?.cancel()
        sendCh.close()
        relay.removeConnection(relayId)
        val callbacks = synchronized(lock) {
            val list = onCloseCallbacks.toList()
            onCloseCallbacks.clear()
            list
        }
        for (fn in callbacks) {
            try {
                fn(reason)
            } catch (_: Exception) {
                // 忽略回调异常
            }
        }
    }

    /**
     * 处理连接关闭。
     */
    internal fun handleClose(reason: Throwable?) {
        synchronized(lock) {
            if (_state.value == RelayState.CLOSED) return
            _state.value = RelayState.CLOSED
        }
        connectionScope.cancel()
        notifyClose(reason)
    }

    /**
     * 将连接置为 OPEN 并启动发送循环。
     * 在收到 OPEN_ACK（出站）或创建入站连接时调用。
     */
    internal fun markOpen() {
        synchronized(lock) {
            _state.value = RelayState.OPEN
        }
        openDeferred.complete(Unit)
        startSendAndRetransmit()
    }

    override fun toString(): String = "RelayConnection(relayId=$relayId, state=${_state.value})"
}

// ============================================================
// Proto 转换函数
// ============================================================

/**
 * 将 [RelayEnvelope] 编码为 protobuf 字节数组。
 */
internal fun encodeRelayEnvelope(env: RelayEnvelope): ByteArray {
    return RelayProto.RelayEnvelope.newBuilder()
        .setRelayId(env.relayId)
        .setKind(relayKindToProto(env.kind))
        .setSenderSession(sessionRefToProto(env.senderSession))
        .setTargetSession(sessionRefToProto(env.targetSession))
        .setSeq(env.seq)
        .setAckSeq(env.ackSeq)
        .setPayload(com.google.protobuf.ByteString.copyFrom(env.payload))
        .setSentAtMs(env.sentAtMs)
        .build()
        .toByteArray()
}

/**
 * 从 protobuf 字节数组解码 [RelayEnvelope]。
 */
internal fun decodeRelayEnvelope(data: ByteArray): RelayEnvelope? {
    return try {
        val pbEnv = RelayProto.RelayEnvelope.parseFrom(data)
        RelayEnvelope(
            relayId = pbEnv.relayId,
            kind = relayKindFromProto(pbEnv.kind),
            senderSession = sessionRefFromProto(pbEnv.senderSession),
            targetSession = sessionRefFromProto(pbEnv.targetSession),
            seq = pbEnv.seq,
            ackSeq = pbEnv.ackSeq,
            payload = pbEnv.payload.toByteArray(),
            sentAtMs = pbEnv.sentAtMs
        )
    } catch (_: Exception) {
        null
    }
}

/**
 * 将 [RelayKind] 转换为 proto [RelayProto.RelayKind]。
 */
internal fun relayKindToProto(kind: RelayKind): RelayProto.RelayKind = when (kind) {
    RelayKind.OPEN -> RelayProto.RelayKind.RELAY_KIND_OPEN
    RelayKind.OPEN_ACK -> RelayProto.RelayKind.RELAY_KIND_OPEN_ACK
    RelayKind.DATA -> RelayProto.RelayKind.RELAY_KIND_DATA
    RelayKind.ACK -> RelayProto.RelayKind.RELAY_KIND_ACK
    RelayKind.CLOSE -> RelayProto.RelayKind.RELAY_KIND_CLOSE
    RelayKind.PING -> RelayProto.RelayKind.RELAY_KIND_PING
    RelayKind.ERROR -> RelayProto.RelayKind.RELAY_KIND_ERROR
    RelayKind.UNSPECIFIED -> RelayProto.RelayKind.RELAY_KIND_UNSPECIFIED
}

/**
 * 将 proto [RelayProto.RelayKind] 转换为 [RelayKind]。
 */
internal fun relayKindFromProto(kind: RelayProto.RelayKind): RelayKind = when (kind) {
    RelayProto.RelayKind.RELAY_KIND_OPEN -> RelayKind.OPEN
    RelayProto.RelayKind.RELAY_KIND_OPEN_ACK -> RelayKind.OPEN_ACK
    RelayProto.RelayKind.RELAY_KIND_DATA -> RelayKind.DATA
    RelayProto.RelayKind.RELAY_KIND_ACK -> RelayKind.ACK
    RelayProto.RelayKind.RELAY_KIND_CLOSE -> RelayKind.CLOSE
    RelayProto.RelayKind.RELAY_KIND_PING -> RelayKind.PING
    RelayProto.RelayKind.RELAY_KIND_ERROR -> RelayKind.ERROR
    else -> RelayKind.UNSPECIFIED
}
