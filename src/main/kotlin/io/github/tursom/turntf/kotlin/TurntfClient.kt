package io.github.tursom.turntf.kotlin

import io.github.tursom.turntf.kotlin.internal.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import notifier.client.v1.Client
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import okio.ByteString.Companion.toByteString
import java.util.concurrent.TimeoutException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

/**
 * 基于协程的 turntf WebSocket 客户端。
 *
 * 客户端拥有完整的连接生命周期管理，包括：
 * - 登录认证
 * - 自动重连（支持指数退避）
 * - 消息去重（通过已见游标传递）
 * - 请求/响应关联
 * - 消息持久化
 * - 基于 Flow 的事件发布
 *
 * 公开的挂起函数仅在 SDK 完成相应协议帧所需的记账工作后才会返回。
 * 例如，[sendMessage] 会在消息被本地持久化后才返回。
 *
 * @param config 客户端配置，包含服务器地址、凭据、重连策略等
 *
 * @see Config
 * @see ClientEvent
 * @see ConnectionState
 */
class TurntfClient(config: Config) {
    companion object {
        private const val CLIENT_PROTOCOL_VERSION = "client-v1alpha5"
        private const val CLOSED_MESSAGE = "turntf client is closed"
        private const val NOT_CONNECTED_MESSAGE = "turntf client is not connected"
        private const val DISCONNECTED_MESSAGE = "turntf websocket disconnected"
    }

    private val config = normalize(config)
    private val httpClient: OkHttpClient = this.config.httpClient

    /**
     * HTTP 客户端，用于执行 REST API 请求。
     *
     * 当需要通过 HTTP 而非 WebSocket 进行操作时使用。
     * 与当前 WebSocket 客户端共享相同的配置。
     *
     * @see TurntfHttpClient
     */
    val http = TurntfHttpClient(this.config.baseUrl, httpClient)

    /**
     * Relay 连接管理器，提供用户间的点对点实时传输通道。
     *
     * 支持三种可靠性模式：BestEffort、AtLeastOnce、ReliableOrdered。
     * 入站连接通过 [Relay.onConnection] 注册处理器接收。
     *
     * @see Relay
     * @see RelayConnection
     */
    val relay = Relay(this)

    private val runtimeDispatcher: CoroutineDispatcher = Dispatchers.IO
    // 所有经过认证的协议帧通过单一线程处理，以确保消息持久化、
    // ACK 发送、待处理 RPC 完成和事件发布按服务器发送顺序执行。
    private val orderedDispatcher: ExecutorCoroutineDispatcher = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "turntf-kt-events").apply { isDaemon = true }
    }.asCoroutineDispatcher()
    private val scope = CoroutineScope(SupervisorJob() + runtimeDispatcher)
    private val orderedScope = CoroutineScope(SupervisorJob() + orderedDispatcher)

    private val _events = MutableSharedFlow<ClientEvent>(extraBufferCapacity = 64)
    private val _loginState = MutableStateFlow<LoginInfo?>(null)
    private val _connectionState = MutableStateFlow(ConnectionState.DISCONNECTED)

    /**
     * 实时事件流，用于接收客户端生命周期事件。
     *
     * 包括登录成功、消息接收、数据包接收、错误和断开连接等事件。
     * 使用 SharedFlow 实现，支持多个收集者。
     *
     * @see ClientEvent
     */
    val events: SharedFlow<ClientEvent> = _events.asSharedFlow()

    /**
     * 当前登录状态。
     *
     * 当客户端连接并认证成功后更新为 [LoginInfo]，
     * 断开连接或关闭后更新为 `null`。
     */
    val loginState: StateFlow<LoginInfo?> = _loginState.asStateFlow()

    /**
     * 当前连接状态。
     *
     * 反映 WebSocket 连接的生命周期：[DISCONNECTED] -> [CONNECTING] -> [CONNECTED] -> [CLOSED]。
     * 自动重连过程中会循环回到 [CONNECTING]。
     *
     * @see ConnectionState
     */
    val connectionState: StateFlow<ConnectionState> = _connectionState.asStateFlow()

    // 请求/响应 RPC 完全通过 request_id 关联；断开连接和超时必须主动
    // 使这些 CompletableDeferred 失败，因为 WebSocket 回调不会自动执行此操作。
    private val pending = ConcurrentHashMap<Long, CompletableDeferred<Any>>()
    private val requestId = AtomicLong()
    private val stateLock = Any()

    @Volatile
    private var webSocket: WebSocket? = null

    @Volatile
    private var authenticated = false

    @Volatile
    private var stopReconnect = false

    @Volatile
    private var closed = false

    @Volatile
    private var managerJob: Job? = null

    @Volatile
    private var pingJob: Job? = null

    @Volatile
    // connect() 通过此 CompletableDeferred 等待当前连接尝试完成。
    // 每次启动新的管理循环时都会重新创建，以避免调用者意外地等待陈旧的登录成功。
    private var firstConnect = CompletableDeferred<Unit>()

    /**
     * 启动 WebSocket 连接生命周期，并挂起直到第一个经过身份认证的会话准备就绪。
     *
     * 如果客户端已经连接，则立即返回。
     * 如果客户端已关闭，则抛出 [IllegalStateException]。
     *
     * @throws IllegalStateException 如果客户端已关闭
     * @throws ServerError 如果登录被服务器拒绝
     * @throws ConnectionError 如果网络连接失败
     */
    suspend fun connect() {
        synchronized(stateLock) {
            check(!closed) { CLOSED_MESSAGE }
            if (authenticated) {
                return
            }
            if (managerJob?.isActive != true) {
                firstConnect = CompletableDeferred()
                managerJob = scope.launch { runLoop() }
            }
        }
        firstConnect.await()
    }

    /**
     * 停止重连尝试，关闭当前 WebSocket 连接，并使所有待处理的 RPC 请求失败。
     *
     * 调用后客户端不再可用。所有正在等待的 [connect] 和 RPC 调用将抛出异常。
     */
    suspend fun close() {
        synchronized(stateLock) {
            if (closed) {
                return
            }
            closed = true
            _connectionState.value = ConnectionState.CLOSED
            webSocket?.close(1000, "client close")
            webSocket = null
        }
        cancelPing()
        failAllPending(IllegalStateException(CLOSED_MESSAGE))
        managerJob?.cancel()
        orderedScope.cancel()
        scope.cancel()
        orderedDispatcher.close()
    }

    /**
     * 通过 HTTP 进行登录（委托给 [TurntfHttpClient.login]）。
     *
     * 使用传统方式：节点 ID + 用户 ID + 明文密码。
     * 密码会在本地进行 bcrypt 哈希再传输。
     *
     * @param nodeId 节点 ID
     * @param userId 用户 ID
     * @param password 明文密码
     * @return 登录令牌（JWT 字符串）
     * @see http
     */
    suspend fun login(nodeId: Long, userId: Long, password: String): String = http.login(nodeId, userId, password)

    /**
     * 通过 HTTP 进行登录（委托给 [TurntfHttpClient.login]）。
     *
     * 使用登录名方式。密码会在本地进行 bcrypt 哈希再传输。
     *
     * @param loginName 登录名
     * @param password 明文密码
     * @return 登录令牌（JWT 字符串）
     * @see http
     */
    suspend fun login(loginName: String, password: String): String = http.login(loginName, password)

    /**
     * 通过 HTTP 进行登录（委托给 [TurntfHttpClient.loginWithPassword]）。
     *
     * 使用传统方式，但接受已包装的密码输入。
     *
     * @param nodeId 节点 ID
     * @param userId 用户 ID
     * @param password 密码输入（包装后）
     * @return 登录令牌（JWT 字符串）
     * @see loginWithPassword
     */
    suspend fun loginWithPassword(nodeId: Long, userId: Long, password: PasswordInput): String = http.loginWithPassword(nodeId, userId, password)

    /**
     * 通过 HTTP 进行登录（委托给 [TurntfHttpClient.loginWithPassword]）。
     *
     * 使用登录名方式，但接受已包装的密码输入。
     *
     * @param loginName 登录名
     * @param password 密码输入（包装后）
     * @return 登录令牌（JWT 字符串）
     */
    suspend fun loginWithPassword(loginName: String, password: PasswordInput): String = http.loginWithPassword(loginName, password)

    /**
     * 通过 WebSocket RPC 通道发送应用层心跳。
     *
     * 用于保持连接活跃和检测网络中断。
     * 心跳由 [Config.pingInterval] 控制自动发送，一般不需要手动调用。
     *
     * @throws IllegalStateException 如果客户端未连接
     * @throws TimeoutException 如果请求超时
     */
    suspend fun ping() {
        rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setPing(Client.Ping.newBuilder().setRequestId(requestId).build())
                    .build()
            },
            mapper = { }
        )
    }

    /**
     * 发送持久化消息。
     *
     * 消息会被服务端持久化存储，在接收者离线时暂存，上线后投递。
     * 方法会在消息被本地持久化到 [Config.cursorStore] 后返回。
     *
     * @param input 发送消息的输入参数（目标用户和消息体）
     * @return 服务端回显的持久化消息，包含分配的消息游标信息
     * @throws IllegalArgumentException 如果目标用户无效或消息体为空
     * @throws TimeoutException 如果请求超时
     */
    suspend fun sendMessage(input: SendMessageInput): Message {
        validateUserRef(input.target, "target")
        require(input.body.isNotEmpty()) { "body is required" }
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setSendMessage(
                        Client.SendMessageRequest.newBuilder()
                            .setRequestId(requestId)
                            .setTarget(userRefToProto(input.target))
                            .setBody(com.google.protobuf.ByteString.copyFrom(input.body))
                            .setDeliveryKind(Client.ClientDeliveryKind.CLIENT_DELIVERY_KIND_PERSISTENT)
                            .setDeliveryMode(Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_UNSPECIFIED)
                            .setSyncMode(Client.ClientMessageSyncMode.CLIENT_MESSAGE_SYNC_MODE_UNSPECIFIED)
                            .build()
                    )
                    .build()
            },
            mapper = { value -> value as? Message ?: throw ProtocolError("missing message in send_message_response") }
        )
    }

    /**
     * 发送瞬时数据包，可选地指定目标在线会话。
     *
     * 瞬时数据包不会被持久化，仅在接收者当前在线时才能送达。
     * 适用于实时通信场景。
     *
     * @param input 发送数据包的输入参数（目标用户、消息体、投递模式和可选的目标会话）
     * @return 服务端的中继接受确认
     * @throws IllegalArgumentException 如果参数无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun sendPacket(input: SendPacketInput): RelayAccepted {
        validateUserRef(input.target, "target")
        require(input.body.isNotEmpty()) { "body is required" }
        validateDeliveryMode(input.deliveryMode)
        input.targetSession?.let { if (!it.isZero()) validateSessionRef(it, "targetSession") }
        return rpc(
            build = { requestId ->
                val builder = Client.SendMessageRequest.newBuilder()
                    .setRequestId(requestId)
                    .setTarget(userRefToProto(input.target))
                    .setBody(com.google.protobuf.ByteString.copyFrom(input.body))
                    .setDeliveryKind(Client.ClientDeliveryKind.CLIENT_DELIVERY_KIND_TRANSIENT)
                    .setDeliveryMode(deliveryModeToProto(input.deliveryMode))
                    .setSyncMode(Client.ClientMessageSyncMode.CLIENT_MESSAGE_SYNC_MODE_UNSPECIFIED)
                input.targetSession?.takeUnless { it.isZero() }?.let { builder.targetSession = sessionRefToProto(it) }
                Client.ClientEnvelope.newBuilder().setSendMessage(builder.build()).build()
            },
            mapper = { value -> value as? RelayAccepted ?: throw ProtocolError("missing transient_accepted in send_message_response") }
        )
    }

    /**
     * 发送瞬时数据包的便捷重载方法。
     *
     * @param target 目标用户
     * @param body 数据包内容（原始字节）
     * @param deliveryMode 投递模式
     * @param targetSession 可选的目标会话引用
     * @return 服务端的中继接受确认
     * @see sendPacket
     */
    suspend fun sendPacket(target: UserRef, body: ByteArray, deliveryMode: DeliveryMode, targetSession: SessionRef? = null): RelayAccepted =
        sendPacket(SendPacketInput(target, body, deliveryMode, targetSession))

    /**
     * 创建一个新用户。
     *
     * @param request 创建用户请求
     * @return 创建成功的用户信息
     * @throws IllegalArgumentException 如果用户名或角色为空
     * @throws TimeoutException 如果请求超时
     */
    suspend fun createUser(request: CreateUserRequest): User {
        require(request.username.isNotEmpty()) { "username is required" }
        require(request.role.isNotEmpty()) { "role is required" }
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setCreateUser(
                        Client.CreateUserRequest.newBuilder()
                            .setRequestId(requestId)
                            .setUsername(request.username)
                            .setPassword(request.password?.wireValue().orEmpty())
                            .setProfileJson(com.google.protobuf.ByteString.copyFrom(request.profileJson))
                            .setRole(request.role)
                            .setLoginName(request.loginName)
                            .build()
                    )
                    .build()
            },
            mapper = { value -> value as? User ?: throw ProtocolError("missing user in create_user_response") }
        )
    }

    /**
     * 创建一个频道用户。
     *
     * 频道的角色默认为 "channel"。如果 [CreateUserRequest.role] 已设置，则使用原值。
     *
     * @param request 创建用户请求（角色可以留空，会自动填充为 "channel"）
     * @return 创建成功的频道用户信息
     * @see createUser
     */
    suspend fun createChannel(request: CreateUserRequest): User = createUser(request.copy(role = if (request.role.isEmpty()) "channel" else request.role))

    /**
     * 列出当前登录用户可通讯的活跃用户，并支持名称与 uid 过滤。
     *
     * 与 HTTP `GET /users` 一样，服务端会先收敛出“当前用户可通讯的用户集合”，
     * 再在该集合上应用 [filter]。普通用户看到的其他联系人可能会被隐藏 `login_name`，
     * 因此返回模型中的 [User.loginName] 允许为空字符串。
     *
     * `filter.uid` 在 Kotlin API 中统一使用 [UserRef] 表达：
     * - `null` 或 `UserRef(0, 0)` 表示不按 uid 过滤
     * - WebSocket 传输时会编码成 proto `UserRef`
     * - 如果只填写了一半，SDK 会在本地抛出 [IllegalArgumentException]
     *
     * 注意：当 [Config.realtimeStream] 为 `true` 时，服务端当前会拒绝 `list_users` RPC，
     * 并返回 `invalid_request`。SDK 保持与服务端一致，不在本地伪造结果。
     *
     * @param filter 过滤条件，支持 `name` 子串匹配与 `uid` 精确过滤
     * @return 当前登录用户可通讯的活跃用户列表
     * @throws IllegalArgumentException 如果 `uid` 只填写了一半或字段值不是正整数
     * @throws TimeoutException 如果请求超时
     */
    @Suppress("UNCHECKED_CAST")
    suspend fun listUsers(filter: UserListFilter = UserListFilter()): List<User> {
        val normalized = normalizeUserListFilter(filter)
        return rpc(
            build = { requestId ->
                val builder = Client.ListUsersRequest.newBuilder()
                    .setRequestId(requestId)
                    .setName(normalized.name)
                normalized.uid?.let { builder.uid = userRefToProto(it) }
                Client.ClientEnvelope.newBuilder()
                    .setListUsers(builder.build())
                    .build()
            },
            mapper = { value -> value as? List<User> ?: throw ProtocolError("missing items in list_users_response") }
        )
    }

    /**
     * 获取用户信息。
     *
     * @param target 目标用户引用
     * @return 用户详细信息
     * @throws IllegalArgumentException 如果目标用户引用无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun getUser(target: UserRef): User {
        validateUserRef(target, "target")
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setGetUser(Client.GetUserRequest.newBuilder().setRequestId(requestId).setUser(userRefToProto(target)).build())
                    .build()
            },
            mapper = { value -> value as? User ?: throw ProtocolError("missing user in get_user_response") }
        )
    }

    /**
     * 部分更新用户信息。
     *
     * 只有显式设置的字段会被更新，null 字段保持不变。
     * 对于 `loginName`，空字符串表示解绑当前登录名。
     *
     * @param target 目标用户引用
     * @param request 更新请求，每个字段为 null 表示不更新
     * @return 更新后的用户信息
     * @throws IllegalArgumentException 如果目标用户引用无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun updateUser(target: UserRef, request: UpdateUserRequest): User {
        validateUserRef(target, "target")
        return rpc(
            build = { requestId ->
                val builder = Client.UpdateUserRequest.newBuilder()
                    .setRequestId(requestId)
                    .setUser(userRefToProto(target))
                optionalStringField(request.username)?.let { builder.username = it }
                optionalPasswordField(request.password)?.let { builder.password = it }
                optionalBytesField(request.profileJson)?.let { builder.profileJson = it }
                optionalStringField(request.role)?.let { builder.role = it }
                optionalStringField(request.loginName)?.let { builder.loginName = it }
                Client.ClientEnvelope.newBuilder().setUpdateUser(builder.build()).build()
            },
            mapper = { value -> value as? User ?: throw ProtocolError("missing user in update_user_response") }
        )
    }

    /**
     * 删除一个用户。
     *
     * @param target 目标用户引用
     * @return 删除操作结果，包含状态和被删除用户的引用
     * @throws IllegalArgumentException 如果目标用户引用无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun deleteUser(target: UserRef): DeleteUserResult {
        validateUserRef(target, "target")
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setDeleteUser(Client.DeleteUserRequest.newBuilder().setRequestId(requestId).setUser(userRefToProto(target)).build())
                    .build()
            },
            mapper = { value -> value as? DeleteUserResult ?: throw ProtocolError("missing status in delete_user_response") }
        )
    }

    /**
     * 读取指定用户的一条私有元数据。
     *
     * 这里走的是 WebSocket + protobuf RPC，wire 上只有原始字节 `value`；
     * 因此返回结果中的 [UserMetadata.typedValue] 会始终保持为 `null`。
     * 如果调用方需要 HTTP `typed_value` 视图，应改用 [http] 上的同名接口。
     *
     * @param owner 元数据所属用户
     * @param key 元数据键名
     * @return 元数据条目
     * @throws IllegalArgumentException 如果参数无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun getUserMetadata(owner: UserRef, key: String): UserMetadata {
        validateUserRef(owner, "owner")
        validateUserMetadataKey(key, "key")
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setGetUserMetadata(
                        Client.GetUserMetadataRequest.newBuilder()
                            .setRequestId(requestId)
                            .setOwner(userRefToProto(owner))
                            .setKey(key)
                            .build()
                    )
                    .build()
            },
            mapper = { value -> value as? UserMetadata ?: throw ProtocolError("missing metadata in get_user_metadata_response") }
        )
    }

    /**
     * 创建或替换一条私有元数据。
     *
     * `expiresAt` 使用与服务端 HTTP API 相同的 RFC3339 字符串格式，
     * 调用者可以在两种传输协议间复用相同的值。
     * 但 WebSocket/protobuf 侧 metadata 仍然只发送原始字节，不支持 HTTP `typed_value` 请求体，
     * 因此这里继续要求 [value] 由调用方直接提供。
     *
     * @param owner 元数据所属用户
     * @param key 元数据键名
     * @param value 元数据值（原始字节）
     * @param expiresAt 可选的过期时间（RFC3339 格式），null 表示永不过期
     * @return 创建或更新后的元数据条目
     * @throws IllegalArgumentException 如果参数无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun upsertUserMetadata(owner: UserRef, key: String, value: ByteArray, expiresAt: String? = null): UserMetadata {
        validateUserRef(owner, "owner")
        validateUserMetadataKey(key, "key")
        return rpc(
            build = { requestId ->
                val builder = Client.UpsertUserMetadataRequest.newBuilder()
                    .setRequestId(requestId)
                    .setOwner(userRefToProto(owner))
                    .setKey(key)
                    .setValue(com.google.protobuf.ByteString.copyFrom(value))
                optionalStringField(expiresAt)?.let { builder.expiresAt = it }
                Client.ClientEnvelope.newBuilder().setUpsertUserMetadata(builder.build()).build()
            },
            mapper = { result -> result as? UserMetadata ?: throw ProtocolError("missing metadata in upsert_user_metadata_response") }
        )
    }

    /**
     * 删除一条私有元数据并返回服务端回显的已删除记录。
     *
     * @param owner 元数据所属用户
     * @param key 元数据键名
     * @return 已删除的元数据条目（tombstone 记录）
     * @throws IllegalArgumentException 如果参数无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun deleteUserMetadata(owner: UserRef, key: String): UserMetadata {
        validateUserRef(owner, "owner")
        validateUserMetadataKey(key, "key")
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setDeleteUserMetadata(
                        Client.DeleteUserMetadataRequest.newBuilder()
                            .setRequestId(requestId)
                            .setOwner(userRefToProto(owner))
                            .setKey(key)
                            .build()
                    )
                    .build()
            },
            mapper = { value -> value as? UserMetadata ?: throw ProtocolError("missing metadata in delete_user_metadata_response") }
        )
    }

    /**
     * 按键名顺序扫描私有元数据。
     *
     * 支持按前缀过滤、游标分页和数量限制。
     * 使用服务端的 `prefix` / `after` / `limit` 游标语义。
     * 返回条目只包含 protobuf 下发的原始字节值，不会携带 HTTP `typed_value` 视图。
     *
     * @param owner 元数据所属用户
     * @param prefix 键名前缀过滤，仅返回匹配该前缀的条目
     * @param after 游标值，从指定键之后开始扫描（包含性的 exclusive 游标）
     * @param limit 返回结果的最大数量（0 表示服务端默认限制）
     * @return 扫描结果，包含条目列表和下一页游标
     * @throws IllegalArgumentException 如果参数无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun scanUserMetadata(owner: UserRef, prefix: String = "", after: String = "", limit: Int = 0): UserMetadataScanResult {
        validateUserRef(owner, "owner")
        require(limit >= 0) { "limit must be non-negative" }
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setScanUserMetadata(
                        Client.ScanUserMetadataRequest.newBuilder()
                            .setRequestId(requestId)
                            .setOwner(userRefToProto(owner))
                            .setPrefix(prefix)
                            .setAfter(after)
                            .setLimit(limit)
                            .build()
                    )
                    .build()
            },
            mapper = { value -> value as? UserMetadataScanResult ?: throw ProtocolError("missing items in scan_user_metadata_response") }
        )
    }

    /**
     * 创建或替换一个用户关系附件。
     *
     * 附件系统用于管理用户间的关联关系，如频道权限、黑名单等。
     *
     * @param owner 附件所有者（关系的主体）
     * @param subject 附件关联的目标用户（关系的客体）
     * @param attachmentType 附件类型
     * @param configJson 附件配置的 JSON 数据（原始字节）
     * @return 创建或更新后的附件
     * @throws IllegalArgumentException 如果参数无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun upsertAttachment(owner: UserRef, subject: UserRef, attachmentType: AttachmentType, configJson: ByteArray): Attachment {
        validateUserRef(owner, "owner")
        validateUserRef(subject, "subject")
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setUpsertUserAttachment(
                        Client.UpsertUserAttachmentRequest.newBuilder()
                            .setRequestId(requestId)
                            .setOwner(userRefToProto(owner))
                            .setSubject(userRefToProto(subject))
                            .setAttachmentType(attachmentTypeToProto(attachmentType))
                            .setConfigJson(com.google.protobuf.ByteString.copyFrom(configJson))
                            .build()
                    )
                    .build()
            },
            mapper = { value -> value as? Attachment ?: throw ProtocolError("missing attachment in upsert_user_attachment_response") }
        )
    }

    /**
     * 删除一个用户关系附件。
     *
     * @param owner 附件所有者
     * @param subject 附件关联的目标用户
     * @param attachmentType 附件类型
     * @return 已删除的附件（tombstone 记录）
     * @throws IllegalArgumentException 如果参数无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun deleteAttachment(owner: UserRef, subject: UserRef, attachmentType: AttachmentType): Attachment {
        validateUserRef(owner, "owner")
        validateUserRef(subject, "subject")
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setDeleteUserAttachment(
                        Client.DeleteUserAttachmentRequest.newBuilder()
                            .setRequestId(requestId)
                            .setOwner(userRefToProto(owner))
                            .setSubject(userRefToProto(subject))
                            .setAttachmentType(attachmentTypeToProto(attachmentType))
                            .build()
                    )
                    .build()
            },
            mapper = { value -> value as? Attachment ?: throw ProtocolError("missing attachment in delete_user_attachment_response") }
        )
    }

    /**
     * 列出指定用户的所有附件，可选地按类型过滤。
     *
     * @param owner 附件所有者
     * @param attachmentType 可选的附件类型过滤器，null 表示列出所有类型
     * @return 附件列表
     * @throws IllegalArgumentException 如果所有者引用无效
     * @throws TimeoutException 如果请求超时
     */
    @Suppress("UNCHECKED_CAST")
    suspend fun listAttachments(owner: UserRef, attachmentType: AttachmentType? = null): List<Attachment> {
        validateUserRef(owner, "owner")
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setListUserAttachments(
                        Client.ListUserAttachmentsRequest.newBuilder()
                            .setRequestId(requestId)
                            .setOwner(userRefToProto(owner))
                            .setAttachmentType(attachmentTypeToProto(attachmentType))
                            .build()
                    )
                    .build()
            },
            mapper = { value -> value as? List<Attachment> ?: throw ProtocolError("missing items in list_user_attachments_response") }
        )
    }

    /**
     * 订阅一个频道。
     *
     * 订阅后，频道发布的消息会推送给订阅者。
     *
     * @param subscriber 订阅者
     * @param channel 被订阅的频道
     * @return 订阅信息
     * @throws TimeoutException 如果请求超时
     */
    suspend fun subscribeChannel(subscriber: UserRef, channel: UserRef): Subscription =
        upsertAttachment(subscriber, channel, AttachmentType.CHANNEL_SUBSCRIPTION, "{}".encodeToByteArray()).let {
            Subscription(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId)
        }

    /**
     * 取消订阅一个频道。
     *
     * @param subscriber 订阅者
     * @param channel 需要取消订阅的频道
     * @return 取消后的订阅信息（包含删除时间）
     * @throws TimeoutException 如果请求超时
     */
    suspend fun unsubscribeChannel(subscriber: UserRef, channel: UserRef): Subscription =
        deleteAttachment(subscriber, channel, AttachmentType.CHANNEL_SUBSCRIPTION).let {
            Subscription(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId)
        }

    /**
     * 列出指定用户订阅的所有频道。
     *
     * @param subscriber 订阅者
     * @return 订阅列表
     * @throws TimeoutException 如果请求超时
     */
    suspend fun listSubscriptions(subscriber: UserRef): List<Subscription> =
        listAttachments(subscriber, AttachmentType.CHANNEL_SUBSCRIPTION).map { Subscription(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId) }

    /**
     * 拉黑一个用户。
     *
     * 拉黑后，被拉黑用户的消息将被屏蔽。
     *
     * @param owner 执行拉黑操作的用户
     * @param blocked 被拉黑的用户
     * @return 黑名单条目信息
     * @throws TimeoutException 如果请求超时
     */
    suspend fun blockUser(owner: UserRef, blocked: UserRef): BlacklistEntry =
        upsertAttachment(owner, blocked, AttachmentType.USER_BLACKLIST, "{}".encodeToByteArray()).let {
            BlacklistEntry(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId)
        }

    /**
     * 解除对用户的拉黑。
     *
     * @param owner 执行解除拉黑操作的用户
     * @param blocked 需要解除拉黑的用户
     * @return 解除后的黑名单条目信息（包含删除时间）
     * @throws TimeoutException 如果请求超时
     */
    suspend fun unblockUser(owner: UserRef, blocked: UserRef): BlacklistEntry =
        deleteAttachment(owner, blocked, AttachmentType.USER_BLACKLIST).let {
            BlacklistEntry(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId)
        }

    /**
     * 列出指定用户拉黑的所有用户。
     *
     * @param owner 用户引用
     * @return 黑名单条目列表
     * @throws TimeoutException 如果请求超时
     */
    suspend fun listBlockedUsers(owner: UserRef): List<BlacklistEntry> =
        listAttachments(owner, AttachmentType.USER_BLACKLIST).map { BlacklistEntry(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId) }

    /**
     * 列出指定目标用户的持久化消息。
     *
     * @param target 目标用户
     * @param limit 返回消息的最大数量
     * @return 消息列表
     * @throws IllegalArgumentException 如果目标用户引用无效
     * @throws TimeoutException 如果请求超时
     */
    @Suppress("UNCHECKED_CAST")
    suspend fun listMessages(target: UserRef, limit: Int): List<Message> {
        validateUserRef(target, "target")
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setListMessages(
                        Client.ListMessagesRequest.newBuilder()
                            .setRequestId(requestId)
                            .setUser(userRefToProto(target))
                            .setLimit(limit)
                            .build()
                    )
                    .build()
            },
            mapper = { value -> value as? List<Message> ?: throw ProtocolError("missing items in list_messages_response") }
        )
    }

    /**
     * 列出指定序列号之后的事件。
     *
     * @param after 起始事件序列号（不包含），从该序列号之后开始列出
     * @param limit 返回事件的最大数量
     * @return 事件列表
     * @throws TimeoutException 如果请求超时
     */
    @Suppress("UNCHECKED_CAST")
    suspend fun listEvents(after: Long, limit: Int): List<Event> =
        rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setListEvents(Client.ListEventsRequest.newBuilder().setRequestId(requestId).setAfter(after).setLimit(limit).build())
                    .build()
            },
            mapper = { value -> value as? List<Event> ?: throw ProtocolError("missing items in list_events_response") }
        )

    /**
     * 列出集群中的所有节点。
     *
     * @return 集群节点列表
     * @throws TimeoutException 如果请求超时
     */
    @Suppress("UNCHECKED_CAST")
    suspend fun listClusterNodes(): List<ClusterNode> =
        rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setListClusterNodes(Client.ListClusterNodesRequest.newBuilder().setRequestId(requestId).build())
                    .build()
            },
            mapper = { value -> value as? List<ClusterNode> ?: throw ProtocolError("missing items in list_cluster_nodes_response") }
        )

    /**
     * 列出指定节点上当前已登录的用户。
     *
     * @param nodeId 目标节点 ID
     * @return 已登录用户列表
     * @throws IllegalArgumentException 如果节点 ID 无效
     * @throws TimeoutException 如果请求超时
     */
    @Suppress("UNCHECKED_CAST")
    suspend fun listNodeLoggedInUsers(nodeId: Long): List<LoggedInUser> {
        require(nodeId > 0) { "nodeId is required" }
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setListNodeLoggedInUsers(Client.ListNodeLoggedInUsersRequest.newBuilder().setRequestId(requestId).setNodeId(nodeId).build())
                    .build()
            },
            mapper = { value -> value as? List<LoggedInUser> ?: throw ProtocolError("missing items in list_node_logged_in_users_response") }
        )
    }

    /**
     * 解析目标用户的在线会话信息。
     *
     * 返回用户当前的在线状态、所在节点和各会话的详细信息。
     *
     * @param user 目标用户
     * @return 用户的会话解析结果，包含在线状态和活跃会话列表
     * @throws IllegalArgumentException 如果用户引用无效
     * @throws TimeoutException 如果请求超时
     */
    suspend fun resolveUserSessions(user: UserRef): ResolvedUserSessions {
        validateUserRef(user, "user")
        return rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setResolveUserSessions(Client.ResolveUserSessionsRequest.newBuilder().setRequestId(requestId).setUser(userRefToProto(user)).build())
                    .build()
            },
            mapper = { value -> value as? ResolvedUserSessions ?: throw ProtocolError("missing sessions in resolve_user_sessions_response") }
        )
    }

    /**
     * 查询当前节点的运维状态。
     *
     * 返回消息窗口大小、事件序列号、写门控状态、冲突统计、对端节点状态等运维指标。
     *
     * @return 节点运维状态
     * @throws TimeoutException 如果请求超时
     */
    suspend fun operationsStatus(): OperationsStatus =
        rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setOperationsStatus(Client.OperationsStatusRequest.newBuilder().setRequestId(requestId).build())
                    .build()
            },
            mapper = { value -> value as? OperationsStatus ?: throw ProtocolError("missing status in operations_status_response") }
        )

    /**
     * 获取节点指标信息。
     *
     * @return 指标信息的纯文本字符串
     * @throws TimeoutException 如果请求超时
     */
    suspend fun metrics(): String =
        rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setMetrics(Client.MetricsRequest.newBuilder().setRequestId(requestId).build())
                    .build()
            },
            mapper = { value -> value as? String ?: throw ProtocolError("missing text in metrics_response") }
        )

    private suspend fun runLoop() {
        var delayDuration = config.initialReconnectDelay
        while (scope.isActive && !closed) {
            val attempt = Attempt()
            var error: Throwable? = null
            try {
                connectAttempt(attempt)
                delayDuration = config.initialReconnectDelay
                startPingLoop()
                // 登录成功后，此 await 成为当前活跃 WebSocket 的生命周期门控，
                // 在套接字关闭或失败时完成。
                error = attempt.close.await() ?: IllegalStateException(DISCONNECTED_MESSAGE)
            } catch (t: Throwable) {
                error = unwrap(t)
            } finally {
                cancelPing()
                synchronized(stateLock) {
                    if (webSocket === attempt.socket) {
                        webSocket = null
                    }
                    authenticated = false
                }
                _loginState.value = null
                _connectionState.value = if (closed) ConnectionState.CLOSED else ConnectionState.DISCONNECTED
                failAllPending(IllegalStateException(DISCONNECTED_MESSAGE))
                error?.let {
                    _events.tryEmit(ClientEvent.Disconnect(it))
                }
            }

            if (closed) {
                firstConnect.completeExceptionally(IllegalStateException(CLOSED_MESSAGE))
                return
            }
            if (!shouldRetry(error)) {
                if (!firstConnect.isCompleted) {
                    firstConnect.completeExceptionally(error ?: IllegalStateException(DISCONNECTED_MESSAGE))
                }
                failAllPending(error ?: IllegalStateException(DISCONNECTED_MESSAGE))
                return
            }
            error?.let { _events.tryEmit(ClientEvent.Error(it)) }
            delay(delayDuration.toMillis())
            // 指数退避仅在成功的经过身份验证的尝试后重置，
            // 因此重复的握手失败不会对服务器造成压力。
            delayDuration = delayDuration.multipliedBy(2).coerceAtMost(config.maxReconnectDelay)
        }
    }

    private suspend fun connectAttempt(attempt: Attempt) {
        _connectionState.value = ConnectionState.CONNECTING
        // seen_messages 在新会话开始流式传输之前将持久化游标集重播到服务器，
        // 这使得重新连接可以在不重复投递已持久化消息的情况下恢复。
        attempt.seen = config.cursorStore.loadSeenMessages()
        val request = Request.Builder().url(websocketUrl(config.baseUrl, config.realtimeStream)).build()
        attempt.socket = httpClient.newWebSocket(request, AttemptListener(attempt))
        attempt.login.await()
    }

    private fun startPingLoop() {
        cancelPing()
        pingJob = scope.launch {
            while (isActive && !closed) {
                delay(config.pingInterval.toMillis())
                try {
                    ping()
                } catch (t: Throwable) {
                    val error = unwrap(t)
                    if (!isClosedLike(error) && !isDisconnectedLike(error)) {
                        _events.tryEmit(ClientEvent.Error(error))
                    }
                }
            }
        }
    }

    private fun cancelPing() {
        pingJob?.cancel()
        pingJob = null
    }

    private fun shouldRetry(error: Throwable?): Boolean {
        if (closed || stopReconnect || !config.reconnect) {
            return false
        }
        // 凭据和 SDK 编译时 wire epoch 都不会因重拨而改变；对应的登录失败只能由调用方升级或重新配置。
        return error !is ServerError || !error.unauthorized()
    }

    private suspend fun <T> rpc(build: (Long) -> Client.ClientEnvelope, mapper: (Any) -> T): T {
        val id = nextRequestId()
        val deferred = CompletableDeferred<Any>()
        pending[id] = deferred
        scope.launch {
            delay(config.requestTimeout.toMillis())
            pending.remove(id)?.completeExceptionally(TimeoutException("request timed out"))
        }
        try {
            sendEnvelope(build(id))
            return mapper(deferred.await())
        } finally {
            // 超时协程和 WebSocket 回调会竞争同一个条目，
            // 最后的 remove 确保无论谁赢得竞争，都不会泄漏映射状态。
            pending.remove(id)
        }
    }

    private fun sendEnvelope(envelope: Client.ClientEnvelope) {
        val socket = webSocket
        check(!closed) { CLOSED_MESSAGE }
        check(authenticated && socket != null) { NOT_CONNECTED_MESSAGE }
        check(socket.send(envelope.toByteArray().toByteString())) { NOT_CONNECTED_MESSAGE }
    }

    private fun completePending(requestId: Long, value: Any) {
        pending.remove(requestId)?.complete(value)
    }

    private fun failPending(requestId: Long, error: Throwable) {
        pending.remove(requestId)?.completeExceptionally(error)
    }

    private fun failAllPending(error: Throwable) {
        pending.values.forEach { it.completeExceptionally(error) }
        pending.clear()
    }

    private fun nextRequestId(): Long {
        while (true) {
            val next = requestId.incrementAndGet()
            if (next > 0) {
                return next
            }
            // Proto 使用 uint64，但 Kotlin 使用有符号 Long。
            // 将非正数的回绕值重置为 0，使本地生成的 ID 始终可表示并通过 requireUnsigned() 检查。
            requestId.compareAndSet(next, 0)
        }
    }

    private suspend fun persistMessage(message: Message) {
        // 游标在消息载荷之后保存，这样重连时不会广告一个存储无法
        // 具体化或后续检查的已见游标。
        config.cursorStore.saveMessage(message)
        config.cursorStore.saveCursor(message.cursor())
    }

    private inner class AttemptListener(private val attempt: Attempt) : WebSocketListener() {
        override fun onOpen(webSocket: WebSocket, response: Response) {
            val login = Client.LoginRequest.newBuilder()
                .setPassword(config.credentials.password.wireValue())
                .setTransientOnly(config.transientOnly)
                .setProtocolVersion(CLIENT_PROTOCOL_VERSION)
            if (config.credentials.loginName.isNotBlank()) {
                login.loginName = config.credentials.loginName
            } else {
                login.user = userRefToProto(UserRef(config.credentials.nodeId, config.credentials.userId))
            }
            // 登录帧同时作为重连状态传递：在服务器在此会话上开始推送任何
            // 新的持久化流量之前，发送先前已见消息游标。
            attempt.seen.forEach { login.addSeenMessages(cursorToProto(it)) }
            if (!webSocket.send(Client.ClientEnvelope.newBuilder().setLogin(login.build()).build().toByteArray().toByteString())) {
                attempt.login.completeExceptionally(IllegalStateException(NOT_CONNECTED_MESSAGE))
                attempt.close.complete(IllegalStateException(NOT_CONNECTED_MESSAGE))
            }
        }

        override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
            val env = try {
                Client.ServerEnvelope.parseFrom(bytes.toByteArray())
            } catch (_: Exception) {
                _events.tryEmit(ClientEvent.Error(ProtocolError("invalid protobuf frame")))
                return
            }
            if (!attempt.login.isCompleted) {
                handleLoginEnvelope(webSocket, env)
                return
            }
            // 有序处理确保推送投递、RPC 响应和 ACK 副作用与线路顺序一致，
            // 即使 OkHttp 可能并发调用回调。
            orderedScope.launch {
                handleAuthedEnvelope(env)
            }
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            attempt.close.complete(IllegalStateException(DISCONNECTED_MESSAGE))
        }

        override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
            webSocket.close(code, reason)
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            val error = if (response == null) ConnectionError("dial", t) else ConnectionError("dial", RuntimeException("status=${response.code}", t))
            attempt.login.completeExceptionally(error)
            attempt.close.complete(error)
        }

        private fun handleLoginEnvelope(webSocket: WebSocket, env: Client.ServerEnvelope) {
            when (env.bodyCase) {
                Client.ServerEnvelope.BodyCase.LOGIN_RESPONSE -> {
                    val got = env.loginResponse.protocolVersion
                    if (got != CLIENT_PROTOCOL_VERSION) {
                        // 历史 ClientEnvelope/ServerEnvelope 曾把相同 tag 重新赋予完全不同的 RPC。
                        // 因此版本不匹配不是可重试的网络故障：必须在写入 socket/auth 状态、
                        // 更新 loginState/connectionState、完成 firstConnect 或发布 Login 事件前终止，
                        // 否则后续帧可能被解释成另一种操作。
                        val error = ProtocolError(
                            "unsupported login response protocol version: got=\"$got\" want=\"$CLIENT_PROTOCOL_VERSION\""
                        )
                        stopReconnect = true
                        attempt.login.completeExceptionally(error)
                        attempt.close.complete(error)
                        webSocket.close(1002, "protocol version mismatch")
                        return
                    }
                    val info = loginInfoFromProto(env.loginResponse)
                    synchronized(stateLock) {
                        this@TurntfClient.webSocket = webSocket
                        authenticated = true
                    }
                    _loginState.value = info
                    _connectionState.value = ConnectionState.CONNECTED
                    if (!firstConnect.isCompleted) {
                        firstConnect.complete(Unit)
                    }
                    _events.tryEmit(ClientEvent.Login(info))
                    attempt.login.complete(info)
                }
                Client.ServerEnvelope.BodyCase.ERROR -> {
                    val error = ServerError(env.error.code, env.error.message, env.error.requestId)
                    // 版本常量与凭据一样固定在当前客户端实例中，自动重连无法修复这两类拒绝。
                    if (error.unauthorized() || error.code == "unsupported_protocol_version") {
                        stopReconnect = true
                    }
                    attempt.login.completeExceptionally(error)
                    attempt.close.complete(error)
                    webSocket.close(1008, "login failed")
                }
                else -> {
                    val error = ProtocolError("expected login_response or error")
                    attempt.login.completeExceptionally(error)
                    attempt.close.complete(error)
                    webSocket.close(1002, "protocol")
                }
            }
        }
    }

    private suspend fun handleAuthedEnvelope(env: Client.ServerEnvelope) {
        try {
            when (env.bodyCase) {
                Client.ServerEnvelope.BodyCase.MESSAGE_PUSHED -> {
                    val message = messageFromProto(env.messagePushed.message)
                    persistMessage(message)
                    if (config.ackMessages) {
                        try {
                            // 仅在本地持久化后发送 ACK。重连时相同的游标会通过
                            // seen_messages 重新广告，因此服务器只会了解到我们已持久化记录的工作。
                            sendEnvelope(
                                Client.ClientEnvelope.newBuilder()
                                    .setAckMessage(Client.AckMessage.newBuilder().setCursor(cursorToProto(message.cursor())).build())
                                    .build()
                            )
                        } catch (_: Throwable) {
                        }
                    }
                    _events.tryEmit(ClientEvent.MessageReceived(message))
                }
                Client.ServerEnvelope.BodyCase.PACKET_PUSHED -> {
                    val packet = packetFromProto(env.packetPushed.packet)
                    // 先尝试 relay 帧分发，非 relay 包才投递给用户
                    if (!relay.handlePacket(packet)) {
                        _events.tryEmit(ClientEvent.PacketReceived(packet))
                    }
                }
                Client.ServerEnvelope.BodyCase.SEND_MESSAGE_RESPONSE -> {
                    val requestId = requireUnsigned(env.sendMessageResponse.requestId, "request_id")
                    when (env.sendMessageResponse.bodyCase) {
                        Client.SendMessageResponse.BodyCase.MESSAGE -> {
                            val message = messageFromProto(env.sendMessageResponse.message)
                            // 持久化发送响应也会推进游标存储，这样在自身成功发送后立即
                            // 重连的客户端不会重新消费回显的持久化消息。
                            persistMessage(message)
                            completePending(requestId, message)
                        }
                        Client.SendMessageResponse.BodyCase.TRANSIENT_ACCEPTED -> completePending(requestId, relayAcceptedFromProto(env.sendMessageResponse.transientAccepted))
                        else -> failPending(requestId, ProtocolError("empty send_message_response"))
                    }
                }
                Client.ServerEnvelope.BodyCase.PONG -> completePending(requireUnsigned(env.pong.requestId, "request_id"), Unit)
                Client.ServerEnvelope.BodyCase.CREATE_USER_RESPONSE -> completePending(requireUnsigned(env.createUserResponse.requestId, "request_id"), userFromProto(env.createUserResponse.user))
                Client.ServerEnvelope.BodyCase.GET_USER_RESPONSE -> completePending(requireUnsigned(env.getUserResponse.requestId, "request_id"), userFromProto(env.getUserResponse.user))
                Client.ServerEnvelope.BodyCase.UPDATE_USER_RESPONSE -> completePending(requireUnsigned(env.updateUserResponse.requestId, "request_id"), userFromProto(env.updateUserResponse.user))
                Client.ServerEnvelope.BodyCase.DELETE_USER_RESPONSE -> completePending(requireUnsigned(env.deleteUserResponse.requestId, "request_id"), deleteUserResultFromProto(env.deleteUserResponse))
                Client.ServerEnvelope.BodyCase.LIST_USERS_RESPONSE -> completePending(requireUnsigned(env.listUsersResponse.requestId, "request_id"), env.listUsersResponse.itemsList.map(::userFromProto))
                Client.ServerEnvelope.BodyCase.GET_USER_METADATA_RESPONSE -> completePending(requireUnsigned(env.getUserMetadataResponse.requestId, "request_id"), userMetadataFromProto(env.getUserMetadataResponse.metadata))
                Client.ServerEnvelope.BodyCase.UPSERT_USER_METADATA_RESPONSE -> completePending(requireUnsigned(env.upsertUserMetadataResponse.requestId, "request_id"), userMetadataFromProto(env.upsertUserMetadataResponse.metadata))
                Client.ServerEnvelope.BodyCase.DELETE_USER_METADATA_RESPONSE -> completePending(requireUnsigned(env.deleteUserMetadataResponse.requestId, "request_id"), userMetadataFromProto(env.deleteUserMetadataResponse.metadata))
                Client.ServerEnvelope.BodyCase.SCAN_USER_METADATA_RESPONSE -> completePending(requireUnsigned(env.scanUserMetadataResponse.requestId, "request_id"), userMetadataScanResultFromProto(env.scanUserMetadataResponse))
                Client.ServerEnvelope.BodyCase.LIST_MESSAGES_RESPONSE -> completePending(requireUnsigned(env.listMessagesResponse.requestId, "request_id"), env.listMessagesResponse.itemsList.map(::messageFromProto))
                Client.ServerEnvelope.BodyCase.UPSERT_USER_ATTACHMENT_RESPONSE -> completePending(requireUnsigned(env.upsertUserAttachmentResponse.requestId, "request_id"), attachmentFromProto(env.upsertUserAttachmentResponse.attachment))
                Client.ServerEnvelope.BodyCase.DELETE_USER_ATTACHMENT_RESPONSE -> completePending(requireUnsigned(env.deleteUserAttachmentResponse.requestId, "request_id"), attachmentFromProto(env.deleteUserAttachmentResponse.attachment))
                Client.ServerEnvelope.BodyCase.LIST_USER_ATTACHMENTS_RESPONSE -> completePending(requireUnsigned(env.listUserAttachmentsResponse.requestId, "request_id"), env.listUserAttachmentsResponse.itemsList.map(::attachmentFromProto))
                Client.ServerEnvelope.BodyCase.LIST_EVENTS_RESPONSE -> completePending(requireUnsigned(env.listEventsResponse.requestId, "request_id"), env.listEventsResponse.itemsList.map(::eventFromProto))
                Client.ServerEnvelope.BodyCase.LIST_CLUSTER_NODES_RESPONSE -> completePending(requireUnsigned(env.listClusterNodesResponse.requestId, "request_id"), env.listClusterNodesResponse.itemsList.map(::clusterNodeFromProto))
                Client.ServerEnvelope.BodyCase.LIST_NODE_LOGGED_IN_USERS_RESPONSE -> completePending(requireUnsigned(env.listNodeLoggedInUsersResponse.requestId, "request_id"), env.listNodeLoggedInUsersResponse.itemsList.map(::loggedInUserFromProto))
                Client.ServerEnvelope.BodyCase.RESOLVE_USER_SESSIONS_RESPONSE -> completePending(requireUnsigned(env.resolveUserSessionsResponse.requestId, "request_id"), resolvedUserSessionsFromProto(env.resolveUserSessionsResponse))
                Client.ServerEnvelope.BodyCase.OPERATIONS_STATUS_RESPONSE -> completePending(requireUnsigned(env.operationsStatusResponse.requestId, "request_id"), operationsStatusFromProto(env.operationsStatusResponse.status))
                Client.ServerEnvelope.BodyCase.METRICS_RESPONSE -> completePending(requireUnsigned(env.metricsResponse.requestId, "request_id"), env.metricsResponse.text)
                Client.ServerEnvelope.BodyCase.ERROR -> {
                    val error = ServerError(env.error.code, env.error.message, env.error.requestId)
                    if (env.error.requestId != 0L) {
                        // request_id == 0 意味着该失败不能归因于调用者发出的 RPC，
                        // 应该作为流级别错误呈现，而不是完成某个 RPC。
                        failPending(requireUnsigned(env.error.requestId, "request_id"), error)
                    } else {
                        _events.tryEmit(ClientEvent.Error(error))
                    }
                }
                Client.ServerEnvelope.BodyCase.LOGIN_RESPONSE -> _events.tryEmit(ClientEvent.Error(ProtocolError("unexpected login_response after authentication")))
                Client.ServerEnvelope.BodyCase.BODY_NOT_SET -> _events.tryEmit(ClientEvent.Error(ProtocolError("unsupported server envelope")))
            }
        } catch (t: Throwable) {
            _events.tryEmit(ClientEvent.Error(unwrap(t)))
        }
    }

    private data class Attempt(
        val login: CompletableDeferred<LoginInfo> = CompletableDeferred(),
        val close: CompletableDeferred<Throwable?> = CompletableDeferred(),
        var seen: List<MessageCursor> = emptyList(),
        var socket: WebSocket? = null
    )

    private fun normalize(config: Config): Config {
        validateBaseUrl(config.baseUrl)
        config.credentials.validate()
        return config
    }

    private fun unwrap(error: Throwable?): Throwable {
        return when (error) {
            null -> IllegalStateException(DISCONNECTED_MESSAGE)
            is CancellationException -> IllegalStateException(CLOSED_MESSAGE)
            else -> error
        }
    }

    private fun isClosedLike(error: Throwable): Boolean = error is IllegalStateException && error.message == CLOSED_MESSAGE

    private fun isDisconnectedLike(error: Throwable): Boolean =
        error is IllegalStateException && (error.message == NOT_CONNECTED_MESSAGE || error.message == DISCONNECTED_MESSAGE)
}
