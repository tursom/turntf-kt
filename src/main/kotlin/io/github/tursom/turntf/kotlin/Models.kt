package io.github.tursom.turntf.kotlin

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.mindrot.jbcrypt.BCrypt
import okhttp3.OkHttpClient
import java.time.Duration

/**
 * 指示密码来源是明文输入还是已有的哈希值。
 *
 * SDK 使用此枚举区分密码在传输前是否需要经过本地哈希处理，或是直接转发调用者提供的已哈希值。
 * 该信息被 [PasswordInput] 携带，确保传输层行为一致。
 *
 * @see PasswordInput
 * @see plainPassword
 * @see hashedPassword
 */
enum class PasswordSource {
    /** 密码来自明文输入，需要在使用前进行哈希处理 */
    PLAIN,
    /** 密码已经过哈希处理，可以直接用于传输 */
    HASHED
}

/**
 * 密码包装器，同时用于 HTTP 和 WebSocket 传输流程。
 *
 * SDK 保持传输载荷不透明，同时保留密码来源信息——即值是本地哈希生成的还是调用者预先提供的哈希值。
 * 这允许上层调用者以统一的方式处理密码，而无需关心底层传输细节。
 *
 * @param source 密码来源，指示该密码是明文哈希而来还是预先哈希的
 * @param encoded 编码后的密码值（已哈希的字符串）
 */
data class PasswordInput(
    val source: PasswordSource,
    val encoded: String
) {
    /**
     * 验证密码值非空。
     *
     * @throws IllegalArgumentException 如果 `encoded` 为空字符串
     */
    fun validate() {
        require(encoded.isNotEmpty()) { "password is required" }
    }

    /**
     * 返回用于网络传输的编码密码值。
     *
     * 在返回值之前会调用 [validate] 确保密码非空。
     *
     * @return 编码后的密码字符串
     * @throws IllegalArgumentException 如果密码未通过验证
     */
    fun wireValue(): String {
        validate()
        return encoded
    }
}

/**
 * 使用 bcrypt 对明文密码进行哈希处理，用于 turntf 登录和用户管理请求。
 *
 * 此函数会生成一个随机的 salt 并执行 bcrypt 哈希运算。
 * 哈希后的密码可以直接用于 [plainPassword] 创建 [PasswordInput]。
 *
 * @param plain 明文密码字符串
 * @return bcrypt 哈希后的密码字符串
 * @throws IllegalArgumentException 如果密码为空
 * @see plainPassword
 * @see PasswordInput
 */
fun hashPassword(plain: String): String {
    require(plain.isNotEmpty()) { "password is required" }
    return BCrypt.hashpw(plain, BCrypt.gensalt())
}

/**
 * 从明文密码创建密码载荷，自动在本地进行 bcrypt 哈希处理。
 *
 * 这是最常用的密码创建方式——调用者传入明文密码，SDK 负责哈希和包装。
 * 生成的 [PasswordInput.source] 为 [PasswordSource.PLAIN]。
 *
 * @param plain 明文密码字符串
 * @return 包含哈希后密码的 [PasswordInput] 实例
 * @see hashedPassword
 * @see hashPassword
 */
fun plainPassword(plain: String): PasswordInput = PasswordInput(PasswordSource.PLAIN, hashPassword(plain))

/**
 * 包装一个已经过哈希处理的密码值，使 SDK 可以直接转发而无需重新哈希。
 *
 * 适用于从外部系统导入已有哈希密码的场景。
 * 生成的 [PasswordInput.source] 为 [PasswordSource.HASHED]。
 *
 * @param value 已哈希的密码字符串
 * @return 包含已哈希密码的 [PasswordInput] 实例
 * @see plainPassword
 */
fun hashedPassword(value: String): PasswordInput = PasswordInput(PasswordSource.HASHED, value)

/**
 * 用于 WebSocket 登录和 HTTP 登录委托的身份凭据。
 *
 * 必须且仅能提供一种登录方式：
 * - 传统方式：提供 `nodeId` + `userId` + `password`
 * - 登录名方式：提供 `loginName` + `password`
 *
 * `username` 是用户资料字段，不用于身份认证。
 *
 * @param nodeId 目标节点 ID（传统登录方式使用）
 * @param userId 目标用户 ID（传统登录方式使用）
 * @param password 密码输入，包含哈希后的密码值和来源信息
 * @param loginName 登录名（登录名登录方式使用）
 */
data class Credentials(
    val nodeId: Long = 0,
    val userId: Long = 0,
    val password: PasswordInput,
    val loginName: String = ""
) {
    /**
     * 验证凭据的有效性。
     *
     * 检查是否恰好提供了一种登录方式（传统方式或登录名方式），
     * 并验证所提供的参数符合对应的格式要求。
     *
     * @throws IllegalArgumentException 如果登录方式不明确或参数无效
     */
    fun validate() {
        val hasUserSelector = nodeId > 0 || userId > 0
        val hasLoginNameSelector = loginName.isNotBlank()
        require(hasUserSelector xor hasLoginNameSelector) {
            "exactly one of (nodeId,userId) or loginName must be provided"
        }
        if (hasUserSelector) {
            require(nodeId > 0) { "credentials.nodeId is required" }
            require(userId > 0) { "credentials.userId is required" }
        }
        password.validate()
    }
}

/**
 * [TurntfClient] 的运行时配置。
 *
 * 默认值针对长时间运行的会话进行了优化：启用重连、30 秒心跳间隔、
 * 10 秒 RPC 超时、持久化消息处理后自动发送确认（ACK）。
 *
 * @param baseUrl 服务器基础 URL（例如 "https://example.com"）
 * @param credentials 登录凭据
 * @param cursorStore 游标存储实现，用于持久化消息去重和断线重连恢复 [CursorStore]
 * @param httpClient OkHttp 客户端实例，用于 HTTP 和 WebSocket 连接
 * @param reconnect 是否在断开连接时自动重连
 * @param initialReconnectDelay 初始重连延迟
 * @param maxReconnectDelay 最大重连延迟（指数退避的上限）
 * @param pingInterval 应用层心跳间隔
 * @param requestTimeout 单个 RPC 请求的超时时间
 * @param ackMessages 是否在持久化消息处理完成后自动发送确认（ACK）
 * @param transientOnly 是否仅使用瞬时连接（不处理持久化消息，适用于纯实时场景）
 * @param realtimeStream 是否使用实时 WebSocket 端点（/ws/realtime），而非默认的客户端端点（/ws/client）
 */
data class Config(
    val baseUrl: String,
    val credentials: Credentials,
    val cursorStore: CursorStore = MemoryCursorStore(),
    val httpClient: OkHttpClient = OkHttpClient(),
    val reconnect: Boolean = true,
    val initialReconnectDelay: Duration = Duration.ofSeconds(1),
    val maxReconnectDelay: Duration = Duration.ofSeconds(30),
    val pingInterval: Duration = Duration.ofSeconds(30),
    val requestTimeout: Duration = Duration.ofSeconds(10),
    val ackMessages: Boolean = true,
    val transientOnly: Boolean = false,
    val realtimeStream: Boolean = false
)

/**
 * 用户引用，通过节点 ID 和用户 ID 唯一标识一个用户。
 *
 * 在 turntf 集群中，用户由其所在的节点（nodeId）和在该节点上的用户 ID（userId）共同标识。
 *
 * @param nodeId 节点 ID
 * @param userId 用户 ID
 */
data class UserRef(val nodeId: Long, val userId: Long)

/**
 * 会话引用，通过服务节点 ID 和会话 ID 唯一标识一个客户端会话。
 *
 * 用于定位用户的具体在线会话，特别是在发送瞬时数据包时指定目标会话。
 *
 * @param servingNodeId 服务节点 ID
 * @param sessionId 会话 ID 字符串
 */
data class SessionRef(val servingNodeId: Long, val sessionId: String) {
    /**
     * 检查此会话引用是否为零值（未设置）。
     *
     * @return 如果 `servingNodeId` 为 0 且 `sessionId` 为空字符串，则返回 `true`
     */
    fun isZero(): Boolean = servingNodeId == 0L && sessionId.isEmpty()
}

/**
 * 用户信息，包含用户在 turntf 系统中的完整资料。
 *
 * @param nodeId 节点 ID
 * @param userId 用户 ID
 * @param username 显示用户名
 * @param role 用户角色（例如 "user"、"channel" 等）
 * @param profileJson 用户资料 JSON 数据的原始字节
 * @param systemReserved 是否为系统保留用户
 * @param createdAt 用户创建时间的 RFC3339 字符串
 * @param updatedAt 用户信息最后更新时间的 RFC3339 字符串
 * @param originNodeId 用户所属的源节点 ID（分布式环境下使用）
 * @param loginName 登录名，用于替代 nodeId+userId 的传统登录方式
 */
data class User(
    val nodeId: Long,
    val userId: Long,
    val username: String,
    val role: String,
    val profileJson: ByteArray = byteArrayOf(),
    val systemReserved: Boolean = false,
    val createdAt: String = "",
    val updatedAt: String = "",
    val originNodeId: Long = 0,
    val loginName: String = ""
)

/**
 * 用户私有的元数据条目，同时用于 HTTP 和 WebSocket API。
 *
 * SDK 始终将 [value] 暴露为原始字节（ByteArray），
 * 即使 HTTP 传输层将其序列化为 base64。这使得调用者在切换传输协议时无需修改应用层模型。
 *
 * @param owner 元数据所属的用户引用
 * @param key 元数据键名
 * @param value 元数据值（原始字节）
 * @param updatedAt 最后更新时间（RFC3339 格式）
 * @param deletedAt 删除时间（如果已删除，RFC3339 格式）
 * @param expiresAt 过期时间（如果设置了过期，RFC3339 格式）
 * @param originNodeId 源节点 ID
 */
data class UserMetadata(
    val owner: UserRef,
    val key: String,
    val value: ByteArray = byteArrayOf(),
    val updatedAt: String = "",
    val deletedAt: String = "",
    val expiresAt: String = "",
    val originNodeId: Long = 0
)

/**
 * 基于游标的用户私有元数据扫描结果。
 *
 * [nextAfter] 应作为下一次扫描调用的 `after` 参数传入，以从最后一个返回的键继续遍历。
 *
 * @param items 本次扫描返回的元数据条目列表
 * @param count 符合条件的总条目数（如果服务端返回了计数信息）
 * @param nextAfter 下一次扫描的起始游标值，空字符串表示没有更多数据
 */
data class UserMetadataScanResult(
    val items: List<UserMetadata> = emptyList(),
    val count: Int = 0,
    val nextAfter: String = ""
)

/**
 * 消息游标，通过节点 ID 和序列号唯一标识一条持久化消息。
 *
 * 用于消息确认（ACK）、断线重连时的已见消息传递，以及持久化消息的去重。
 *
 * @param nodeId 消息所在的节点 ID
 * @param seq 消息序列号
 */
data class MessageCursor(val nodeId: Long, val seq: Long)

/**
 * 消息投递模式，控制瞬时数据包在集群中的投递行为。
 *
 * @property wireValue 对应传输层使用的字符串值
 */
enum class DeliveryMode(val wireValue: String) {
    /** 未指定投递模式 */
    UNSPECIFIED(""),
    /** 尽最大努力投递，可能丢失，不保证送达 */
    BEST_EFFORT("best_effort"),
    /** 路由重试模式，在路由层面保证投递尝试 */
    ROUTE_RETRY("route_retry")
}

/**
 * 持久化消息，代表一条已经持久化存储的端到端消息。
 *
 * 与瞬时数据包不同，持久化消息会被服务端存储并在断线重连后重新投递。
 *
 * @param recipient 消息接收者
 * @param nodeId 消息所在的节点 ID
 * @param seq 消息序列号（在同一节点内递增）
 * @param sender 消息发送者
 * @param body 消息内容（原始字节）
 * @param createdAtHlc 消息创建时间的混合逻辑时钟（HLC）时间戳
 */
data class Message(
    val recipient: UserRef,
    val nodeId: Long,
    val seq: Long,
    val sender: UserRef,
    val body: ByteArray,
    val createdAtHlc: String
) {
    /**
     * 从当前消息创建一个 [MessageCursor] 游标。
     *
     * @return 包含当前消息 nodeId 和 seq 的游标
     */
    fun cursor(): MessageCursor = MessageCursor(nodeId, seq)
}

/**
 * 瞬时数据包，代表一条不需要持久化的实时消息。
 *
 * 适用于实时通信场景，如聊天消息、实时通知等。
 * 瞬时数据包不会被存储，断开连接后不会重播。
 *
 * @param packetId 数据包 ID（在源节点内唯一）
 * @param sourceNodeId 源节点 ID
 * @param targetNodeId 目标节点 ID
 * @param recipient 数据包接收者
 * @param sender 数据包发送者
 * @param body 数据包内容（原始字节）
 * @param deliveryMode 投递模式
 * @param targetSession 目标会话引用，指定时仅投递到该特定会话
 */
data class Packet(
    val packetId: Long,
    val sourceNodeId: Long,
    val targetNodeId: Long,
    val recipient: UserRef,
    val sender: UserRef,
    val body: ByteArray,
    val deliveryMode: DeliveryMode,
    val targetSession: SessionRef
)

/**
 * 中继接受确认，服务端接收到瞬时数据包后返回的确认信息。
 *
 * 表示服务端已成功接受并转发瞬时数据包（不保证送达）。
 *
 * @param packetId 数据包 ID
 * @param sourceNodeId 源节点 ID
 * @param targetNodeId 目标节点 ID
 * @param recipient 数据包接收者
 * @param deliveryMode 投递模式
 * @param targetSession 目标会话引用
 */
data class RelayAccepted(
    val packetId: Long,
    val sourceNodeId: Long,
    val targetNodeId: Long,
    val recipient: UserRef,
    val deliveryMode: DeliveryMode,
    val targetSession: SessionRef
)

/**
 * 附件类型，定义用户之间的关联关系类型。
 *
 * 每种类型代表一种不同的语义关系，具有不同的访问控制策略。
 *
 * @property wireValue 对应传输层使用的字符串值
 */
enum class AttachmentType(val wireValue: String) {
    /** 频道管理员，拥有频道管理权限 */
    CHANNEL_MANAGER("channel_manager"),
    /** 频道写者，拥有向频道发送消息的权限 */
    CHANNEL_WRITER("channel_writer"),
    /** 频道订阅者，订阅了频道的消息推送 */
    CHANNEL_SUBSCRIPTION("channel_subscription"),
    /** 用户黑名单，被拉黑的用户 */
    USER_BLACKLIST("user_blacklist")
}

/**
 * 用户关系附件，定义两个用户之间的关联关系。
 *
 * 附件系统是 turntf 中实现频道订阅、黑名单、权限管理等功能的统一机制。
 * 每个附件包含关联双方、类型和配置信息。
 *
 * @param owner 附件所有者（关系的主体）
 * @param subject 附件指向的对象（关系的客体）
 * @param attachmentType 附件类型
 * @param configJson 附件配置的 JSON 数据（原始字节）
 * @param attachedAt 附件创建时间（RFC3339 格式）
 * @param deletedAt 附件删除时间（如果已删除，RFC3339 格式）
 * @param originNodeId 源节点 ID
 */
data class Attachment(
    val owner: UserRef,
    val subject: UserRef,
    val attachmentType: AttachmentType,
    val configJson: ByteArray = byteArrayOf(),
    val attachedAt: String = "",
    val deletedAt: String = "",
    val originNodeId: Long = 0
)

/**
 * 频道订阅信息，表示一个用户订阅了某个频道。
 *
 * 频道订阅使用 [AttachmentType.CHANNEL_SUBSCRIPTION] 类型的附件实现。
 *
 * @param subscriber 订阅者（关注频道的用户）
 * @param channel 被订阅的频道
 * @param subscribedAt 订阅时间（RFC3339 格式）
 * @param deletedAt 取消订阅时间（如果已取消，RFC3339 格式）
 * @param originNodeId 源节点 ID
 */
data class Subscription(
    val subscriber: UserRef,
    val channel: UserRef,
    val subscribedAt: String = "",
    val deletedAt: String = "",
    val originNodeId: Long = 0
)

/**
 * 黑名单条目，表示一个用户被另一个用户拉黑。
 *
 * 黑名单使用 [AttachmentType.USER_BLACKLIST] 类型的附件实现。
 *
 * @param owner 黑名单所有者（执行拉黑操作的用户）
 * @param blocked 被拉黑的用户
 * @param blockedAt 拉黑时间（RFC3339 格式）
 * @param deletedAt 解除拉黑时间（如果已解除，RFC3339 格式）
 * @param originNodeId 源节点 ID
 */
data class BlacklistEntry(
    val owner: UserRef,
    val blocked: UserRef,
    val blockedAt: String = "",
    val deletedAt: String = "",
    val originNodeId: Long = 0
)

/**
 * 领域事件，代表集群中发生的一个事件。
 *
 * 事件是 turntf 事件溯源架构的核心组成部分，记录了系统中发生的所有状态变更。
 * 可通过 [TurntfClient.listEvents] 或 HTTP API 按序查询。
 *
 * @param sequence 事件序列号（全局递增）
 * @param eventId 事件 ID
 * @param eventType 事件类型字符串
 * @param aggregate 聚合根类型名称
 * @param aggregateNodeId 聚合根所在节点 ID
 * @param aggregateId 聚合根 ID
 * @param hlc 混合逻辑时钟（HLC）时间戳
 * @param originNodeId 事件产生的源节点 ID
 * @param eventJson 事件数据的 JSON 字节
 */
data class Event(
    val sequence: Long,
    val eventId: Long,
    val eventType: String,
    val aggregate: String,
    val aggregateNodeId: Long,
    val aggregateId: Long,
    val hlc: String,
    val originNodeId: Long,
    val eventJson: ByteArray = byteArrayOf()
)

/**
 * 集群节点信息，表示 turntf 集群中的一个节点。
 *
 * @param nodeId 节点 ID
 * @param isLocal 是否为当前连接的本地节点
 * @param configuredUrl 节点配置的 URL
 * @param source 节点信息的来源（例如 "config"、"discovery" 等）
 */
data class ClusterNode(
    val nodeId: Long,
    val isLocal: Boolean,
    val configuredUrl: String = "",
    val source: String = ""
)

/**
 * 已登录用户信息，表示当前在某个节点上已登录的用户。
 *
 * @param nodeId 所在节点 ID
 * @param userId 用户 ID
 * @param username 显示用户名
 * @param loginName 登录名（如果使用登录名方式登录）
 */
data class LoggedInUser(
    val nodeId: Long,
    val userId: Long,
    val username: String,
    val loginName: String = ""
)

/**
 * 用户会话解析结果，包含用户在线状态和活跃会话信息。
 *
 * 用于查询用户当前的在线状况，包括其在哪些节点上有在线会话，
 * 以及每个会话的传输方式和瞬态能力。
 *
 * @param user 被查询的用户引用
 * @param presence 用户在各节点的在线状态列表
 * @param sessions 用户的所有活跃会话列表
 */
data class ResolvedUserSessions(
    val user: UserRef,
    val presence: List<OnlineNodePresence> = emptyList(),
    val sessions: List<ResolvedSession> = emptyList()
) {
    /**
     * 节点在线状态，表示用户在一个服务节点上的在线情况。
     *
     * @param servingNodeId 服务节点 ID
     * @param sessionCount 该节点上的会话数量
     * @param transportHint 传输方式提示（如 "ws"、"wss" 等）
     */
    data class OnlineNodePresence(
        val servingNodeId: Long,
        val sessionCount: Int,
        val transportHint: String = ""
    )

    /**
     * 已解析的会话信息。
     *
     * @param session 会话引用
     * @param transport 传输方式
     * @param transientCapable 是否支持瞬时消息
     */
    data class ResolvedSession(
        val session: SessionRef,
        val transport: String = "",
        val transientCapable: Boolean = false
    )
}

/**
 * 集群节点的运维状态，用于监控和管理节点运行状况。
 *
 * 包含消息窗口、事件序列、写门控、冲突统计、消息修剪、事件投射、对端状态等运维指标。
 * 适用于集群管理后台或自动化运维工具。
 *
 * @param nodeId 节点 ID
 * @param messageWindowSize 消息窗口大小（当前积压的待处理消息数）
 * @param lastEventSequence 最后事件序列号
 * @param writeGateReady 写门控是否就绪（节点是否可写入）
 * @param conflictTotal 总冲突数
 * @param messageTrim 消息修剪状态
 * @param projection 事件投射状态
 * @param peers 对端节点状态列表（集群中其他节点的连接状态）
 * @param eventLogTrim 事件日志修剪状态
 */
data class OperationsStatus(
    val nodeId: Long,
    val messageWindowSize: Int,
    val lastEventSequence: Long,
    val writeGateReady: Boolean,
    val conflictTotal: Long,
    val messageTrim: MessageTrimStatus = MessageTrimStatus(),
    val projection: ProjectionStatus = ProjectionStatus(),
    val peers: List<PeerStatus> = emptyList(),
    val eventLogTrim: EventLogTrimStatus = EventLogTrimStatus()
) {
    /**
     * 消息修剪状态。
     *
     * @param trimmedTotal 已修剪的消息总数
     * @param lastTrimmedAt 最后一次修剪时间（RFC3339 格式）
     */
    data class MessageTrimStatus(val trimmedTotal: Long = 0, val lastTrimmedAt: String = "")

    /**
     * 事件日志修剪状态。
     *
     * @param trimmedTotal 已修剪的事件总数
     * @param lastTrimmedAt 最后一次修剪时间（RFC3339 格式）
     */
    data class EventLogTrimStatus(val trimmedTotal: Long = 0, val lastTrimmedAt: String = "")

    /**
     * 事件投射状态。
     *
     * @param pendingTotal 待投射的事件总数
     * @param lastFailedAt 最后一次投射失败的时间（RFC3339 格式）
     */
    data class ProjectionStatus(val pendingTotal: Long = 0, val lastFailedAt: String = "")

    /**
     * 对端来源状态，表示对端节点上一个事件来源的同步情况。
     *
     * @param originNodeId 来源节点 ID
     * @param ackedEventId 已确认的事件 ID
     * @param appliedEventId 已应用的事件 ID
     * @param unconfirmedEvents 未确认的事件数
     * @param cursorUpdatedAt 游标更新时间（RFC3339 格式）
     * @param remoteLastEventId 对端最后事件 ID
     * @param pendingCatchup 是否正在追赶同步
     */
    data class PeerOriginStatus(
        val originNodeId: Long,
        val ackedEventId: Long,
        val appliedEventId: Long,
        val unconfirmedEvents: Long,
        val cursorUpdatedAt: String = "",
        val remoteLastEventId: Long,
        val pendingCatchup: Boolean
    )

    /**
     * 对端节点状态，表示集群中对端节点的完整连接和同步状态。
     *
     * @param nodeId 对端节点 ID
     * @param configuredUrl 对端节点配置的 URL
     * @param source 对端信息来源
     * @param discoveredUrl 自动发现的对端 URL
     * @param discoveryState 发现状态
     * @param lastDiscoveredAt 最后发现时间
     * @param lastConnectedAt 最后连接时间
     * @param lastDiscoveryError 最后的发现错误信息
     * @param connected 是否已连接
     * @param sessionDirection 会话方向（"inbound"/"outbound"）
     * @param origins 各事件来源的同步状态列表
     * @param pendingSnapshotPartitions 待处理的快照分区数
     * @param remoteSnapshotVersion 对端快照版本
     * @param remoteMessageWindowSize 对端消息窗口大小
     * @param clockOffsetMs 时钟偏移（毫秒）
     * @param lastClockSync 最后时钟同步时间
     * @param snapshotDigestsSentTotal 已发送的快照摘要总数
     * @param snapshotDigestsReceivedTotal 已接收的快照摘要总数
     * @param snapshotChunksSentTotal 已发送的快照块总数
     * @param snapshotChunksReceivedTotal 已接收的快照块总数
     * @param lastSnapshotDigestAt 最后快照摘要时间
     * @param lastSnapshotChunkAt 最后快照块时间
     */
    data class PeerStatus(
        val nodeId: Long,
        val configuredUrl: String = "",
        val source: String = "",
        val discoveredUrl: String = "",
        val discoveryState: String = "",
        val lastDiscoveredAt: String = "",
        val lastConnectedAt: String = "",
        val lastDiscoveryError: String = "",
        val connected: Boolean = false,
        val sessionDirection: String = "",
        val origins: List<PeerOriginStatus> = emptyList(),
        val pendingSnapshotPartitions: Int = 0,
        val remoteSnapshotVersion: String = "",
        val remoteMessageWindowSize: Int = 0,
        val clockOffsetMs: Long = 0,
        val lastClockSync: String = "",
        val snapshotDigestsSentTotal: Long = 0,
        val snapshotDigestsReceivedTotal: Long = 0,
        val snapshotChunksSentTotal: Long = 0,
        val snapshotChunksReceivedTotal: Long = 0,
        val lastSnapshotDigestAt: String = "",
        val lastSnapshotChunkAt: String = ""
    )
}

/**
 * 删除用户操作的结果。
 *
 * @param status 操作状态字符串（例如 "deleted"）
 * @param user 被删除用户的引用
 */
data class DeleteUserResult(val status: String, val user: UserRef)

/**
 * 登录成功后的信息。
 *
 * @param user 已登录用户的详细信息
 * @param protocolVersion 服务端协议版本号
 * @param sessionRef 当前会话的引用
 */
data class LoginInfo(
    val user: User,
    val protocolVersion: String,
    val sessionRef: SessionRef
)

/**
 * 发送持久化消息的输入参数。
 *
 * @param target 消息目标用户
 * @param body 消息内容（原始字节）
 */
data class SendMessageInput(val target: UserRef, val body: ByteArray)

/**
 * 发送瞬时数据包的输入参数。
 *
 * @param target 数据包目标用户
 * @param body 数据包内容（原始字节）
 * @param deliveryMode 投递模式
 * @param targetSession 可选的目标会话引用，指定时仅投递到该特定会话
 */
data class SendPacketInput(
    val target: UserRef,
    val body: ByteArray,
    val deliveryMode: DeliveryMode,
    val targetSession: SessionRef? = null
)

/**
 * 创建新用户或频道的请求。
 *
 * `loginName` 是可选的。留空表示"创建时不绑定登录名"。
 *
 * @param username 用户名
 * @param password 可选的密码，如果为 null 则创建的用户无密码
 * @param profileJson 用户资料的 JSON 数据（原始字节）
 * @param role 用户角色（例如 "user"、"channel"）
 * @param loginName 可选的登录名
 */
data class CreateUserRequest(
    val username: String,
    val password: PasswordInput? = null,
    val profileJson: ByteArray = byteArrayOf(),
    val role: String,
    val loginName: String = ""
)

/**
 * 部分更新用户信息的请求。
 *
 * `null` 表示"保持字段不变"。对于 `loginName`，空字符串表示"解绑当前登录名"，
 * 与服务端的双轨登录语义保持一致。
 *
 * @param username 新的用户名，null 表示不更新
 * @param password 新的密码，null 表示不更新
 * @param profileJson 新的用户资料 JSON，null 表示不更新
 * @param role 新的用户角色，null 表示不更新
 * @param loginName 新的登录名，null 表示不更新，空字符串表示解绑
 */
data class UpdateUserRequest(
    val username: String? = null,
    val password: PasswordInput? = null,
    val profileJson: ByteArray? = null,
    val role: String? = null,
    val loginName: String? = null
)

/**
 * 持久化游标存储接口，用于 WebSocket 重连时的消息去重和重播抑制。
 *
 * 存储实现应跨重启保持持久化消息游标集的稳定，
 * 以便在断线重连时将已处理的持久化消息游标传递给服务端，避免重复投递。
 *
 * @see MemoryCursorStore
 * @see Config.cursorStore
 */
interface CursorStore {
    /**
     * 加载已处理过的消息游标列表。
     *
     * 返回的游标会在下一次 WebSocket 登录时通过 LoginRequest.seen_messages 发送给服务端。
     * 存储应跨应用重启保持已处理的持久化消息游标集。
     *
     * @return 已处理的消息游标列表
     */
    suspend fun loadSeenMessages(): List<MessageCursor>

    /**
     * 持久化一条消息。
     *
     * @param message 需要持久化的消息
     */
    suspend fun saveMessage(message: Message)

    /**
     * 持久化一个消息游标。
     *
     * @param cursor 需要持久化的游标
     */
    suspend fun saveCursor(cursor: MessageCursor)
}

/**
 * 内存 [CursorStore] 实现，适用于测试、演示和短生命周期进程。
 *
 * 数据仅保存在内存中，应用重启后数据丢失。
 * 采用 LinkedHashMap 保证插入顺序，并通过 Mutex 保证线程安全。
 */
class MemoryCursorStore : CursorStore {
    private val mutex = Mutex()
    private val messages = linkedMapOf<MessageCursor, Message>()
    private val order = mutableListOf<MessageCursor>()

    override suspend fun loadSeenMessages(): List<MessageCursor> = mutex.withLock { order.toList() }

    override suspend fun saveMessage(message: Message) {
        mutex.withLock {
            messages[message.cursor()] = message
        }
    }

    override suspend fun saveCursor(cursor: MessageCursor) {
        mutex.withLock {
            // 游标可以在没有消息载荷的情况下被确认（在自定义存储实现中），
            // 因此内存变体保留了一个占位条目以保持该契约，
            // 并使 loadSeenMessages() 和 messages 在逻辑上保持对齐。
            messages.putIfAbsent(cursor, Message(UserRef(0, 0), cursor.nodeId, cursor.seq, UserRef(0, 0), byteArrayOf(), ""))
            if (cursor !in order) {
                order += cursor
            }
        }
    }
}

/**
 * [TurntfClient] 发布的实时事件流。
 *
 * 投递事件仅在 SDK 完成相应协议帧所需的记账工作后才会发布。
 * 事件通过 [TurntfClient.events] 共享流（SharedFlow）对外暴露。
 *
 * @see TurntfClient.events
 */
sealed interface ClientEvent {
    /** 登录成功事件，包含登录后的用户和会话信息 */
    data class Login(val info: LoginInfo) : ClientEvent
    /** 收到持久化消息事件 */
    data class MessageReceived(val message: Message) : ClientEvent
    /** 收到瞬时数据包事件 */
    data class PacketReceived(val packet: Packet) : ClientEvent
    /** SDK 内部错误事件，包含异常的详细信息 */
    data class Error(val error: Throwable) : ClientEvent
    /** 连接断开事件，包含断开原因 */
    data class Disconnect(val error: Throwable) : ClientEvent
}

/**
 * 连接生命周期状态，通过 [TurntfClient.connectionState] 对外暴露。
 *
 * @see TurntfClient.connectionState
 */
enum class ConnectionState {
    /** 未连接状态 */
    DISCONNECTED,
    /** 正在建立连接（进行 WebSocket 握手和登录） */
    CONNECTING,
    /** 已连接并通过身份认证 */
    CONNECTED,
    /** 连接已关闭（不再可用） */
    CLOSED
}

/**
 * SDK 级别的协议、连接和服务器错误的基础异常类型。
 *
 * 所有 turntf SDK 抛出的异常都继承自此类型。
 *
 * @param message 错误描述信息
 * @param cause 可选的原始异常原因
 */
open class TurntfException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * 服务器返回的错误，包含错误码和请求 ID。
 *
 * 当服务端返回错误响应时抛出此异常。
 *
 * @param code 错误码字符串（例如 "unauthorized"、"not_found" 等）
 * @param message 错误描述信息
 * @param requestId 对应的请求 ID（0 表示该错误不与特定请求关联）
 */
class ServerError(
    val code: String,
    message: String,
    val requestId: Long
) : TurntfException(
    if (requestId == 0L) "turntf server error: $code ($message)"
    else "turntf server error: $code ($message), request_id=$requestId"
) {
    /**
     * 检查此错误是否为未授权错误。
     *
     * @return 如果错误码为 "unauthorized" 则返回 true
     */
    fun unauthorized(): Boolean = code == "unauthorized"
}

/**
 * 协议错误，表示 SDK 与服务器之间的协议交互出现问题。
 *
 * 例如收到不符合预期的消息类型、字段校验失败等。
 *
 * @param message 协议错误的描述信息
 */
class ProtocolError(message: String) : TurntfException("turntf protocol error: $message")

/**
 * 连接错误，表示 WebSocket 连接过程中的网络层错误。
 *
 * @param op 发生错误时的操作描述（例如 "dial"）
 * @param cause 原始的 I/O 异常
 */
class ConnectionError(val op: String, cause: Throwable) : TurntfException("turntf connection error during $op: ${cause.message}", cause)
