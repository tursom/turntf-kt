package io.github.tursom.turntf.kotlin.internal

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.tursom.turntf.kotlin.Attachment
import io.github.tursom.turntf.kotlin.AttachmentType
import io.github.tursom.turntf.kotlin.BlacklistEntry
import io.github.tursom.turntf.kotlin.ClusterNode
import io.github.tursom.turntf.kotlin.DeleteUserResult
import io.github.tursom.turntf.kotlin.DeliveryMode
import io.github.tursom.turntf.kotlin.Event
import io.github.tursom.turntf.kotlin.LoggedInUser
import io.github.tursom.turntf.kotlin.LoginInfo
import io.github.tursom.turntf.kotlin.Message
import io.github.tursom.turntf.kotlin.MessageCursor
import io.github.tursom.turntf.kotlin.OperationsStatus
import io.github.tursom.turntf.kotlin.Packet
import io.github.tursom.turntf.kotlin.PasswordInput
import io.github.tursom.turntf.kotlin.ProtocolError
import io.github.tursom.turntf.kotlin.RelayAccepted
import io.github.tursom.turntf.kotlin.ResolvedUserSessions
import io.github.tursom.turntf.kotlin.SessionRef
import io.github.tursom.turntf.kotlin.Subscription
import io.github.tursom.turntf.kotlin.User
import io.github.tursom.turntf.kotlin.UserMetadata
import io.github.tursom.turntf.kotlin.UserMetadataScanResult
import io.github.tursom.turntf.kotlin.UserRef
import notifier.client.v1.Client
import com.google.protobuf.ByteString
import java.net.URI

/**
 * Jackson ObjectMapper 实例，用于 JSON 解析和序列化。
 *
 * 自动注册 Kotlin 模块以支持 Kotlin 数据类的序列化。
 */
val mapper: ObjectMapper = ObjectMapper().registerKotlinModule()

/**
 * 验证 baseUrl 不为空。
 *
 * @param baseUrl 待验证的基础 URL
 * @throws IllegalArgumentException 如果 baseUrl 为空或空白
 */
fun validateBaseUrl(baseUrl: String) {
    require(baseUrl.isNotBlank()) { "baseUrl is required" }
}

/**
 * 验证 [UserRef] 的节点 ID 和用户 ID 均大于 0。
 *
 * @param ref 待验证的用户引用
 * @param field 字段名前缀，用于错误消息
 * @throws IllegalArgumentException 如果 nodeId 或 userId 不合法
 */
fun validateUserRef(ref: UserRef, field: String) {
    require(ref.nodeId > 0) { "$field.nodeId is required" }
    require(ref.userId > 0) { "$field.userId is required" }
}

/**
 * 验证元数据键名不为空。
 *
 * @param key 待验证的键名
 * @param field 字段名前缀，用于错误消息
 * @throws IllegalArgumentException 如果键名为空
 */
fun validateUserMetadataKey(key: String, field: String) {
    require(key.isNotEmpty()) { "$field is required" }
}

/**
 * 验证 [SessionRef] 的 servingNodeId 大于 0 且 sessionId 不为空。
 *
 * @param ref 待验证的会话引用
 * @param field 字段名前缀，用于错误消息
 * @throws IllegalArgumentException 如果会话引用不合法
 */
fun validateSessionRef(ref: SessionRef, field: String) {
    require(ref.servingNodeId > 0) { "$field.servingNodeId is required" }
    require(ref.sessionId.isNotEmpty()) { "$field.sessionId is required" }
}

/**
 * 验证投递模式为有效的瞬时投递模式（BEST_EFFORT 或 ROUTE_RETRY）。
 *
 * @param mode 待验证的投递模式
 * @throws IllegalArgumentException 如果投递模式为 UNSPECIFIED
 */
fun validateDeliveryMode(mode: DeliveryMode) {
    require(mode == DeliveryMode.BEST_EFFORT || mode == DeliveryMode.ROUTE_RETRY) { "invalid deliveryMode $mode" }
}

/**
 * 确保一个 Long 值在无符号范围内（即 >= 0）。
 *
 * Proto 使用 uint64 类型，而 Kotlin 使用有符号 Long，此函数用于在从 Proto 转换时进行校验。
 *
 * @param value 待检查的值
 * @param field 字段名，用于错误消息
 * @return 如果校验通过，返回原值
 * @throws ProtocolError 如果值小于 0
 */
fun requireUnsigned(value: Long, field: String): Long {
    if (value < 0) {
        throw ProtocolError("$field exceeds signed long range")
    }
    return value
}

/**
 * 将 HTTP baseUrl 转换为对应的 WebSocket URL。
 *
 * 根据是否启用实时流选择合适的路径后缀：
 * - 实时模式：/ws/realtime
 * - 客户端模式：/ws/client
 *
 * @param baseUrl HTTP 基础 URL
 * @param realtime 是否使用实时 WebSocket 端点
 * @return 完整的 WebSocket URL 字符串
 * @throws IllegalStateException 如果基础 URL 的 scheme 不受支持
 */
fun websocketUrl(baseUrl: String, realtime: Boolean): String {
    val uri = URI(baseUrl)
    val scheme = when (uri.scheme) {
        "http" -> "ws"
        "https" -> "wss"
        "ws", "wss" -> uri.scheme
        else -> error("unsupported base URL scheme \"${uri.scheme}\"")
    }
    val suffix = if (realtime) "/ws/realtime" else "/ws/client"
    val path = if (uri.path.isNullOrBlank() || uri.path == "/") suffix else uri.path.removeSuffix("/") + suffix
    return URI(scheme, uri.userInfo, uri.host, uri.port, path, null, null).toString()
}

// REST 端点在更大的 JSON 载荷中嵌入 JSON 子文档；空字节被视为空对象，
// 以便附件/profile 辅助函数可以复用相同的序列化路径。

/**
 * 将字节数组解析为 JsonNode。
 *
 * 如果字节数组为空，则返回一个空对象节点，使调用者可以保持统一的 JSON 解析路径。
 *
 * @param value 待解析的字节数组
 * @return 解析后的 JsonNode
 */
fun parseJsonBytes(value: ByteArray): JsonNode = if (value.isEmpty()) mapper.createObjectNode() else mapper.readTree(value)

/**
 * 从 JsonNode 中安全地提取指定字段的字符串值。
 *
 * @param node JSON 节点
 * @param field 字段名
 * @return 字符串值，如果字段不存在或为 null 则返回空字符串
 */
fun text(node: JsonNode, field: String): String = node.path(field).takeUnless { it.isMissingNode || it.isNull }?.asText("") ?: ""

/**
 * 从 JsonNode 中安全地提取指定字段的整数值。
 *
 * @param node JSON 节点
 * @param field 字段名
 * @return 整数值，如果字段不存在或为 null 则返回 0
 */
fun intValue(node: JsonNode, field: String): Int = node.path(field).takeUnless { it.isMissingNode || it.isNull }?.asInt() ?: 0

/**
 * 从 JsonNode 中安全地提取指定字段的长整数值。
 *
 * @param node JSON 节点
 * @param field 字段名
 * @return 长整数值，如果字段不存在或为 null 则返回 0L
 */
fun longValue(node: JsonNode, field: String): Long = node.path(field).takeUnless { it.isMissingNode || it.isNull }?.asLong() ?: 0L

/**
 * 从 JsonNode 中安全地提取指定字段的布尔值。
 *
 * @param node JSON 节点
 * @param field 字段名
 * @return 布尔值，如果字段不存在或为 null 则返回 false
 */
fun boolValue(node: JsonNode, field: String): Boolean = node.path(field).takeUnless { it.isMissingNode || it.isNull }?.asBoolean() ?: false

/**
 * 从 JsonNode 中安全地提取指定字段的字节数组值。
 *
 * @param node JSON 节点
 * @param field 字段名
 * @return 字节数组，如果字段不存在或为 null 则返回空数组
 */
fun bytesValue(node: JsonNode, field: String): ByteArray = node.path(field).takeUnless { it.isMissingNode || it.isNull }?.binaryValue() ?: byteArrayOf()

private fun userRefNode(node: JsonNode): UserRef = UserRef(longValue(node, "node_id"), longValue(node, "user_id"))

/**
 * 从 HTTP JSON 响应中解析 [User] 对象。
 *
 * 兼容新旧版本的 HTTP 响应格式：新版使用 `profile` 字段（嵌入式 JSON），
 * 旧版使用 `profile_json` 字段（原始字节或 base64），统一归一化为 `profileJson`。
 *
 * @param node HTTP 响应中的 JSON 节点
 * @return 解析后的 User 对象
 */
fun userFromHttp(node: JsonNode): User {
    val profile = when {
        // 新/旧 HTTP 响应可能暴露解析后的 profile 内容或原始的 *_json 字节。
        // 将两者都归一化为 profileJson 使公开模型与传输无关。
        node.has("profile") -> mapper.writeValueAsBytes(node.path("profile"))
        node.has("profile_json") -> bytesValue(node, "profile_json")
        else -> byteArrayOf()
    }
    return User(
        nodeId = longValue(node, "node_id"),
        userId = longValue(node, "user_id"),
        username = text(node, "username"),
        role = text(node, "role"),
        profileJson = profile,
        systemReserved = boolValue(node, "system_reserved"),
        createdAt = text(node, "created_at"),
        updatedAt = text(node, "updated_at"),
        originNodeId = longValue(node, "origin_node_id"),
        loginName = text(node, "login_name")
    )
}

/**
 * 从 HTTP JSON 响应中解析 [Message] 对象。
 *
 * @param node HTTP 响应中的 JSON 节点
 * @return 解析后的 Message 对象
 */
fun messageFromHttp(node: JsonNode): Message = Message(
    recipient = userRefNode(node.path("recipient")),
    nodeId = longValue(node, "node_id"),
    seq = longValue(node, "seq"),
    sender = userRefNode(node.path("sender")),
    body = bytesValue(node, "body"),
    createdAtHlc = text(node, "created_at_hlc").ifEmpty { text(node, "created_at") }
)

/**
 * 从 HTTP JSON 响应中解析 [Attachment] 对象。
 *
 * @param node HTTP 响应中的 JSON 节点
 * @return 解析后的 Attachment 对象
 */
fun attachmentFromHttp(node: JsonNode): Attachment = Attachment(
    owner = userRefNode(node.path("owner")),
    subject = userRefNode(node.path("subject")),
    attachmentType = AttachmentType.valueOf(text(node, "attachment_type").uppercase()),
    configJson = bytesValue(node, "config_json"),
    attachedAt = text(node, "attached_at"),
    deletedAt = text(node, "deleted_at"),
    originNodeId = longValue(node, "origin_node_id")
)

/**
 * 从 HTTP JSON 响应中解析 [UserMetadata] 对象。
 *
 * @param node HTTP 响应中的 JSON 节点
 * @return 解析后的 UserMetadata 对象
 */
fun userMetadataFromHttp(node: JsonNode): UserMetadata = UserMetadata(
    owner = userRefNode(node.path("owner")),
    key = text(node, "key"),
    value = bytesValue(node, "value"),
    updatedAt = text(node, "updated_at"),
    deletedAt = text(node, "deleted_at"),
    expiresAt = text(node, "expires_at"),
    originNodeId = longValue(node, "origin_node_id")
)

/**
 * 将 [Attachment] 转换为 [BlacklistEntry]。
 *
 * 黑名单使用 [AttachmentType.USER_BLACKLIST] 类型的附件实现，
 * 此函数从附件中提取相关信息构造黑名单条目。
 *
 * @param attachment 用户黑名单类型的附件
 * @return 转换后的黑名单条目
 */
fun blacklistEntryFromAttachment(attachment: Attachment): BlacklistEntry = BlacklistEntry(
    owner = attachment.owner,
    blocked = attachment.subject,
    blockedAt = attachment.attachedAt,
    deletedAt = attachment.deletedAt,
    originNodeId = attachment.originNodeId
)

/**
 * 从 HTTP JSON 响应中解析 [ClusterNode] 对象。
 *
 * @param node HTTP 响应中的 JSON 节点
 * @return 解析后的 ClusterNode 对象
 */
fun clusterNodeFromHttp(node: JsonNode): ClusterNode = ClusterNode(
    nodeId = longValue(node, "node_id"),
    isLocal = boolValue(node, "is_local"),
    configuredUrl = text(node, "configured_url"),
    source = text(node, "source")
)

/**
 * 从 HTTP JSON 响应中解析 [LoggedInUser] 对象。
 *
 * @param node HTTP 响应中的 JSON 节点
 * @return 解析后的 LoggedInUser 对象
 */
fun loggedInUserFromHttp(node: JsonNode): LoggedInUser = LoggedInUser(
    nodeId = longValue(node, "node_id"),
    userId = longValue(node, "user_id"),
    username = text(node, "username"),
    loginName = text(node, "login_name")
)

// HTTP 处理程序在裸数组和 {"items": [...]} 封装之间存在不一致，
// 因此适配器集中处理这种容忍性，而不是将形状检查分散到调用方。

/**
 * 从 JSON 节点中提取条目列表节点。
 *
 * 兼容不同格式的 HTTP 响应：有些返回裸数组，有些返回 `{"items": [...]}` 封装格式。
 *
 * @param node HTTP 响应节点
 * @param field 封装格式中的字段名
 * @return 如果是数组则直接返回，否则返回指定字段的子节点
 */
fun itemsNode(node: JsonNode, field: String): JsonNode = if (node.isArray) node else node.path(field)

/**
 * 从 HTTP JSON 响应中解析 [UserMetadataScanResult] 对象。
 *
 * @param node HTTP 响应中的 JSON 节点
 * @return 解析后的 UserMetadataScanResult 对象
 */
fun userMetadataScanResultFromHttp(node: JsonNode): UserMetadataScanResult {
    val items = itemsNode(node, "items").map(::userMetadataFromHttp)
    return UserMetadataScanResult(
        items = items,
        count = if (node.isArray) items.size else intValue(node, "count"),
        nextAfter = text(node, "next_after")
    )
}

/**
 * 从 HTTP JSON 响应中解析 [Event] 对象。
 *
 * @param node HTTP 响应中的 JSON 节点
 * @return 解析后的 Event 对象
 */
fun eventFromHttp(node: JsonNode): Event = Event(
    sequence = longValue(node, "sequence"),
    eventId = longValue(node, "event_id"),
    eventType = text(node, "event_type"),
    aggregate = text(node, "aggregate"),
    aggregateNodeId = longValue(node, "aggregate_node_id"),
    aggregateId = longValue(node, "aggregate_id"),
    hlc = text(node, "hlc"),
    originNodeId = longValue(node, "origin_node_id"),
    eventJson = bytesValue(node, "event_json")
)

/**
 * 从 HTTP JSON 响应中解析 [DeleteUserResult] 对象。
 *
 * @param node HTTP 响应中的 JSON 节点
 * @return 解析后的 DeleteUserResult 对象
 */
fun deleteUserResultFromHttp(node: JsonNode): DeleteUserResult = DeleteUserResult(
    status = text(node, "status"),
    user = UserRef(longValue(node, "node_id"), longValue(node, "user_id"))
)

/**
 * 从 HTTP JSON 响应中解析 [OperationsStatus] 对象及其所有嵌套子类型。
 *
 * @param node HTTP 响应中的 JSON 节点
 * @return 解析后的 OperationsStatus 对象
 */
fun operationsStatusFromHttp(node: JsonNode): OperationsStatus = OperationsStatus(
    nodeId = longValue(node, "node_id"),
    messageWindowSize = intValue(node, "message_window_size"),
    lastEventSequence = longValue(node, "last_event_sequence"),
    writeGateReady = boolValue(node, "write_gate_ready"),
    conflictTotal = longValue(node, "conflict_total"),
    messageTrim = OperationsStatus.MessageTrimStatus(
        trimmedTotal = longValue(node.path("message_trim"), "trimmed_total"),
        lastTrimmedAt = text(node.path("message_trim"), "last_trimmed_at")
    ),
    projection = OperationsStatus.ProjectionStatus(
        pendingTotal = longValue(node.path("projection"), "pending_total"),
        lastFailedAt = text(node.path("projection"), "last_failed_at")
    ),
    peers = node.path("peers").map { peer ->
        OperationsStatus.PeerStatus(
            nodeId = longValue(peer, "node_id"),
            configuredUrl = text(peer, "configured_url"),
            source = text(peer, "source"),
            discoveredUrl = text(peer, "discovered_url"),
            discoveryState = text(peer, "discovery_state"),
            lastDiscoveredAt = text(peer, "last_discovered_at"),
            lastConnectedAt = text(peer, "last_connected_at"),
            lastDiscoveryError = text(peer, "last_discovery_error"),
            connected = boolValue(peer, "connected"),
            sessionDirection = text(peer, "session_direction"),
            origins = peer.path("origins").map { origin ->
                OperationsStatus.PeerOriginStatus(
                    originNodeId = longValue(origin, "origin_node_id"),
                    ackedEventId = longValue(origin, "acked_event_id"),
                    appliedEventId = longValue(origin, "applied_event_id"),
                    unconfirmedEvents = longValue(origin, "unconfirmed_events"),
                    cursorUpdatedAt = text(origin, "cursor_updated_at"),
                    remoteLastEventId = longValue(origin, "remote_last_event_id"),
                    pendingCatchup = boolValue(origin, "pending_catchup")
                )
            },
            pendingSnapshotPartitions = intValue(peer, "pending_snapshot_partitions"),
            remoteSnapshotVersion = text(peer, "remote_snapshot_version"),
            remoteMessageWindowSize = intValue(peer, "remote_message_window_size"),
            clockOffsetMs = longValue(peer, "clock_offset_ms"),
            lastClockSync = text(peer, "last_clock_sync"),
            snapshotDigestsSentTotal = longValue(peer, "snapshot_digests_sent_total"),
            snapshotDigestsReceivedTotal = longValue(peer, "snapshot_digests_received_total"),
            snapshotChunksSentTotal = longValue(peer, "snapshot_chunks_sent_total"),
            snapshotChunksReceivedTotal = longValue(peer, "snapshot_chunks_received_total"),
            lastSnapshotDigestAt = text(peer, "last_snapshot_digest_at"),
            lastSnapshotChunkAt = text(peer, "last_snapshot_chunk_at")
        )
    },
    eventLogTrim = OperationsStatus.EventLogTrimStatus(
        trimmedTotal = longValue(node.path("event_log_trim"), "trimmed_total"),
        lastTrimmedAt = text(node.path("event_log_trim"), "last_trimmed_at")
    )
)

// === Proto 转换函数 ===

/**
 * 将 [UserRef] 转换为 Proto 的 Client.UserRef。
 *
 * @param value Kotlin 侧的用户引用
 * @return Proto 侧的用户引用
 */
fun userRefToProto(value: UserRef): Client.UserRef = Client.UserRef.newBuilder().setNodeId(value.nodeId).setUserId(value.userId).build()

/**
 * 将 Proto 的 Client.UserRef 转换为 [UserRef]。
 *
 * @param value Proto 侧的用户引用，为 null 时返回零值
 * @return Kotlin 侧的用户引用
 */
fun userRefFromProto(value: Client.UserRef?): UserRef = if (value == null) UserRef(0, 0) else UserRef(value.nodeId, value.userId)

/**
 * 将 [SessionRef] 转换为 Proto 的 Client.SessionRef。
 *
 * @param value Kotlin 侧的会话引用
 * @return Proto 侧的会话引用
 */
fun sessionRefToProto(value: SessionRef): Client.SessionRef = Client.SessionRef.newBuilder().setServingNodeId(value.servingNodeId).setSessionId(value.sessionId).build()

/**
 * 将 Proto 的 Client.SessionRef 转换为 [SessionRef]。
 *
 * @param value Proto 侧的会话引用，为 null 时返回零值
 * @return Kotlin 侧的会话引用
 */
fun sessionRefFromProto(value: Client.SessionRef?): SessionRef = if (value == null) SessionRef(0, "") else SessionRef(value.servingNodeId, value.sessionId)

/**
 * 将 [MessageCursor] 转换为 Proto 的 Client.MessageCursor。
 *
 * @param value Kotlin 侧的消息游标
 * @return Proto 侧的消息游标
 */
fun cursorToProto(value: MessageCursor): Client.MessageCursor = Client.MessageCursor.newBuilder().setNodeId(value.nodeId).setSeq(value.seq).build()

/**
 * 将 [DeliveryMode] 转换为 Proto 的 Client.ClientDeliveryMode。
 *
 * @param value Kotlin 侧的投递模式
 * @return Proto 侧的投递模式枚举
 */
fun deliveryModeToProto(value: DeliveryMode): Client.ClientDeliveryMode = when (value) {
    DeliveryMode.BEST_EFFORT -> Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_BEST_EFFORT
    DeliveryMode.ROUTE_RETRY -> Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_ROUTE_RETRY
    DeliveryMode.UNSPECIFIED -> Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_UNSPECIFIED
}

/**
 * 将 Proto 的 Client.ClientDeliveryMode 转换为 [DeliveryMode]。
 *
 * @param value Proto 侧的投递模式枚举
 * @return Kotlin 侧的投递模式
 */
fun deliveryModeFromProto(value: Client.ClientDeliveryMode): DeliveryMode = when (value) {
    Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_BEST_EFFORT -> DeliveryMode.BEST_EFFORT
    Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_ROUTE_RETRY -> DeliveryMode.ROUTE_RETRY
    else -> DeliveryMode.UNSPECIFIED
}

/**
 * 将 [AttachmentType] 转换为 Proto 的 Client.AttachmentType。
 *
 * @param value Kotlin 侧的附件类型，为 null 时返回 UNSPECIFIED
 * @return Proto 侧的附件类型枚举
 */
fun attachmentTypeToProto(value: AttachmentType?): Client.AttachmentType = when (value) {
    AttachmentType.CHANNEL_MANAGER -> Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_MANAGER
    AttachmentType.CHANNEL_WRITER -> Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_WRITER
    AttachmentType.CHANNEL_SUBSCRIPTION -> Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION
    AttachmentType.USER_BLACKLIST -> Client.AttachmentType.ATTACHMENT_TYPE_USER_BLACKLIST
    null -> Client.AttachmentType.ATTACHMENT_TYPE_UNSPECIFIED
}

// 公开的 Kotlin 模型没有 UNKNOWN 附件类型，因此向前不兼容的 proto 枚举值
// 会被折叠为具体默认值，而不是通过 API 暴露 null。

/**
 * 将 Proto 的 Client.AttachmentType 转换为 [AttachmentType]。
 *
 * 不兼容的 Proto 枚举值会被折叠为 [AttachmentType.CHANNEL_MANAGER]，
 * 而不是返回 null。
 *
 * @param value Proto 侧的附件类型枚举
 * @return Kotlin 侧的附件类型
 */
fun attachmentTypeFromProto(value: Client.AttachmentType): AttachmentType = when (value) {
    Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_MANAGER -> AttachmentType.CHANNEL_MANAGER
    Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_WRITER -> AttachmentType.CHANNEL_WRITER
    Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION -> AttachmentType.CHANNEL_SUBSCRIPTION
    Client.AttachmentType.ATTACHMENT_TYPE_USER_BLACKLIST -> AttachmentType.USER_BLACKLIST
    else -> AttachmentType.CHANNEL_MANAGER
}

/**
 * 将 Proto 的 Client.User 转换为 [User]。
 *
 * @param value Proto 侧的用户，为 null 时返回空用户
 * @return Kotlin 侧的用户
 */
fun userFromProto(value: Client.User?): User = if (value == null) User(0, 0, "", "") else User(
    nodeId = value.nodeId,
    userId = value.userId,
    username = value.username,
    role = value.role,
    profileJson = value.profileJson.toByteArray(),
    systemReserved = value.systemReserved,
    createdAt = value.createdAt,
    updatedAt = value.updatedAt,
    originNodeId = value.originNodeId,
    loginName = value.loginName
)

/**
 * 将 Proto 的 Client.Message 转换为 [Message]。
 *
 * @param value Proto 侧的消息，为 null 时返回空消息
 * @return Kotlin 侧的消息
 */
fun messageFromProto(value: Client.Message?): Message = if (value == null) Message(UserRef(0, 0), 0, 0, UserRef(0, 0), byteArrayOf(), "") else Message(
    recipient = userRefFromProto(value.recipient),
    nodeId = value.nodeId,
    seq = value.seq,
    sender = userRefFromProto(value.sender),
    body = value.body.toByteArray(),
    createdAtHlc = value.createdAtHlc
)

/**
 * 将 Proto 的 Client.Packet 转换为 [Packet]。
 *
 * @param value Proto 侧的数据包
 * @return Kotlin 侧的数据包
 */
fun packetFromProto(value: Client.Packet): Packet = Packet(
    packetId = requireUnsigned(value.packetId, "packet_id"),
    sourceNodeId = value.sourceNodeId,
    targetNodeId = value.targetNodeId,
    recipient = userRefFromProto(value.recipient),
    sender = userRefFromProto(value.sender),
    body = value.body.toByteArray(),
    deliveryMode = deliveryModeFromProto(value.deliveryMode),
    targetSession = sessionRefFromProto(value.targetSession)
)

/**
 * 将 Proto 的 Client.TransientAccepted 转换为 [RelayAccepted]。
 *
 * @param value Proto 侧的中继接受确认
 * @return Kotlin 侧的中继接受确认
 */
fun relayAcceptedFromProto(value: Client.TransientAccepted): RelayAccepted = RelayAccepted(
    packetId = requireUnsigned(value.packetId, "packet_id"),
    sourceNodeId = value.sourceNodeId,
    targetNodeId = value.targetNodeId,
    recipient = userRefFromProto(value.recipient),
    deliveryMode = deliveryModeFromProto(value.deliveryMode),
    targetSession = sessionRefFromProto(value.targetSession)
)

/**
 * 将 Proto 的 Client.Attachment 转换为 [Attachment]。
 *
 * @param value Proto 侧的附件
 * @return Kotlin 侧的附件
 */
fun attachmentFromProto(value: Client.Attachment): Attachment = Attachment(
    owner = userRefFromProto(value.owner),
    subject = userRefFromProto(value.subject),
    attachmentType = attachmentTypeFromProto(value.attachmentType),
    configJson = value.configJson.toByteArray(),
    attachedAt = value.attachedAt,
    deletedAt = value.deletedAt,
    originNodeId = value.originNodeId
)

/**
 * 将 Proto 的 Client.UserMetadata 转换为 [UserMetadata]。
 *
 * @param value Proto 侧的元数据，为 null 时返回空元数据
 * @return Kotlin 侧的元数据
 */
fun userMetadataFromProto(value: Client.UserMetadata?): UserMetadata = if (value == null) UserMetadata(UserRef(0, 0), "") else UserMetadata(
    owner = userRefFromProto(value.owner),
    key = value.key,
    value = value.value.toByteArray(),
    updatedAt = value.updatedAt,
    deletedAt = value.deletedAt,
    expiresAt = value.expiresAt,
    originNodeId = value.originNodeId
)

/**
 * 将 Proto 的 Client.Event 转换为 [Event]。
 *
 * @param value Proto 侧的事件
 * @return Kotlin 侧的事件
 */
fun eventFromProto(value: Client.Event): Event = Event(
    sequence = value.sequence,
    eventId = value.eventId,
    eventType = value.eventType,
    aggregate = value.aggregate,
    aggregateNodeId = value.aggregateNodeId,
    aggregateId = value.aggregateId,
    hlc = value.hlc,
    originNodeId = value.originNodeId,
    eventJson = value.eventJson.toByteArray()
)

/**
 * 将 Proto 的 Client.ClusterNode 转换为 [ClusterNode]。
 *
 * @param value Proto 侧的集群节点
 * @return Kotlin 侧的集群节点
 */
fun clusterNodeFromProto(value: Client.ClusterNode): ClusterNode = ClusterNode(value.nodeId, value.isLocal, value.configuredUrl, value.source)

/**
 * 将 Proto 的 Client.LoggedInUser 转换为 [LoggedInUser]。
 *
 * @param value Proto 侧的已登录用户
 * @return Kotlin 侧的已登录用户
 */
fun loggedInUserFromProto(value: Client.LoggedInUser): LoggedInUser =
    LoggedInUser(value.nodeId, value.userId, value.username, value.loginName)

/**
 * 将 Proto 的 Client.ResolveUserSessionsResponse 转换为 [ResolvedUserSessions]。
 *
 * 在线状态按服务节点分组，而 itemsList 维护每个会话的传输详情。
 *
 * @param value Proto 侧的会话解析响应
 * @return Kotlin 侧的会话解析结果
 */
fun resolvedUserSessionsFromProto(value: Client.ResolveUserSessionsResponse): ResolvedUserSessions = ResolvedUserSessions(
    user = userRefFromProto(value.user),
    presence = value.presenceList.map { ResolvedUserSessions.OnlineNodePresence(it.servingNodeId, it.sessionCount, it.transportHint) },
    sessions = value.itemsList.map { ResolvedUserSessions.ResolvedSession(sessionRefFromProto(it.session), it.transport, it.transientCapable) }
)

/**
 * 将 Proto 的 Client.ScanUserMetadataResponse 转换为 [UserMetadataScanResult]。
 *
 * @param value Proto 侧的扫描响应
 * @return Kotlin 侧的扫描结果
 */
fun userMetadataScanResultFromProto(value: Client.ScanUserMetadataResponse): UserMetadataScanResult = UserMetadataScanResult(
    items = value.itemsList.map(::userMetadataFromProto),
    count = value.count,
    nextAfter = value.nextAfter
)

/**
 * 将 Proto 的 Client.OperationsStatus 转换为 [OperationsStatus]。
 *
 * @param value Proto 侧的运维状态
 * @return Kotlin 侧的运维状态
 */
fun operationsStatusFromProto(value: Client.OperationsStatus): OperationsStatus = OperationsStatus(
    nodeId = value.nodeId,
    messageWindowSize = value.messageWindowSize,
    lastEventSequence = value.lastEventSequence,
    writeGateReady = value.writeGateReady,
    conflictTotal = value.conflictTotal,
    messageTrim = OperationsStatus.MessageTrimStatus(value.messageTrim.trimmedTotal, value.messageTrim.lastTrimmedAt),
    projection = OperationsStatus.ProjectionStatus(value.projection.pendingTotal, value.projection.lastFailedAt),
    peers = value.peersList.map { peer ->
        OperationsStatus.PeerStatus(
            nodeId = peer.nodeId,
            configuredUrl = peer.configuredUrl,
            source = peer.source,
            discoveredUrl = peer.discoveredUrl,
            discoveryState = peer.discoveryState,
            lastDiscoveredAt = peer.lastDiscoveredAt,
            lastConnectedAt = peer.lastConnectedAt,
            lastDiscoveryError = peer.lastDiscoveryError,
            connected = peer.connected,
            sessionDirection = peer.sessionDirection,
            origins = peer.originsList.map { origin ->
                OperationsStatus.PeerOriginStatus(
                    originNodeId = origin.originNodeId,
                    ackedEventId = origin.ackedEventId,
                    appliedEventId = origin.appliedEventId,
                    unconfirmedEvents = origin.unconfirmedEvents,
                    cursorUpdatedAt = origin.cursorUpdatedAt,
                    remoteLastEventId = requireUnsigned(origin.remoteLastEventId, "remote_last_event_id"),
                    pendingCatchup = origin.pendingCatchup
                )
            },
            pendingSnapshotPartitions = peer.pendingSnapshotPartitions,
            remoteSnapshotVersion = peer.remoteSnapshotVersion,
            remoteMessageWindowSize = peer.remoteMessageWindowSize,
            clockOffsetMs = peer.clockOffsetMs,
            lastClockSync = peer.lastClockSync,
            snapshotDigestsSentTotal = requireUnsigned(peer.snapshotDigestsSentTotal, "snapshot_digests_sent_total"),
            snapshotDigestsReceivedTotal = requireUnsigned(peer.snapshotDigestsReceivedTotal, "snapshot_digests_received_total"),
            snapshotChunksSentTotal = requireUnsigned(peer.snapshotChunksSentTotal, "snapshot_chunks_sent_total"),
            snapshotChunksReceivedTotal = requireUnsigned(peer.snapshotChunksReceivedTotal, "snapshot_chunks_received_total"),
            lastSnapshotDigestAt = peer.lastSnapshotDigestAt,
            lastSnapshotChunkAt = peer.lastSnapshotChunkAt
        )
    },
    eventLogTrim = OperationsStatus.EventLogTrimStatus(value.eventLogTrim.trimmedTotal, value.eventLogTrim.lastTrimmedAt)
)

/**
 * 将 Proto 的 Client.LoginResponse 转换为 [LoginInfo]。
 *
 * @param value Proto 侧的登录响应
 * @return Kotlin 侧的登录信息
 */
fun loginInfoFromProto(value: Client.LoginResponse): LoginInfo = LoginInfo(userFromProto(value.user), value.protocolVersion, sessionRefFromProto(value.sessionRef))

/**
 * 将 Proto 的 Client.DeleteUserResponse 转换为 [DeleteUserResult]。
 *
 * @param value Proto 侧的删除用户响应
 * @return Kotlin 侧的删除用户结果
 */
fun deleteUserResultFromProto(value: Client.DeleteUserResponse): DeleteUserResult = DeleteUserResult(value.status, userRefFromProto(value.user))

/**
 * 将可选的字符串值包装为 Proto 的 StringField。
 *
 * @param value 可选的字符串值
 * @return 如果值非 null，返回对应的 Proto StringField；否则返回 null
 */
fun optionalStringField(value: String?): Client.StringField? = value?.let { Client.StringField.newBuilder().setValue(it).build() }

/**
 * 将可选的密码输入包装为 Proto 的 StringField（使用密码的传输值）。
 *
 * @param value 可选的密码输入
 * @return 如果值非 null，返回对应的 Proto StringField；否则返回 null
 */
fun optionalPasswordField(value: PasswordInput?): Client.StringField? = value?.let { Client.StringField.newBuilder().setValue(it.wireValue()).build() }

/**
 * 将可选的字节数组包装为 Proto 的 BytesField。
 *
 * @param value 可选的字节数组
 * @return 如果值非 null，返回对应的 Proto BytesField；否则返回 null
 */
fun optionalBytesField(value: ByteArray?): Client.BytesField? = value?.let { Client.BytesField.newBuilder().setValue(ByteString.copyFrom(it)).build() }
