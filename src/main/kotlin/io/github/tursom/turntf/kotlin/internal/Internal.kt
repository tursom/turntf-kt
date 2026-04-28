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
import io.github.tursom.turntf.kotlin.UserRef
import notifier.client.v1.Client
import com.google.protobuf.ByteString
import java.net.URI

val mapper: ObjectMapper = ObjectMapper().registerKotlinModule()

fun validateBaseUrl(baseUrl: String) {
    require(baseUrl.isNotBlank()) { "baseUrl is required" }
}

fun validateUserRef(ref: UserRef, field: String) {
    require(ref.nodeId > 0) { "$field.nodeId is required" }
    require(ref.userId > 0) { "$field.userId is required" }
}

fun validateSessionRef(ref: SessionRef, field: String) {
    require(ref.servingNodeId > 0) { "$field.servingNodeId is required" }
    require(ref.sessionId.isNotEmpty()) { "$field.sessionId is required" }
}

fun validateDeliveryMode(mode: DeliveryMode) {
    require(mode == DeliveryMode.BEST_EFFORT || mode == DeliveryMode.ROUTE_RETRY) { "invalid deliveryMode $mode" }
}

fun requireUnsigned(value: Long, field: String): Long {
    if (value < 0) {
        throw ProtocolError("$field exceeds signed long range")
    }
    return value
}

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

fun parseJsonBytes(value: ByteArray): JsonNode = if (value.isEmpty()) mapper.createObjectNode() else mapper.readTree(value)

fun text(node: JsonNode, field: String): String = node.path(field).takeUnless { it.isMissingNode || it.isNull }?.asText("") ?: ""

fun longValue(node: JsonNode, field: String): Long = node.path(field).takeUnless { it.isMissingNode || it.isNull }?.asLong() ?: 0L

fun boolValue(node: JsonNode, field: String): Boolean = node.path(field).takeUnless { it.isMissingNode || it.isNull }?.asBoolean() ?: false

fun bytesValue(node: JsonNode, field: String): ByteArray = node.path(field).takeUnless { it.isMissingNode || it.isNull }?.binaryValue() ?: byteArrayOf()

private fun userRefNode(node: JsonNode): UserRef = UserRef(longValue(node, "node_id"), longValue(node, "user_id"))

fun userFromHttp(node: JsonNode): User {
    val profile = when {
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
        originNodeId = longValue(node, "origin_node_id")
    )
}

fun messageFromHttp(node: JsonNode): Message = Message(
    recipient = userRefNode(node.path("recipient")),
    nodeId = longValue(node, "node_id"),
    seq = longValue(node, "seq"),
    sender = userRefNode(node.path("sender")),
    body = bytesValue(node, "body"),
    createdAtHlc = text(node, "created_at_hlc").ifEmpty { text(node, "created_at") }
)

fun attachmentFromHttp(node: JsonNode): Attachment = Attachment(
    owner = userRefNode(node.path("owner")),
    subject = userRefNode(node.path("subject")),
    attachmentType = AttachmentType.valueOf(text(node, "attachment_type").uppercase()),
    configJson = bytesValue(node, "config_json"),
    attachedAt = text(node, "attached_at"),
    deletedAt = text(node, "deleted_at"),
    originNodeId = longValue(node, "origin_node_id")
)

fun blacklistEntryFromAttachment(attachment: Attachment): BlacklistEntry = BlacklistEntry(
    owner = attachment.owner,
    blocked = attachment.subject,
    blockedAt = attachment.attachedAt,
    deletedAt = attachment.deletedAt,
    originNodeId = attachment.originNodeId
)

fun clusterNodeFromHttp(node: JsonNode): ClusterNode = ClusterNode(
    nodeId = longValue(node, "node_id"),
    isLocal = boolValue(node, "is_local"),
    configuredUrl = text(node, "configured_url"),
    source = text(node, "source")
)

fun loggedInUserFromHttp(node: JsonNode): LoggedInUser = LoggedInUser(
    nodeId = longValue(node, "node_id"),
    userId = longValue(node, "user_id"),
    username = text(node, "username")
)

fun itemsNode(node: JsonNode, field: String): JsonNode = if (node.isArray) node else node.path(field)

fun userRefToProto(value: UserRef): Client.UserRef = Client.UserRef.newBuilder().setNodeId(value.nodeId).setUserId(value.userId).build()

fun userRefFromProto(value: Client.UserRef?): UserRef = if (value == null) UserRef(0, 0) else UserRef(value.nodeId, value.userId)

fun sessionRefToProto(value: SessionRef): Client.SessionRef = Client.SessionRef.newBuilder().setServingNodeId(value.servingNodeId).setSessionId(value.sessionId).build()

fun sessionRefFromProto(value: Client.SessionRef?): SessionRef = if (value == null) SessionRef(0, "") else SessionRef(value.servingNodeId, value.sessionId)

fun cursorToProto(value: MessageCursor): Client.MessageCursor = Client.MessageCursor.newBuilder().setNodeId(value.nodeId).setSeq(value.seq).build()

fun deliveryModeToProto(value: DeliveryMode): Client.ClientDeliveryMode = when (value) {
    DeliveryMode.BEST_EFFORT -> Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_BEST_EFFORT
    DeliveryMode.ROUTE_RETRY -> Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_ROUTE_RETRY
    DeliveryMode.UNSPECIFIED -> Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_UNSPECIFIED
}

fun deliveryModeFromProto(value: Client.ClientDeliveryMode): DeliveryMode = when (value) {
    Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_BEST_EFFORT -> DeliveryMode.BEST_EFFORT
    Client.ClientDeliveryMode.CLIENT_DELIVERY_MODE_ROUTE_RETRY -> DeliveryMode.ROUTE_RETRY
    else -> DeliveryMode.UNSPECIFIED
}

fun attachmentTypeToProto(value: AttachmentType?): Client.AttachmentType = when (value) {
    AttachmentType.CHANNEL_MANAGER -> Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_MANAGER
    AttachmentType.CHANNEL_WRITER -> Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_WRITER
    AttachmentType.CHANNEL_SUBSCRIPTION -> Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION
    AttachmentType.USER_BLACKLIST -> Client.AttachmentType.ATTACHMENT_TYPE_USER_BLACKLIST
    null -> Client.AttachmentType.ATTACHMENT_TYPE_UNSPECIFIED
}

fun attachmentTypeFromProto(value: Client.AttachmentType): AttachmentType = when (value) {
    Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_MANAGER -> AttachmentType.CHANNEL_MANAGER
    Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_WRITER -> AttachmentType.CHANNEL_WRITER
    Client.AttachmentType.ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION -> AttachmentType.CHANNEL_SUBSCRIPTION
    Client.AttachmentType.ATTACHMENT_TYPE_USER_BLACKLIST -> AttachmentType.USER_BLACKLIST
    else -> AttachmentType.CHANNEL_MANAGER
}

fun userFromProto(value: Client.User?): User = if (value == null) User(0, 0, "", "") else User(
    nodeId = value.nodeId,
    userId = value.userId,
    username = value.username,
    role = value.role,
    profileJson = value.profileJson.toByteArray(),
    systemReserved = value.systemReserved,
    createdAt = value.createdAt,
    updatedAt = value.updatedAt,
    originNodeId = value.originNodeId
)

fun messageFromProto(value: Client.Message?): Message = if (value == null) Message(UserRef(0, 0), 0, 0, UserRef(0, 0), byteArrayOf(), "") else Message(
    recipient = userRefFromProto(value.recipient),
    nodeId = value.nodeId,
    seq = value.seq,
    sender = userRefFromProto(value.sender),
    body = value.body.toByteArray(),
    createdAtHlc = value.createdAtHlc
)

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

fun relayAcceptedFromProto(value: Client.TransientAccepted): RelayAccepted = RelayAccepted(
    packetId = requireUnsigned(value.packetId, "packet_id"),
    sourceNodeId = value.sourceNodeId,
    targetNodeId = value.targetNodeId,
    recipient = userRefFromProto(value.recipient),
    deliveryMode = deliveryModeFromProto(value.deliveryMode),
    targetSession = sessionRefFromProto(value.targetSession)
)

fun attachmentFromProto(value: Client.Attachment): Attachment = Attachment(
    owner = userRefFromProto(value.owner),
    subject = userRefFromProto(value.subject),
    attachmentType = attachmentTypeFromProto(value.attachmentType),
    configJson = value.configJson.toByteArray(),
    attachedAt = value.attachedAt,
    deletedAt = value.deletedAt,
    originNodeId = value.originNodeId
)

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

fun clusterNodeFromProto(value: Client.ClusterNode): ClusterNode = ClusterNode(value.nodeId, value.isLocal, value.configuredUrl, value.source)

fun loggedInUserFromProto(value: Client.LoggedInUser): LoggedInUser = LoggedInUser(value.nodeId, value.userId, value.username)

fun resolvedUserSessionsFromProto(value: Client.ResolveUserSessionsResponse): ResolvedUserSessions = ResolvedUserSessions(
    user = userRefFromProto(value.user),
    presence = value.presenceList.map { ResolvedUserSessions.OnlineNodePresence(it.servingNodeId, it.sessionCount, it.transportHint) },
    sessions = value.itemsList.map { ResolvedUserSessions.ResolvedSession(sessionRefFromProto(it.session), it.transport, it.transientCapable) }
)

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

fun loginInfoFromProto(value: Client.LoginResponse): LoginInfo = LoginInfo(userFromProto(value.user), value.protocolVersion, sessionRefFromProto(value.sessionRef))

fun deleteUserResultFromProto(value: Client.DeleteUserResponse): DeleteUserResult = DeleteUserResult(value.status, userRefFromProto(value.user))

fun optionalStringField(value: String?): Client.StringField? = value?.let { Client.StringField.newBuilder().setValue(it).build() }

fun optionalPasswordField(value: PasswordInput?): Client.StringField? = value?.let { Client.StringField.newBuilder().setValue(it.wireValue()).build() }

fun optionalBytesField(value: ByteArray?): Client.BytesField? = value?.let { Client.BytesField.newBuilder().setValue(ByteString.copyFrom(it)).build() }
