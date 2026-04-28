package io.github.tursom.turntf.kotlin

import com.fasterxml.jackson.databind.JsonNode
import io.github.tursom.turntf.kotlin.internal.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException

class TurntfHttpClient(
    baseUrl: String,
    private val client: OkHttpClient = OkHttpClient()
) {
    private val normalizedBaseUrl = baseUrl.removeSuffix("/")

    init {
        validateBaseUrl(baseUrl)
    }

    suspend fun login(nodeId: Long, userId: Long, password: String): String = loginWithPassword(nodeId, userId, plainPassword(password))

    suspend fun loginWithPassword(nodeId: Long, userId: Long, password: PasswordInput): String {
        require(nodeId > 0) { "nodeId is required" }
        require(userId > 0) { "userId is required" }
        val payload = mapper.createObjectNode().apply {
            put("node_id", nodeId)
            put("user_id", userId)
            put("password", password.wireValue())
        }
        val response = doJson("POST", "/auth/login", "", payload, setOf(200))
        return text(response, "token").also { require(it.isNotEmpty()) { "empty token in login response" } }
    }

    suspend fun createUser(token: String, request: CreateUserRequest): User {
        require(request.username.isNotEmpty()) { "username is required" }
        require(request.role.isNotEmpty()) { "role is required" }
        val payload = mapper.createObjectNode().apply {
            put("username", request.username)
            put("role", request.role)
            request.password?.let { put("password", it.wireValue()) }
            if (request.profileJson.isNotEmpty()) {
                set<JsonNode>("profile", parseJsonBytes(request.profileJson))
            }
        }
        return userFromHttp(doJson("POST", "/users", token, payload, setOf(200, 201)))
    }

    suspend fun createChannel(token: String, request: CreateUserRequest): User =
        createUser(token, request.copy(role = if (request.role.isEmpty()) "channel" else request.role))

    suspend fun createSubscription(token: String, user: UserRef, channel: UserRef) {
        upsertAttachment(token, user, channel, AttachmentType.CHANNEL_SUBSCRIPTION, "{}".encodeToByteArray())
    }

    suspend fun listMessages(token: String, target: UserRef, limit: Int = 0): List<Message> {
        validateUserRef(target, "target")
        val path = if (limit > 0) "/nodes/${target.nodeId}/users/${target.userId}/messages?limit=$limit" else "/nodes/${target.nodeId}/users/${target.userId}/messages"
        val response = doJson("GET", path, token, null, setOf(200))
        return itemsNode(response, "items").map { messageFromHttp(it) }
    }

    suspend fun postMessage(token: String, target: UserRef, body: ByteArray): Message {
        validateUserRef(target, "target")
        require(body.isNotEmpty()) { "body is required" }
        val payload = mapper.createObjectNode().apply { put("body", body) }
        return messageFromHttp(doJson("POST", "/nodes/${target.nodeId}/users/${target.userId}/messages", token, payload, setOf(200, 201)))
    }

    suspend fun postPacket(token: String, targetNodeId: Long, relayTarget: UserRef, body: ByteArray, mode: DeliveryMode) {
        require(targetNodeId > 0) { "targetNodeId is required" }
        validateUserRef(relayTarget, "relayTarget")
        validateDeliveryMode(mode)
        require(body.isNotEmpty()) { "body is required" }
        require(targetNodeId == relayTarget.nodeId) { "target node ID $targetNodeId does not match target user nodeId ${relayTarget.nodeId}" }
        val payload = mapper.createObjectNode().apply {
            put("body", body)
            put("delivery_kind", "transient")
            put("delivery_mode", mode.wireValue)
        }
        doJson("POST", "/nodes/${relayTarget.nodeId}/users/${relayTarget.userId}/messages", token, payload, setOf(202))
    }

    suspend fun listClusterNodes(token: String): List<ClusterNode> {
        val response = doJson("GET", "/cluster/nodes", token, null, setOf(200))
        val items = if (response.isArray) response else if (response.has("nodes")) response.path("nodes") else response.path("items")
        return items.map { clusterNodeFromHttp(it) }
    }

    suspend fun listNodeLoggedInUsers(token: String, nodeId: Long): List<LoggedInUser> {
        require(nodeId > 0) { "nodeId is required" }
        val response = doJson("GET", "/cluster/nodes/$nodeId/logged-in-users", token, null, setOf(200))
        return itemsNode(response, "items").map { loggedInUserFromHttp(it) }
    }

    suspend fun blockUser(token: String, owner: UserRef, blocked: UserRef): BlacklistEntry =
        blacklistEntryFromAttachment(upsertAttachment(token, owner, blocked, AttachmentType.USER_BLACKLIST, "{}".encodeToByteArray()))

    suspend fun unblockUser(token: String, owner: UserRef, blocked: UserRef): BlacklistEntry =
        blacklistEntryFromAttachment(deleteAttachment(token, owner, blocked, AttachmentType.USER_BLACKLIST))

    suspend fun listBlockedUsers(token: String, owner: UserRef): List<BlacklistEntry> =
        listAttachments(token, owner, AttachmentType.USER_BLACKLIST).map(::blacklistEntryFromAttachment)

    suspend fun upsertAttachment(
        token: String,
        owner: UserRef,
        subject: UserRef,
        attachmentType: AttachmentType,
        configJson: ByteArray
    ): Attachment {
        validateUserRef(owner, "owner")
        validateUserRef(subject, "subject")
        val payload = mapper.createObjectNode().apply { set<JsonNode>("config_json", parseJsonBytes(configJson)) }
        return attachmentFromHttp(
            doJson(
                "PUT",
                "/nodes/${owner.nodeId}/users/${owner.userId}/attachments/${attachmentType.wireValue}/${subject.nodeId}/${subject.userId}",
                token,
                payload,
                setOf(200, 201)
            )
        )
    }

    suspend fun deleteAttachment(token: String, owner: UserRef, subject: UserRef, attachmentType: AttachmentType): Attachment {
        validateUserRef(owner, "owner")
        validateUserRef(subject, "subject")
        return attachmentFromHttp(doJson("DELETE", "/nodes/${owner.nodeId}/users/${owner.userId}/attachments/${attachmentType.wireValue}/${subject.nodeId}/${subject.userId}", token, null, setOf(200)))
    }

    suspend fun listAttachments(token: String, owner: UserRef, attachmentType: AttachmentType? = null): List<Attachment> {
        validateUserRef(owner, "owner")
        val path = buildString {
            append("/nodes/${owner.nodeId}/users/${owner.userId}/attachments")
            attachmentType?.let { append("?attachment_type=${it.wireValue}") }
        }
        val response = doJson("GET", path, token, null, setOf(200))
        return itemsNode(response, "items").map { attachmentFromHttp(it) }
    }

    private suspend fun doJson(method: String, path: String, token: String, requestBody: JsonNode?, wantStatuses: Set<Int>): JsonNode =
        withContext(Dispatchers.IO) {
            val builder = Request.Builder().url(normalizedBaseUrl + path)
            val body = requestBody?.toString()?.toRequestBody("application/json".toMediaType())
            builder.method(method, body)
            if (requestBody != null) {
                builder.header("Content-Type", "application/json")
            }
            if (token.isNotEmpty()) {
                builder.header("Authorization", "Bearer $token")
            }
            try {
                client.newCall(builder.build()).execute().use { response ->
                    if (response.code !in wantStatuses) {
                        val data = response.body?.string()?.trim().orEmpty()
                        throw ProtocolError("unexpected HTTP status ${response.code}: $data")
                    }
                    val text = response.body?.string().orEmpty()
                    if (text.isBlank()) {
                        mapper.nullNode()
                    } else {
                        mapper.readTree(text)
                    }
                }
            } catch (e: IOException) {
                throw ConnectionError("$method $path", e)
            }
        }
}
