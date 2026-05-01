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

/**
 * Suspend-friendly HTTP transport for turntf's management and query endpoints.
 *
 * The client keeps the public API byte-oriented even when the REST surface uses embedded JSON or
 * base64 fields, so callers can switch between HTTP and websocket transports without reshaping
 * their domain model.
 */
class TurntfHttpClient(
    baseUrl: String,
    private val client: OkHttpClient = OkHttpClient()
) {
    private val normalizedBaseUrl = baseUrl.removeSuffix("/")

    init {
        validateBaseUrl(baseUrl)
    }

    /** Performs HTTP login with a plaintext password hashed locally before transmission. */
    suspend fun login(nodeId: Long, userId: Long, password: String): String = loginWithPassword(nodeId, userId, plainPassword(password))

    /** Performs HTTP login by `login_name` with a plaintext password hashed locally before transmission. */
    suspend fun login(loginName: String, password: String): String = loginWithPassword(loginName, plainPassword(password))

    /** Performs HTTP login with an already-constructed password payload. */
    suspend fun loginWithPassword(nodeId: Long, userId: Long, password: PasswordInput): String {
        require(nodeId > 0) { "nodeId is required" }
        require(userId > 0) { "userId is required" }
        return loginWithPassword(password) {
            put("node_id", nodeId)
            put("user_id", userId)
        }
    }

    /**
     * Performs HTTP login by `login_name` with an already-constructed password payload.
     *
     * `login_name` authentication is independent from the user's display `username`.
     */
    suspend fun loginWithPassword(loginName: String, password: PasswordInput): String {
        require(loginName.isNotBlank()) { "loginName is required" }
        return loginWithPassword(password) {
            put("login_name", loginName)
        }
    }

    private suspend fun loginWithPassword(password: PasswordInput, selector: com.fasterxml.jackson.databind.node.ObjectNode.() -> Unit): String {
        val payload = mapper.createObjectNode().apply {
            selector()
            put("password", password.wireValue())
        }
        val response = doJson("POST", "/auth/login", "", payload, setOf(200))
        return text(response, "token").also { require(it.isNotEmpty()) { "empty token in login response" } }
    }

    /** Creates a user and normalizes the response into the shared public model. */
    suspend fun createUser(token: String, request: CreateUserRequest): User {
        require(request.username.isNotEmpty()) { "username is required" }
        require(request.role.isNotEmpty()) { "role is required" }
        val payload = mapper.createObjectNode().apply {
            put("username", request.username)
            put("role", request.role)
            request.password?.let { put("password", it.wireValue()) }
            if (request.loginName.isNotEmpty()) {
                put("login_name", request.loginName)
            }
            if (request.profileJson.isNotEmpty()) {
                // The REST API accepts embedded JSON here, while the websocket/proto API ships raw
                // bytes. Parsing once at the boundary keeps both transports exposing ByteArray.
                set<JsonNode>("profile", parseJsonBytes(request.profileJson))
            }
        }
        return userFromHttp(doJson("POST", "/users", token, payload, setOf(200, 201)))
    }

    /** Creates a channel user, defaulting the role to `channel` when omitted by the caller. */
    suspend fun createChannel(token: String, request: CreateUserRequest): User =
        createUser(token, request.copy(role = if (request.role.isEmpty()) "channel" else request.role))

    /** Reads one private metadata entry owned by [owner]. */
    suspend fun getUserMetadata(token: String, owner: UserRef, key: String): UserMetadata {
        validateUserRef(owner, "owner")
        validateUserMetadataKey(key, "key")
        return userMetadataFromHttp(doJson("GET", "/nodes/${owner.nodeId}/users/${owner.userId}/metadata/$key", token, null, setOf(200)))
    }

    /**
     * Creates or replaces one private metadata entry.
     *
     * The REST API transports [value] as base64 inside JSON, but callers keep working with the raw
     * bytes so this method matches the websocket/proto model.
     */
    suspend fun upsertUserMetadata(token: String, owner: UserRef, key: String, value: ByteArray, expiresAt: String? = null): UserMetadata {
        validateUserRef(owner, "owner")
        validateUserMetadataKey(key, "key")
        val payload = mapper.createObjectNode().apply {
            put("value", value)
            expiresAt?.let { put("expires_at", it) }
        }
        return userMetadataFromHttp(doJson("PUT", "/nodes/${owner.nodeId}/users/${owner.userId}/metadata/$key", token, payload, setOf(200, 201)))
    }

    /** Deletes one private metadata entry and returns the tombstoned record echoed by the server. */
    suspend fun deleteUserMetadata(token: String, owner: UserRef, key: String): UserMetadata {
        validateUserRef(owner, "owner")
        validateUserMetadataKey(key, "key")
        return userMetadataFromHttp(doJson("DELETE", "/nodes/${owner.nodeId}/users/${owner.userId}/metadata/$key", token, null, setOf(200)))
    }

    /** Scans private metadata in key order using the server's `prefix` / `after` / `limit` cursor semantics. */
    suspend fun scanUserMetadata(
        token: String,
        owner: UserRef,
        prefix: String = "",
        after: String = "",
        limit: Int = 0
    ): UserMetadataScanResult {
        validateUserRef(owner, "owner")
        require(limit >= 0) { "limit must be non-negative" }
        val query = buildList {
            if (prefix.isNotEmpty()) add("prefix=$prefix")
            if (after.isNotEmpty()) add("after=$after")
            if (limit > 0) add("limit=$limit")
        }
        val path = buildString {
            append("/nodes/${owner.nodeId}/users/${owner.userId}/metadata")
            if (query.isNotEmpty()) {
                append("?")
                append(query.joinToString("&"))
            }
        }
        return userMetadataScanResultFromHttp(doJson("GET", path, token, null, setOf(200)))
    }

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
        // config_json follows the same convention as profile_json: callers hand us bytes, but the
        // HTTP transport expects a JSON node embedded in the outer request document.
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
                        // Some mutation endpoints legitimately return an empty body. Exposing that
                        // as NullNode lets higher-level callers keep a uniform JSON parsing path.
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
