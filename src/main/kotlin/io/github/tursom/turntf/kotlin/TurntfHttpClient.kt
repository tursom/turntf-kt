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
 * 支持挂起函数的 turntf HTTP 传输客户端，用于管理类和查询类端点。
 *
 * 客户端保持公开 API 的面向字节接口，即使 REST 接口使用嵌入式 JSON 或 base64 字段。
 * 这使得调用者可以在 HTTP 和 WebSocket 传输之间切换，而无需调整其领域模型。
 *
 * @param baseUrl 服务器基础 URL（例如 "https://example.com"）
 * @param client OkHttp 客户端实例
 */
class TurntfHttpClient(
    baseUrl: String,
    private val client: OkHttpClient = OkHttpClient()
) {
    private val normalizedBaseUrl = baseUrl.removeSuffix("/")

    init {
        validateBaseUrl(baseUrl)
    }

    /**
     * 通过 HTTP 进行登录，使用明文密码并自动在本地进行 bcrypt 哈希处理。
     *
     * 使用传统身份验证方式：通过节点 ID 和用户 ID 标识用户。
     *
     * @param nodeId 节点 ID
     * @param userId 用户 ID
     * @param password 明文密码（将自动哈希）
     * @return 登录令牌（JWT 字符串）
     * @throws IllegalArgumentException 如果节点 ID 或用户 ID 无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun login(nodeId: Long, userId: Long, password: String): String = loginWithPassword(nodeId, userId, plainPassword(password))

    /**
     * 通过 HTTP 进行登录，使用登录名和明文密码。
     *
     * 密码会自动在本地进行 bcrypt 哈希处理。
     *
     * @param loginName 登录名
     * @param password 明文密码（将自动哈希）
     * @return 登录令牌（JWT 字符串）
     * @throws IllegalArgumentException 如果登录名为空
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun login(loginName: String, password: String): String = loginWithPassword(loginName, plainPassword(password))

    /**
     * 通过 HTTP 进行登录，使用已经构造好的密码载荷。
     *
     * 使用传统身份验证方式：通过节点 ID 和用户 ID 标识用户。
     *
     * @param nodeId 节点 ID
     * @param userId 用户 ID
     * @param password 已包装的密码输入
     * @return 登录令牌（JWT 字符串）
     * @throws IllegalArgumentException 如果节点 ID 或用户 ID 无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun loginWithPassword(nodeId: Long, userId: Long, password: PasswordInput): String {
        require(nodeId > 0) { "nodeId is required" }
        require(userId > 0) { "userId is required" }
        return loginWithPassword(password) {
            put("node_id", nodeId)
            put("user_id", userId)
        }
    }

    /**
     * 通过 HTTP 进行登录，使用登录名和已经构造好的密码载荷。
     *
     * `login_name` 身份验证独立于用户的显示 `username`。
     *
     * @param loginName 登录名
     * @param password 已包装的密码输入
     * @return 登录令牌（JWT 字符串）
     * @throws IllegalArgumentException 如果登录名为空
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
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

    /**
     * 创建用户并将响应标准化为共享的公开模型。
     *
     * REST API 接受嵌入式 JSON 格式的 profile，而 WebSocket/proto API 使用原始字节。
     * 在边界处解析一次使两种传输方式都暴露 [ByteArray] 接口。
     *
     * @param token 身份验证令牌
     * @param request 创建用户请求
     * @return 创建成功的用户信息
     * @throws IllegalArgumentException 如果用户名或角色为空
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
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
                // REST API 在此接受嵌入式 JSON，而 WebSocket/proto API 使用原始字节。
                // 在边界处解析一次使两种传输方式都暴露 ByteArray。
                set<JsonNode>("profile", parseJsonBytes(request.profileJson))
            }
        }
        return userFromHttp(doJson("POST", "/users", token, payload, setOf(200, 201)))
    }

    /**
     * 创建频道用户，当调用者未指定角色时默认角色为 "channel"。
     *
     * @param token 身份验证令牌
     * @param request 创建用户请求（角色可以留空）
     * @return 创建成功的频道用户信息
     * @see createUser
     */
    suspend fun createChannel(token: String, request: CreateUserRequest): User =
        createUser(token, request.copy(role = if (request.role.isEmpty()) "channel" else request.role))

    /**
     * 列出当前调用者可通讯的活跃用户，并支持名称与 uid 过滤。
     *
     * 服务端会先根据当前登录用户的权限、频道关系和黑名单关系裁剪“可通讯用户集合”，
     * 再在该集合内应用 [filter]。这意味着普通用户不能通过此接口搜索到自己不可见的用户。
     *
     * SDK 公开的 `uid` 过滤统一使用 [UserRef]：
     * - HTTP 传输时会自动编码为 `node_id:user_id`
     * - `UserRef(0, 0)` 或 `null` 表示“不按 uid 过滤”
     *
     * 服务端可能会对普通用户看到的其他联系人隐藏 `login_name`，因此返回的 [User.loginName]
     * 允许为空字符串；管理员或查看自己时仍可能拿到完整值。
     *
     * @param token 身份验证令牌；在未开启鉴权的服务端上可以传空字符串
     * @param filter 过滤条件，支持 `name` 大小写不敏感子串匹配与 `uid` 精确过滤
     * @return 当前用户可通讯的活跃用户列表
     * @throws IllegalArgumentException 如果 `uid` 只填写了一半或字段值不是正整数
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun listUsers(token: String, filter: UserListFilter = UserListFilter()): List<User> {
        val normalized = normalizeUserListFilter(filter)
        val path = buildString {
            append("/users")
            val query = buildList {
                if (normalized.name.isNotEmpty()) {
                    add("name=${encodeQueryComponent(normalized.name)}")
                }
                normalized.uid?.let { add("uid=${encodeQueryComponent(userRefToHttpUid(it))}") }
            }
            if (query.isNotEmpty()) {
                append("?")
                append(query.joinToString("&"))
            }
        }
        val response = doJson("GET", path, token, null, setOf(200))
        return itemsNode(response, "items").map(::userFromHttp)
    }

    /**
     * 读取指定用户的一条私有元数据。
     *
     * @param token 身份验证令牌
     * @param owner 元数据所属用户
     * @param key 元数据键名
     * @return 元数据条目
     * @throws IllegalArgumentException 如果参数无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun getUserMetadata(token: String, owner: UserRef, key: String): UserMetadata {
        validateUserRef(owner, "owner")
        validateUserMetadataKey(key, "key")
        return userMetadataFromHttp(doJson("GET", "/nodes/${owner.nodeId}/users/${owner.userId}/metadata/$key", token, null, setOf(200)))
    }

    /**
     * 创建或替换一条私有元数据。
     *
     * REST API 将 [value] 作为 base64 编码在 JSON 中传输，但调用者始终使用原始字节，
     * 使此方法与 WebSocket/proto 模型保持一致。
     *
     * @param token 身份验证令牌
     * @param owner 元数据所属用户
     * @param key 元数据键名
     * @param value 元数据值（原始字节，将在 HTTP 层自动进行 base64 编码）
     * @param expiresAt 可选的过期时间（RFC3339 格式）
     * @return 创建或更新后的元数据条目
     * @throws IllegalArgumentException 如果参数无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
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

    /**
     * 删除一条私有元数据并返回服务端回显的已删除记录。
     *
     * @param token 身份验证令牌
     * @param owner 元数据所属用户
     * @param key 元数据键名
     * @return 已删除的元数据条目（tombstone 记录）
     * @throws IllegalArgumentException 如果参数无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun deleteUserMetadata(token: String, owner: UserRef, key: String): UserMetadata {
        validateUserRef(owner, "owner")
        validateUserMetadataKey(key, "key")
        return userMetadataFromHttp(doJson("DELETE", "/nodes/${owner.nodeId}/users/${owner.userId}/metadata/$key", token, null, setOf(200)))
    }

    /**
     * 按键名顺序扫描私有元数据。
     *
     * 支持按前缀过滤、游标分页和数量限制。
     * 使用服务端的 `prefix` / `after` / `limit` 游标语义。
     *
     * @param token 身份验证令牌
     * @param owner 元数据所属用户
     * @param prefix 键名前缀过滤，仅返回匹配该前缀的条目
     * @param after 游标值，从指定键之后开始扫描
     * @param limit 返回结果的最大数量（0 表示服务端默认限制）
     * @return 扫描结果，包含条目列表和下一页游标
     * @throws IllegalArgumentException 如果参数无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
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

    /**
     * 创建频道订阅。
     *
     * 订阅后，频道发布的消息会推送给订阅者。
     *
     * @param token 身份验证令牌
     * @param user 订阅者
     * @param channel 被订阅的频道
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun createSubscription(token: String, user: UserRef, channel: UserRef) {
        upsertAttachment(token, user, channel, AttachmentType.CHANNEL_SUBSCRIPTION, "{}".encodeToByteArray())
    }

    /**
     * 列出指定目标用户的持久化消息，可选地按发送者（peer）过滤。
     *
     * target 支持 nodeId=0, userId=0 作为"当前用户"的 sentinel 值。
     *
     * @param token 身份验证令牌
     * @param target 目标用户
     * @param limit 返回消息的最大数量（0 表示服务端默认限制）
     * @param peerNodeId 可选的发送者节点 ID，与 peerUserId 同时提供时按 session 过滤
     * @param peerUserId 可选的发送者用户 ID，与 peerNodeId 同时提供时按 session 过滤
     * @return 消息列表
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun listMessages(token: String, target: UserRef, limit: Int = 0, peerNodeId: Long? = null, peerUserId: Long? = null): List<Message> {
        val path = buildString {
            append("/nodes/${target.nodeId}/users/${target.userId}/messages")
            val params = buildList {
                if (limit > 0) add("limit=$limit")
                if (peerNodeId != null && peerUserId != null) {
                    add("peer_node_id=$peerNodeId")
                    add("peer_user_id=$peerUserId")
                }
            }
            if (params.isNotEmpty()) {
                append("?")
                append(params.joinToString("&"))
            }
        }
        val response = doJson("GET", path, token, null, setOf(200))
        return itemsNode(response, "items").map { messageFromHttp(it) }
    }

    /**
     * 向目标用户发送持久化消息。
     *
     * 消息会被服务端持久化存储，目标用户上线后会接收到。
     *
     * @param token 身份验证令牌
     * @param target 目标用户
     * @param body 消息体（原始字节）
     * @return 服务端回显的持久化消息
     * @throws IllegalArgumentException 如果参数无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun postMessage(token: String, target: UserRef, body: ByteArray): Message {
        validateUserRef(target, "target")
        require(body.isNotEmpty()) { "body is required" }
        val payload = mapper.createObjectNode().apply { put("body", body) }
        return messageFromHttp(doJson("POST", "/nodes/${target.nodeId}/users/${target.userId}/messages", token, payload, setOf(200, 201)))
    }

    /**
     * 向目标用户发送瞬时数据包。
     *
     * 数据包不会被持久化，仅在目标用户当前在线时才能送达。
     *
     * @param token 身份验证令牌
     * @param targetNodeId 目标节点 ID（必须与 relayTarget.nodeId 一致）
     * @param relayTarget 中继目标用户
     * @param body 数据包内容（原始字节）
     * @param mode 投递模式
     * @throws IllegalArgumentException 如果参数无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
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

    /**
     * 列出集群中的所有节点。
     *
     * @param token 身份验证令牌
     * @return 集群节点列表
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun listClusterNodes(token: String): List<ClusterNode> {
        val response = doJson("GET", "/cluster/nodes", token, null, setOf(200))
        val items = if (response.isArray) response else if (response.has("nodes")) response.path("nodes") else response.path("items")
        return items.map { clusterNodeFromHttp(it) }
    }

    /**
     * 列出指定节点上当前已登录的用户。
     *
     * @param token 身份验证令牌
     * @param nodeId 目标节点 ID
     * @return 已登录用户列表
     * @throws IllegalArgumentException 如果节点 ID 无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun listNodeLoggedInUsers(token: String, nodeId: Long): List<LoggedInUser> {
        require(nodeId > 0) { "nodeId is required" }
        val response = doJson("GET", "/cluster/nodes/$nodeId/logged-in-users", token, null, setOf(200))
        return itemsNode(response, "items").map { loggedInUserFromHttp(it) }
    }

    /**
     * 拉黑一个用户。
     *
     * @param token 身份验证令牌
     * @param owner 执行拉黑操作的用户
     * @param blocked 被拉黑的用户
     * @return 黑名单条目信息
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun blockUser(token: String, owner: UserRef, blocked: UserRef): BlacklistEntry =
        blacklistEntryFromAttachment(upsertAttachment(token, owner, blocked, AttachmentType.USER_BLACKLIST, "{}".encodeToByteArray()))

    /**
     * 解除对用户的拉黑。
     *
     * @param token 身份验证令牌
     * @param owner 执行解除拉黑操作的用户
     * @param blocked 需要解除拉黑的用户
     * @return 解除后的黑名单条目信息
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun unblockUser(token: String, owner: UserRef, blocked: UserRef): BlacklistEntry =
        blacklistEntryFromAttachment(deleteAttachment(token, owner, blocked, AttachmentType.USER_BLACKLIST))

    /**
     * 列出指定用户拉黑的所有用户。
     *
     * @param token 身份验证令牌
     * @param owner 用户引用
     * @return 黑名单条目列表
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun listBlockedUsers(token: String, owner: UserRef): List<BlacklistEntry> =
        listAttachments(token, owner, AttachmentType.USER_BLACKLIST).map(::blacklistEntryFromAttachment)

    /**
     * 创建或替换一个用户关系附件。
     *
     * @param token 身份验证令牌
     * @param owner 附件所有者
     * @param subject 附件关联的目标用户
     * @param attachmentType 附件类型
     * @param configJson 附件配置的 JSON 数据（原始字节）
     * @return 创建或更新后的附件
     * @throws IllegalArgumentException 如果参数无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun upsertAttachment(
        token: String,
        owner: UserRef,
        subject: UserRef,
        attachmentType: AttachmentType,
        configJson: ByteArray
    ): Attachment {
        validateUserRef(owner, "owner")
        validateUserRef(subject, "subject")
        // config_json 遵循与 profile_json 相同的约定：调用者提供字节，
        // 但 HTTP 传输层期望在外部请求文档中嵌入一个 JSON 节点。
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

    /**
     * 删除一个用户关系附件。
     *
     * @param token 身份验证令牌
     * @param owner 附件所有者
     * @param subject 附件关联的目标用户
     * @param attachmentType 附件类型
     * @return 已删除的附件（tombstone 记录）
     * @throws IllegalArgumentException 如果参数无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun deleteAttachment(token: String, owner: UserRef, subject: UserRef, attachmentType: AttachmentType): Attachment {
        validateUserRef(owner, "owner")
        validateUserRef(subject, "subject")
        return attachmentFromHttp(doJson("DELETE", "/nodes/${owner.nodeId}/users/${owner.userId}/attachments/${attachmentType.wireValue}/${subject.nodeId}/${subject.userId}", token, null, setOf(200)))
    }

    /**
     * 列出指定用户的所有附件，可选地按类型过滤。
     *
     * @param token 身份验证令牌
     * @param owner 附件所有者
     * @param attachmentType 可选的附件类型过滤器，null 表示列出所有类型
     * @return 附件列表
     * @throws IllegalArgumentException 如果所有者引用无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun listAttachments(token: String, owner: UserRef, attachmentType: AttachmentType? = null): List<Attachment> {
        validateUserRef(owner, "owner")
        val path = buildString {
            append("/nodes/${owner.nodeId}/users/${owner.userId}/attachments")
            attachmentType?.let { append("?attachment_type=${it.wireValue}") }
        }
        val response = doJson("GET", path, token, null, setOf(200))
        return itemsNode(response, "items").map { attachmentFromHttp(it) }
    }

    /**
     * 获取指定用户的详细信息。
     *
     * @param token 身份验证令牌
     * @param target 目标用户引用
     * @return 用户详细信息
     * @throws IllegalArgumentException 如果目标用户引用无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun getUser(token: String, target: UserRef): User {
        validateUserRef(target, "target")
        return userFromHttp(doJson("GET", "/nodes/${target.nodeId}/users/${target.userId}", token, null, setOf(200)))
    }

    /**
     * 更新用户信息。仅非 null 的字段会被更新。
     *
     * login_name 为空字符串时解除登录名绑定。频道（role="channel"）不支持设置 login_name。
     *
     * @param token 身份验证令牌
     * @param target 目标用户引用
     * @param request 更新请求，null 字段表示不修改
     * @return 更新后的用户信息
     * @throws IllegalArgumentException 如果参数无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun updateUser(token: String, target: UserRef, request: UpdateUserRequest): User {
        validateUserRef(target, "target")
        if (request.role == "channel" && !request.loginName.isNullOrBlank()) {
            throw IllegalArgumentException("channel users cannot have a login_name")
        }
        val payload = mapper.createObjectNode().apply {
            request.username?.let { put("username", it) }
            request.loginName?.let { put("login_name", it.ifEmpty { "" }) }
            request.password?.let { put("password", it.wireValue()) }
            if (request.profileJson != null && request.profileJson!!.isNotEmpty()) {
                set<JsonNode>("profile", parseJsonBytes(request.profileJson!!))
            }
            request.role?.let { put("role", it) }
        }
        return userFromHttp(doJson("PATCH", "/nodes/${target.nodeId}/users/${target.userId}", token, payload, setOf(200)))
    }

    /**
     * 删除指定用户（软删除）。
     *
     * @param token 身份验证令牌
     * @param target 目标用户引用
     * @return 删除结果，包含操作状态和被删除用户引用
     * @throws IllegalArgumentException 如果目标用户引用无效
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun deleteUser(token: String, target: UserRef): DeleteUserResult {
        validateUserRef(target, "target")
        return deleteUserResultFromHttp(doJson("DELETE", "/nodes/${target.nodeId}/users/${target.userId}", token, null, setOf(200)))
    }

    /**
     * 查询事件日志，支持分页游标。
     *
     * @param token 身份验证令牌
     * @param after 起始事件序列号（不含），0 表示从头开始
     * @param limit 返回事件的最大数量，0 表示使用服务端默认值
     * @return 事件列表
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun listEvents(token: String, after: Long = 0, limit: Int = 0): List<Event> {
        val query = buildList {
            if (after > 0) add("after=$after")
            if (limit > 0) add("limit=$limit")
        }
        val path = buildString {
            append("/events")
            if (query.isNotEmpty()) {
                append("?")
                append(query.joinToString("&"))
            }
        }
        val response = doJson("GET", path, token, null, setOf(200))
        return itemsNode(response, "items").map(::eventFromHttp)
    }

    /**
     * 查询节点运行状态，包含消息窗口、写闸门、投影等指标。
     *
     * @param token 身份验证令牌
     * @return 运行状态信息
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun operationsStatus(token: String): OperationsStatus =
        operationsStatusFromHttp(doJson("GET", "/ops/status", token, null, setOf(200)))

    /**
     * 获取 Prometheus 格式的监控指标文本。
     *
     * @param token 身份验证令牌
     * @return 监控指标文本（Prometheus 格式）
     * @throws ConnectionError 如果网络请求失败
     * @throws ProtocolError 如果服务器返回意外状态码
     */
    suspend fun metrics(token: String): String = withContext(Dispatchers.IO) {
        val builder = Request.Builder().url(normalizedBaseUrl + "/metrics")
        builder.method("GET", null)
        if (token.isNotEmpty()) {
            builder.header("Authorization", "Bearer $token")
        }
        try {
            client.newCall(builder.build()).execute().use { response ->
                if (response.code != 200) {
                    val data = response.body?.string()?.trim().orEmpty()
                    throw ProtocolError("unexpected HTTP status ${response.code}: $data")
                }
                response.body?.string().orEmpty()
            }
        } catch (e: IOException) {
            throw ConnectionError("GET /metrics", e)
        }
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
                        // 某些修改端点在合法情况下返回空 body。将其暴露为
                        // NullNode 使上层调用者可以保持统一的 JSON 解析路径。
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
