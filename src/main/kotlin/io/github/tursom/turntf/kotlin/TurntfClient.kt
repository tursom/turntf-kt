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

class TurntfClient(config: Config) {
    companion object {
        private const val CLOSED_MESSAGE = "turntf client is closed"
        private const val NOT_CONNECTED_MESSAGE = "turntf client is not connected"
        private const val DISCONNECTED_MESSAGE = "turntf websocket disconnected"
    }

    private val config = normalize(config)
    private val httpClient: OkHttpClient = this.config.httpClient
    val http = TurntfHttpClient(this.config.baseUrl, httpClient)

    private val runtimeDispatcher: CoroutineDispatcher = Dispatchers.IO
    private val orderedDispatcher: ExecutorCoroutineDispatcher = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "turntf-kt-events").apply { isDaemon = true }
    }.asCoroutineDispatcher()
    private val scope = CoroutineScope(SupervisorJob() + runtimeDispatcher)
    private val orderedScope = CoroutineScope(SupervisorJob() + orderedDispatcher)

    private val _events = MutableSharedFlow<ClientEvent>(extraBufferCapacity = 64)
    private val _loginState = MutableStateFlow<LoginInfo?>(null)
    private val _connectionState = MutableStateFlow(ConnectionState.DISCONNECTED)

    val events: SharedFlow<ClientEvent> = _events.asSharedFlow()
    val loginState: StateFlow<LoginInfo?> = _loginState.asStateFlow()
    val connectionState: StateFlow<ConnectionState> = _connectionState.asStateFlow()

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
    private var firstConnect = CompletableDeferred<Unit>()

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

    suspend fun login(nodeId: Long, userId: Long, password: String): String = http.login(nodeId, userId, password)

    suspend fun loginWithPassword(nodeId: Long, userId: Long, password: PasswordInput): String = http.loginWithPassword(nodeId, userId, password)

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

    suspend fun sendPacket(target: UserRef, body: ByteArray, deliveryMode: DeliveryMode, targetSession: SessionRef? = null): RelayAccepted =
        sendPacket(SendPacketInput(target, body, deliveryMode, targetSession))

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
                            .build()
                    )
                    .build()
            },
            mapper = { value -> value as? User ?: throw ProtocolError("missing user in create_user_response") }
        )
    }

    suspend fun createChannel(request: CreateUserRequest): User = createUser(request.copy(role = if (request.role.isEmpty()) "channel" else request.role))

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
                Client.ClientEnvelope.newBuilder().setUpdateUser(builder.build()).build()
            },
            mapper = { value -> value as? User ?: throw ProtocolError("missing user in update_user_response") }
        )
    }

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

    suspend fun subscribeChannel(subscriber: UserRef, channel: UserRef): Subscription =
        upsertAttachment(subscriber, channel, AttachmentType.CHANNEL_SUBSCRIPTION, "{}".encodeToByteArray()).let {
            Subscription(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId)
        }

    suspend fun unsubscribeChannel(subscriber: UserRef, channel: UserRef): Subscription =
        deleteAttachment(subscriber, channel, AttachmentType.CHANNEL_SUBSCRIPTION).let {
            Subscription(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId)
        }

    suspend fun listSubscriptions(subscriber: UserRef): List<Subscription> =
        listAttachments(subscriber, AttachmentType.CHANNEL_SUBSCRIPTION).map { Subscription(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId) }

    suspend fun blockUser(owner: UserRef, blocked: UserRef): BlacklistEntry =
        upsertAttachment(owner, blocked, AttachmentType.USER_BLACKLIST, "{}".encodeToByteArray()).let {
            BlacklistEntry(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId)
        }

    suspend fun unblockUser(owner: UserRef, blocked: UserRef): BlacklistEntry =
        deleteAttachment(owner, blocked, AttachmentType.USER_BLACKLIST).let {
            BlacklistEntry(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId)
        }

    suspend fun listBlockedUsers(owner: UserRef): List<BlacklistEntry> =
        listAttachments(owner, AttachmentType.USER_BLACKLIST).map { BlacklistEntry(it.owner, it.subject, it.attachedAt, it.deletedAt, it.originNodeId) }

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

    suspend fun operationsStatus(): OperationsStatus =
        rpc(
            build = { requestId ->
                Client.ClientEnvelope.newBuilder()
                    .setOperationsStatus(Client.OperationsStatusRequest.newBuilder().setRequestId(requestId).build())
                    .build()
            },
            mapper = { value -> value as? OperationsStatus ?: throw ProtocolError("missing status in operations_status_response") }
        )

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
            delayDuration = delayDuration.multipliedBy(2).coerceAtMost(config.maxReconnectDelay)
        }
    }

    private suspend fun connectAttempt(attempt: Attempt) {
        _connectionState.value = ConnectionState.CONNECTING
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
            requestId.compareAndSet(next, 0)
        }
    }

    private suspend fun persistMessage(message: Message) {
        config.cursorStore.saveMessage(message)
        config.cursorStore.saveCursor(message.cursor())
    }

    private inner class AttemptListener(private val attempt: Attempt) : WebSocketListener() {
        override fun onOpen(webSocket: WebSocket, response: Response) {
            val login = Client.LoginRequest.newBuilder()
                .setUser(userRefToProto(UserRef(config.credentials.nodeId, config.credentials.userId)))
                .setPassword(config.credentials.password.wireValue())
                .setTransientOnly(config.transientOnly)
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
                    if (error.unauthorized()) {
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
                Client.ServerEnvelope.BodyCase.PACKET_PUSHED -> _events.tryEmit(ClientEvent.PacketReceived(packetFromProto(env.packetPushed.packet)))
                Client.ServerEnvelope.BodyCase.SEND_MESSAGE_RESPONSE -> {
                    val requestId = requireUnsigned(env.sendMessageResponse.requestId, "request_id")
                    when (env.sendMessageResponse.bodyCase) {
                        Client.SendMessageResponse.BodyCase.MESSAGE -> {
                            val message = messageFromProto(env.sendMessageResponse.message)
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
        require(config.credentials.nodeId > 0) { "credentials.nodeId is required" }
        require(config.credentials.userId > 0) { "credentials.userId is required" }
        config.credentials.password.validate()
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
