package io.github.tursom.turntf.kotlin

import com.google.protobuf.ByteString
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.test.runTest
import notifier.client.v1.Client
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.ByteString.Companion.toByteString
import org.mindrot.jbcrypt.BCrypt
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TurntfClientTest {
    @Test
    fun loginAckSendAndPing() = runTest {
        MockWebServer().use { server ->
            val acked = CompletableDeferred<Unit>()
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onMessage(webSocket: WebSocket, bytes: okio.ByteString) {
                    val env = Client.ClientEnvelope.parseFrom(bytes.toByteArray())
                    when (env.bodyCase) {
                        Client.ClientEnvelope.BodyCase.LOGIN -> {
                            assertTrue(BCrypt.checkpw("alice-password", env.login.password))
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setLoginResponse(
                                        Client.LoginResponse.newBuilder()
                                            .setUser(
                                                Client.User.newBuilder()
                                                    .setNodeId(4096)
                                                    .setUserId(1025)
                                                    .setUsername("alice")
                                                    .setRole("user")
                                                    .setLoginName("alice.login")
                                                    .build()
                                            )
                                            .setProtocolVersion("client-v1alpha5")
                                            .setSessionRef(Client.SessionRef.newBuilder().setServingNodeId(4096).setSessionId("session-a").build())
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setMessagePushed(
                                        Client.MessagePushed.newBuilder()
                                            .setMessage(
                                                Client.Message.newBuilder()
                                                    .setRecipient(Client.UserRef.newBuilder().setNodeId(4096).setUserId(1025).build())
                                                    .setNodeId(4096)
                                                    .setSeq(7)
                                                    .setSender(Client.UserRef.newBuilder().setNodeId(4096).setUserId(1).build())
                                                    .setBody(ByteString.copyFrom(byteArrayOf(1, 2)))
                                                    .setCreatedAtHlc("hlc1")
                                                    .build()
                                            )
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                        }
                        Client.ClientEnvelope.BodyCase.ACK_MESSAGE -> if (!acked.isCompleted) acked.complete(Unit)
                        Client.ClientEnvelope.BodyCase.SEND_MESSAGE -> webSocket.send(
                            Client.ServerEnvelope.newBuilder()
                                .setSendMessageResponse(
                                    Client.SendMessageResponse.newBuilder()
                                        .setRequestId(env.sendMessage.requestId)
                                        .setMessage(
                                            Client.Message.newBuilder()
                                                .setRecipient(Client.UserRef.newBuilder().setNodeId(4096).setUserId(1025).build())
                                                .setNodeId(4096)
                                                .setSeq(8)
                                                .setSender(Client.UserRef.newBuilder().setNodeId(4096).setUserId(1025).build())
                                                .setBody(env.sendMessage.body)
                                                .setCreatedAtHlc("hlc2")
                                                .build()
                                        )
                                        .build()
                                )
                                .build()
                                .toByteArray()
                                .toByteString()
                        )
                        Client.ClientEnvelope.BodyCase.PING -> {
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setPong(Client.Pong.newBuilder().setRequestId(env.ping.requestId).build())
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                            webSocket.close(1000, "done")
                        }
                        else -> {}
                    }
                }
            }))
            server.start()

            val store = MemoryCursorStore()
            val client = TurntfClient(
                Config(
                    baseUrl = server.url("/").toString(),
                    credentials = Credentials(4096, 1025, plainPassword("alice-password")),
                    cursorStore = store,
                    pingInterval = java.time.Duration.ofHours(1)
                )
            )

            val job = launch {
                client.connect()
            }
            job.join()
            assertNotNull(client.loginState.value)
            assertEquals(ConnectionState.CONNECTED, client.connectionState.value)
            assertEquals("alice.login", client.loginState.value?.user?.loginName)
            acked.await()

            val message = client.sendMessage(SendMessageInput(UserRef(4096, 1025), "payload".encodeToByteArray()))
            assertEquals(8, message.seq)
            client.ping()
            client.close()
            assertEquals(ConnectionState.CLOSED, client.connectionState.value)
        }
    }

    @Test
    fun userMetadataRpc() = runTest {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onMessage(webSocket: WebSocket, bytes: okio.ByteString) {
                    val env = Client.ClientEnvelope.parseFrom(bytes.toByteArray())
                    when (env.bodyCase) {
                        Client.ClientEnvelope.BodyCase.LOGIN -> {
                            assertTrue(BCrypt.checkpw("alice-password", env.login.password))
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setLoginResponse(
                                        Client.LoginResponse.newBuilder()
                                            .setUser(
                                                Client.User.newBuilder()
                                                    .setNodeId(4096)
                                                    .setUserId(1025)
                                                    .setUsername("alice")
                                                    .setRole("user")
                                                    .setLoginName("alice.login")
                                                    .build()
                                            )
                                            .setProtocolVersion("client-v1alpha5")
                                            .setSessionRef(Client.SessionRef.newBuilder().setServingNodeId(4096).setSessionId("session-a").build())
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                        }
                        Client.ClientEnvelope.BodyCase.GET_USER_METADATA -> {
                            assertEquals(4096, env.getUserMetadata.owner.nodeId)
                            assertEquals(1025, env.getUserMetadata.owner.userId)
                            assertEquals("prefs.theme", env.getUserMetadata.key)
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setGetUserMetadataResponse(
                                        Client.GetUserMetadataResponse.newBuilder()
                                            .setRequestId(env.getUserMetadata.requestId)
                                            .setMetadata(
                                                Client.UserMetadata.newBuilder()
                                                    .setOwner(Client.UserRef.newBuilder().setNodeId(4096).setUserId(1025).build())
                                                    .setKey("prefs.theme")
                                                    .setValue(ByteString.copyFrom(byteArrayOf(1, 2)))
                                                    .setUpdatedAt("hlc-meta-1")
                                                    .setExpiresAt("2026-05-01T00:00:00Z")
                                                    .setOriginNodeId(4096)
                                                    .build()
                                            )
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                        }
                        Client.ClientEnvelope.BodyCase.UPSERT_USER_METADATA -> {
                            assertEquals("prefs.theme", env.upsertUserMetadata.key)
                            assertTrue(env.upsertUserMetadata.hasExpiresAt())
                            assertEquals("2026-05-01T00:00:00Z", env.upsertUserMetadata.expiresAt.value)
                            assertContentEquals(byteArrayOf(3, 4), env.upsertUserMetadata.value.toByteArray())
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setUpsertUserMetadataResponse(
                                        Client.UpsertUserMetadataResponse.newBuilder()
                                            .setRequestId(env.upsertUserMetadata.requestId)
                                            .setMetadata(
                                                Client.UserMetadata.newBuilder()
                                                    .setOwner(Client.UserRef.newBuilder().setNodeId(4096).setUserId(1025).build())
                                                    .setKey("prefs.theme")
                                                    .setValue(env.upsertUserMetadata.value)
                                                    .setUpdatedAt("hlc-meta-2")
                                                    .setExpiresAt(env.upsertUserMetadata.expiresAt.value)
                                                    .setOriginNodeId(4096)
                                                    .build()
                                            )
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                        }
                        Client.ClientEnvelope.BodyCase.DELETE_USER_METADATA -> {
                            assertEquals("prefs.theme", env.deleteUserMetadata.key)
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setDeleteUserMetadataResponse(
                                        Client.DeleteUserMetadataResponse.newBuilder()
                                            .setRequestId(env.deleteUserMetadata.requestId)
                                            .setMetadata(
                                                Client.UserMetadata.newBuilder()
                                                    .setOwner(Client.UserRef.newBuilder().setNodeId(4096).setUserId(1025).build())
                                                    .setKey("prefs.theme")
                                                    .setValue(ByteString.copyFrom(byteArrayOf(3, 4)))
                                                    .setUpdatedAt("hlc-meta-2")
                                                    .setDeletedAt("hlc-meta-3")
                                                    .setExpiresAt("2026-05-01T00:00:00Z")
                                                    .setOriginNodeId(4096)
                                                    .build()
                                            )
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                        }
                        Client.ClientEnvelope.BodyCase.SCAN_USER_METADATA -> {
                            assertEquals("prefs.", env.scanUserMetadata.prefix)
                            assertEquals("prefs.theme", env.scanUserMetadata.after)
                            assertEquals(2, env.scanUserMetadata.limit)
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setScanUserMetadataResponse(
                                        Client.ScanUserMetadataResponse.newBuilder()
                                            .setRequestId(env.scanUserMetadata.requestId)
                                            .addItems(
                                                Client.UserMetadata.newBuilder()
                                                    .setOwner(Client.UserRef.newBuilder().setNodeId(4096).setUserId(1025).build())
                                                    .setKey("prefs.theme")
                                                    .setValue(ByteString.copyFrom(byteArrayOf(3, 4)))
                                                    .setUpdatedAt("hlc-meta-2")
                                                    .setOriginNodeId(4096)
                                                    .build()
                                            )
                                            .addItems(
                                                Client.UserMetadata.newBuilder()
                                                    .setOwner(Client.UserRef.newBuilder().setNodeId(4096).setUserId(1025).build())
                                                    .setKey("prefs.lang")
                                                    .setValue(ByteString.copyFrom(byteArrayOf(5, 6)))
                                                    .setUpdatedAt("hlc-meta-4")
                                                    .setOriginNodeId(4096)
                                                    .build()
                                            )
                                            .setCount(2)
                                            .setNextAfter("prefs.lang")
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                            webSocket.close(1000, "done")
                        }
                        else -> {}
                    }
                }
            }))
            server.start()

            val client = TurntfClient(
                Config(
                    baseUrl = server.url("/").toString(),
                    credentials = Credentials(4096, 1025, plainPassword("alice-password")),
                    reconnect = false,
                    pingInterval = java.time.Duration.ofHours(1)
                )
            )

            val job = launch {
                client.connect()
            }
            job.join()

            val owner = UserRef(4096, 1025)
            val metadata = client.getUserMetadata(owner, "prefs.theme")
            assertContentEquals(byteArrayOf(1, 2), metadata.value)
            assertNull(metadata.typedValue)
            assertEquals("2026-05-01T00:00:00Z", metadata.expiresAt)

            val upserted = client.upsertUserMetadata(owner, "prefs.theme", byteArrayOf(3, 4), "2026-05-01T00:00:00Z")
            assertEquals("hlc-meta-2", upserted.updatedAt)
            assertContentEquals(byteArrayOf(3, 4), upserted.value)
            assertNull(upserted.typedValue)

            val deleted = client.deleteUserMetadata(owner, "prefs.theme")
            assertEquals("hlc-meta-3", deleted.deletedAt)
            assertNull(deleted.typedValue)

            val scan = client.scanUserMetadata(owner, prefix = "prefs.", after = "prefs.theme", limit = 2)
            assertEquals(2, scan.count)
            assertEquals("prefs.lang", scan.nextAfter)
            assertEquals(listOf("prefs.theme", "prefs.lang"), scan.items.map { it.key })
            assertContentEquals(byteArrayOf(5, 6), scan.items.last().value)
            assertTrue(scan.items.all { it.typedValue == null })

            client.close()
        }
    }

    @Test
    fun listUsersRpcSupportsFiltersAndRedactedLoginName() = runTest {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                private var listUsersRequests = 0

                override fun onMessage(webSocket: WebSocket, bytes: okio.ByteString) {
                    val env = Client.ClientEnvelope.parseFrom(bytes.toByteArray())
                    when (env.bodyCase) {
                        Client.ClientEnvelope.BodyCase.LOGIN -> {
                            assertTrue(BCrypt.checkpw("alice-password", env.login.password))
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setLoginResponse(
                                        Client.LoginResponse.newBuilder()
                                            .setUser(
                                                Client.User.newBuilder()
                                                    .setNodeId(4096)
                                                    .setUserId(1025)
                                                    .setUsername("alice")
                                                    .setRole("user")
                                                    .setLoginName("alice.login")
                                                    .build()
                                            )
                                            .setProtocolVersion("client-v1alpha5")
                                            .setSessionRef(Client.SessionRef.newBuilder().setServingNodeId(4096).setSessionId("session-a").build())
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                        }
                        Client.ClientEnvelope.BodyCase.LIST_USERS -> {
                            listUsersRequests += 1
                            val request = env.listUsers
                            when (listUsersRequests) {
                                1 -> {
                                    assertEquals("", request.name)
                                    assertFalse(request.hasUid())
                                    webSocket.send(
                                        Client.ServerEnvelope.newBuilder()
                                            .setListUsersResponse(
                                                Client.ListUsersResponse.newBuilder()
                                                    .setRequestId(request.requestId)
                                                    .addItems(
                                                        Client.User.newBuilder()
                                                            .setNodeId(4096)
                                                            .setUserId(1025)
                                                            .setUsername("alice")
                                                            .setRole("user")
                                                            .setLoginName("alice.login")
                                                            .build()
                                                    )
                                                    .addItems(
                                                        Client.User.newBuilder()
                                                            .setNodeId(4096)
                                                            .setUserId(1027)
                                                            .setUsername("carol")
                                                            .setRole("user")
                                                            .build()
                                                    )
                                                    .setCount(2)
                                                    .build()
                                            )
                                            .build()
                                            .toByteArray()
                                            .toByteString()
                                    )
                                }
                                2 -> {
                                    assertEquals("carol visible", request.name)
                                    assertFalse(request.hasUid())
                                    webSocket.send(
                                        Client.ServerEnvelope.newBuilder()
                                            .setListUsersResponse(
                                                Client.ListUsersResponse.newBuilder()
                                                    .setRequestId(request.requestId)
                                                    .addItems(
                                                        Client.User.newBuilder()
                                                            .setNodeId(4096)
                                                            .setUserId(1027)
                                                            .setUsername("carol")
                                                            .setRole("user")
                                                            .build()
                                                    )
                                                    .setCount(1)
                                                    .build()
                                            )
                                            .build()
                                            .toByteArray()
                                            .toByteString()
                                    )
                                }
                                3 -> {
                                    assertTrue(request.hasUid())
                                    assertEquals(4096, request.uid.nodeId)
                                    assertEquals(1027, request.uid.userId)
                                    webSocket.send(
                                        Client.ServerEnvelope.newBuilder()
                                            .setListUsersResponse(
                                                Client.ListUsersResponse.newBuilder()
                                                    .setRequestId(request.requestId)
                                                    .addItems(
                                                        Client.User.newBuilder()
                                                            .setNodeId(4096)
                                                            .setUserId(1027)
                                                            .setUsername("carol")
                                                            .setRole("user")
                                                            .build()
                                                    )
                                                    .setCount(1)
                                                    .build()
                                            )
                                            .build()
                                            .toByteArray()
                                            .toByteString()
                                    )
                                }
                                4 -> {
                                    assertEquals("carol", request.name)
                                    assertTrue(request.hasUid())
                                    assertEquals(4096, request.uid.nodeId)
                                    assertEquals(1027, request.uid.userId)
                                    webSocket.send(
                                        Client.ServerEnvelope.newBuilder()
                                            .setListUsersResponse(
                                                Client.ListUsersResponse.newBuilder()
                                                    .setRequestId(request.requestId)
                                                    .addItems(
                                                        Client.User.newBuilder()
                                                            .setNodeId(4096)
                                                            .setUserId(1027)
                                                            .setUsername("carol")
                                                            .setRole("user")
                                                            .build()
                                                    )
                                                    .setCount(1)
                                                    .build()
                                            )
                                            .build()
                                            .toByteArray()
                                            .toByteString()
                                    )
                                    webSocket.close(1000, "done")
                                }
                            }
                        }
                        else -> {}
                    }
                }
            }))
            server.start()

            val client = TurntfClient(
                Config(
                    baseUrl = server.url("/").toString(),
                    credentials = Credentials(4096, 1025, plainPassword("alice-password")),
                    reconnect = false,
                    pingInterval = java.time.Duration.ofHours(1)
                )
            )

            val job = launch { client.connect() }
            job.join()

            val visibleUsers = client.listUsers(UserListFilter(uid = UserRef(0, 0)))
            assertEquals(2, visibleUsers.size)
            assertEquals("alice.login", visibleUsers.first().loginName)

            val filteredByName = client.listUsers(UserListFilter(name = "  carol visible  "))
            assertEquals(1, filteredByName.size)
            assertEquals("carol", filteredByName.single().username)
            assertEquals("", filteredByName.single().loginName)

            val filteredByUid = client.listUsers(UserListFilter(uid = UserRef(4096, 1027)))
            assertEquals(listOf(1027L), filteredByUid.map { it.userId })

            val filteredByNameAndUid = client.listUsers(UserListFilter(name = "carol", uid = UserRef(4096, 1027)))
            assertEquals(listOf(1027L), filteredByNameAndUid.map { it.userId })

            assertFailsWith<IllegalArgumentException> {
                client.listUsers(UserListFilter(uid = UserRef(4096, 0)))
            }

            client.close()
        }
    }

    @Test
    fun loginByLoginNameAndUserRpcFields() = runTest {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onMessage(webSocket: WebSocket, bytes: okio.ByteString) {
                    val env = Client.ClientEnvelope.parseFrom(bytes.toByteArray())
                    when (env.bodyCase) {
                        Client.ClientEnvelope.BodyCase.LOGIN -> {
                            assertEquals("alice.login", env.login.loginName)
                            assertFalse(env.login.hasUser())
                            assertTrue(BCrypt.checkpw("alice-password", env.login.password))
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setLoginResponse(
                                        Client.LoginResponse.newBuilder()
                                            .setUser(
                                                Client.User.newBuilder()
                                                    .setNodeId(4096)
                                                    .setUserId(1025)
                                                    .setUsername("alice")
                                                    .setRole("user")
                                                    .setLoginName("alice.login")
                                                    .build()
                                            )
                                            .setProtocolVersion("client-v1alpha5")
                                            .setSessionRef(Client.SessionRef.newBuilder().setServingNodeId(4096).setSessionId("session-a").build())
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                        }
                        Client.ClientEnvelope.BodyCase.CREATE_USER -> {
                            assertEquals("bob.login", env.createUser.loginName)
                            assertTrue(BCrypt.checkpw("bob-password", env.createUser.password))
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setCreateUserResponse(
                                        Client.CreateUserResponse.newBuilder()
                                            .setRequestId(env.createUser.requestId)
                                            .setUser(
                                                Client.User.newBuilder()
                                                    .setNodeId(4096)
                                                    .setUserId(2048)
                                                    .setUsername("bob")
                                                    .setRole("user")
                                                    .setLoginName("bob.login")
                                                    .build()
                                            )
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                        }
                        Client.ClientEnvelope.BodyCase.UPDATE_USER -> {
                            assertTrue(env.updateUser.hasLoginName())
                            assertEquals("", env.updateUser.loginName.value)
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setUpdateUserResponse(
                                        Client.UpdateUserResponse.newBuilder()
                                            .setRequestId(env.updateUser.requestId)
                                            .setUser(
                                                Client.User.newBuilder()
                                                    .setNodeId(4096)
                                                    .setUserId(1025)
                                                    .setUsername("alice")
                                                    .setRole("user")
                                                    .setLoginName("")
                                                    .build()
                                            )
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                        }
                        Client.ClientEnvelope.BodyCase.LIST_NODE_LOGGED_IN_USERS -> {
                            webSocket.send(
                                Client.ServerEnvelope.newBuilder()
                                    .setListNodeLoggedInUsersResponse(
                                        Client.ListNodeLoggedInUsersResponse.newBuilder()
                                            .setRequestId(env.listNodeLoggedInUsers.requestId)
                                            .addItems(
                                                Client.LoggedInUser.newBuilder()
                                                    .setNodeId(4096)
                                                    .setUserId(1025)
                                                    .setUsername("alice")
                                                    .setLoginName("alice.login")
                                                    .build()
                                            )
                                            .setCount(1)
                                            .build()
                                    )
                                    .build()
                                    .toByteArray()
                                    .toByteString()
                            )
                            webSocket.close(1000, "done")
                        }
                        else -> {}
                    }
                }
            }))
            server.start()

            val client = TurntfClient(
                Config(
                    baseUrl = server.url("/").toString(),
                    credentials = Credentials(password = plainPassword("alice-password"), loginName = "alice.login"),
                    reconnect = false,
                    pingInterval = java.time.Duration.ofHours(1)
                )
            )

            val job = launch { client.connect() }
            job.join()
            assertEquals("alice.login", client.loginState.value?.user?.loginName)

            val created = client.createUser(
                CreateUserRequest(
                    username = "bob",
                    password = plainPassword("bob-password"),
                    role = "user",
                    loginName = "bob.login"
                )
            )
            assertEquals("bob.login", created.loginName)

            val updated = client.updateUser(UserRef(4096, 1025), UpdateUserRequest(loginName = ""))
            assertEquals("", updated.loginName)

            val loggedInUsers = client.listNodeLoggedInUsers(4096)
            assertEquals(listOf("alice.login"), loggedInUsers.map { it.loginName })

            client.close()
        }
    }

    @Test
    fun initialAndReconnectLoginFramesDeclareCurrentProtocolVersion() = runTest {
        MockWebServer().use { server ->
            val versions = ConcurrentLinkedQueue<String>()
            val attempts = AtomicInteger()
            val reconnected = CountDownLatch(1)
            val listener = object : WebSocketListener() {
                override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                    webSocket.close(code, reason)
                }

                override fun onMessage(webSocket: WebSocket, bytes: okio.ByteString) {
                    val env = Client.ClientEnvelope.parseFrom(bytes.toByteArray())
                    if (env.bodyCase != Client.ClientEnvelope.BodyCase.LOGIN) return
                    versions.add(env.login.protocolVersion)
                    val attempt = attempts.incrementAndGet()
                    webSocket.send(loginResponse("client-v1alpha5").toByteArray().toByteString())
                    if (attempt == 1) {
                        webSocket.close(1012, "restart")
                    } else {
                        reconnected.countDown()
                    }
                }
            }
            server.enqueue(MockResponse().withWebSocketUpgrade(listener))
            server.enqueue(MockResponse().withWebSocketUpgrade(listener))
            server.start()

            val client = TurntfClient(reconnectingConfig(server))
            client.connect()
            val completed = withContext(Dispatchers.IO) { reconnected.await(2, TimeUnit.SECONDS) }
            assertTrue(completed)
            assertEquals(listOf("client-v1alpha5", "client-v1alpha5"), versions.toList())
            client.close()
        }
    }

    @Test
    fun unsupportedProtocolServerErrorIsTerminal() = runTest {
        MockWebServer().use { server ->
            val versions = ConcurrentLinkedQueue<String>()
            server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                    webSocket.close(code, reason)
                }

                override fun onMessage(webSocket: WebSocket, bytes: okio.ByteString) {
                    val env = Client.ClientEnvelope.parseFrom(bytes.toByteArray())
                    versions.add(env.login.protocolVersion)
                    webSocket.send(
                        Client.ServerEnvelope.newBuilder()
                            .setError(
                                Client.Error.newBuilder()
                                    .setCode("unsupported_protocol_version")
                                    .setMessage("unsupported client protocol version")
                                    .setRequestId(0)
                                    .build()
                            )
                            .build()
                            .toByteArray()
                            .toByteString()
                    )
                }
            }))
            server.start()

            val client = TurntfClient(reconnectingConfig(server))
            val error = assertFailsWith<ServerError> { client.connect() }
            assertEquals("unsupported_protocol_version", error.code)
            withContext(Dispatchers.IO) { Thread.sleep(100) }
            assertEquals(listOf("client-v1alpha5"), versions.toList())
            assertEquals(1, server.requestCount)
            assertNull(client.loginState.value)
            client.close()
        }
    }

    @Test
    fun mismatchedLoginResponseVersionIsTerminalBeforeStatePublication() = runTest {
        listOf("", "client-v1alpha4").forEach { version ->
            MockWebServer().use { server ->
                val versions = ConcurrentLinkedQueue<String>()
                server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                    override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                        webSocket.close(code, reason)
                    }

                    override fun onMessage(webSocket: WebSocket, bytes: okio.ByteString) {
                        val env = Client.ClientEnvelope.parseFrom(bytes.toByteArray())
                        versions.add(env.login.protocolVersion)
                        webSocket.send(loginResponse(version).toByteArray().toByteString())
                    }
                }))
                server.start()

                val client = TurntfClient(reconnectingConfig(server))
                assertFailsWith<ProtocolError> { client.connect() }
                withContext(Dispatchers.IO) { Thread.sleep(100) }
                assertEquals(listOf("client-v1alpha5"), versions.toList())
                assertEquals(1, server.requestCount)
                assertNull(client.loginState.value)
                assertEquals(ConnectionState.DISCONNECTED, client.connectionState.value)
                client.close()
            }
        }
    }

    private fun reconnectingConfig(server: MockWebServer): Config = Config(
        baseUrl = server.url("/").toString(),
        credentials = Credentials(4096, 1025, plainPassword("alice-password")),
        reconnect = true,
        initialReconnectDelay = Duration.ofMillis(10),
        maxReconnectDelay = Duration.ofMillis(20),
        pingInterval = Duration.ofHours(1),
        requestTimeout = Duration.ofSeconds(1)
    )

    private fun loginResponse(protocolVersion: String): Client.ServerEnvelope =
        Client.ServerEnvelope.newBuilder()
            .setLoginResponse(
                Client.LoginResponse.newBuilder()
                    .setUser(
                        Client.User.newBuilder()
                            .setNodeId(4096)
                            .setUserId(1025)
                            .setUsername("alice")
                            .setRole("user")
                            .build()
                    )
                    .setProtocolVersion(protocolVersion)
                    .setSessionRef(
                        Client.SessionRef.newBuilder()
                            .setServingNodeId(4096)
                            .setSessionId("session-version-test")
                            .build()
                    )
                    .build()
            )
            .build()
}
