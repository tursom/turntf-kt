package io.github.tursom.turntf.kotlin

import com.google.protobuf.ByteString
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import notifier.client.v1.Client
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.ByteString.Companion.toByteString
import org.mindrot.jbcrypt.BCrypt
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
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
                                            .setProtocolVersion("client-v1alpha1")
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
                                            .setProtocolVersion("client-v1alpha1")
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
            assertEquals("2026-05-01T00:00:00Z", metadata.expiresAt)

            val upserted = client.upsertUserMetadata(owner, "prefs.theme", byteArrayOf(3, 4), "2026-05-01T00:00:00Z")
            assertEquals("hlc-meta-2", upserted.updatedAt)
            assertContentEquals(byteArrayOf(3, 4), upserted.value)

            val deleted = client.deleteUserMetadata(owner, "prefs.theme")
            assertEquals("hlc-meta-3", deleted.deletedAt)

            val scan = client.scanUserMetadata(owner, prefix = "prefs.", after = "prefs.theme", limit = 2)
            assertEquals(2, scan.count)
            assertEquals("prefs.lang", scan.nextAfter)
            assertEquals(listOf("prefs.theme", "prefs.lang"), scan.items.map { it.key })
            assertContentEquals(byteArrayOf(5, 6), scan.items.last().value)

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
                                            .setProtocolVersion("client-v1alpha1")
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
}
