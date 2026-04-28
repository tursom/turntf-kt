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
import kotlin.test.assertEquals
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
                                            .setUser(Client.User.newBuilder().setNodeId(4096).setUserId(1025).setUsername("alice").setRole("user").build())
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
            acked.await()

            val message = client.sendMessage(SendMessageInput(UserRef(4096, 1025), "payload".encodeToByteArray()))
            assertEquals(8, message.seq)
            client.ping()
            client.close()
            assertEquals(ConnectionState.CLOSED, client.connectionState.value)
        }
    }
}
