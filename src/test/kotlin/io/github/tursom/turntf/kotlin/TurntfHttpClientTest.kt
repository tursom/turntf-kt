package io.github.tursom.turntf.kotlin

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.mindrot.jbcrypt.BCrypt
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TurntfHttpClientTest {
    private val mapper = jacksonObjectMapper()

    @Test
    fun requestsAndEncoding() = runTest {
        MockWebServer().use { server ->
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    return when (request.path) {
                        "/auth/login" -> {
                            val body = mapper.readTree(request.body.readUtf8())
                            assertTrue(BCrypt.checkpw("root", body.path("password").asText()))
                            json(200, """{"token":"admin-token"}""")
                        }
                        "/users" -> {
                            val body = mapper.readTree(request.body.readUtf8())
                            assertTrue(BCrypt.checkpw("alice-password", body.path("password").asText()))
                            json(201, """{"node_id":4096,"user_id":1025,"username":"alice","role":"user","profile":{"tier":"gold"}}""")
                        }
                        "/nodes/4096/users/1025/messages?limit=20" -> {
                            json(200, """{"items":[{"recipient":{"node_id":4096,"user_id":1025},"node_id":4096,"seq":3,"sender":{"node_id":4096,"user_id":1},"body":"/wA=","created_at":"hlc1"}]}""")
                        }
                        "/nodes/4096/users/1025/messages" -> {
                            val body = mapper.readTree(request.body.readUtf8())
                            assertEquals("/wA=", body.path("body").asText())
                            json(201, """{"recipient":{"node_id":4096,"user_id":1025},"node_id":4096,"seq":4,"sender":{"node_id":4096,"user_id":1},"body":"/wA=","created_at":"hlc2"}""")
                        }
                        else -> MockResponse().setResponseCode(404)
                    }
                }
            }
            server.start()

            val client = TurntfHttpClient(server.url("/").toString())
            val token = client.login(4096, 1, "root")
            assertEquals("admin-token", token)

            val user = client.createUser(token, CreateUserRequest("alice", plainPassword("alice-password"), """{"tier":"gold"}""".encodeToByteArray(), "user"))
            assertEquals(4096, user.nodeId)

            val items = client.listMessages(token, UserRef(4096, 1025), 20)
            assertEquals(1, items.size)
            assertContentEquals(byteArrayOf(0xff.toByte(), 0x00), items.first().body)

            val created = client.postMessage(token, UserRef(4096, 1025), byteArrayOf(0xff.toByte(), 0x00))
            assertEquals(4, created.seq)
        }
    }

    private fun json(status: Int, body: String) = MockResponse()
        .setResponseCode(status)
        .setHeader("Content-Type", "application/json")
        .setBody(body)
}
