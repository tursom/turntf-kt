package io.github.tursom.turntf.kotlin

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.mindrot.jbcrypt.BCrypt
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TurntfHttpClientTest {
    private val mapper = jacksonObjectMapper()

    @Test
    fun requestsAndEncoding() = runTest {
        MockWebServer().use { server ->
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    return when ("${request.method} ${request.path}") {
                        "POST /auth/login" -> {
                            val body = mapper.readTree(request.body.readUtf8())
                            if (body.has("login_name")) {
                                assertEquals("alice.login", body.path("login_name").asText())
                                assertTrue(body.path("node_id").isMissingNode)
                                assertTrue(body.path("user_id").isMissingNode)
                                assertTrue(BCrypt.checkpw("root", body.path("password").asText()))
                                json(200, """{"token":"login-name-token"}""")
                            } else {
                                assertEquals(4096, body.path("node_id").asInt())
                                assertEquals(1, body.path("user_id").asInt())
                                assertTrue(BCrypt.checkpw("root", body.path("password").asText()))
                                json(200, """{"token":"admin-token"}""")
                            }
                        }
                        "POST /users" -> {
                            val body = mapper.readTree(request.body.readUtf8())
                            assertTrue(BCrypt.checkpw("alice-password", body.path("password").asText()))
                            assertEquals("alice.login", body.path("login_name").asText())
                            json(201, """{"node_id":4096,"user_id":1025,"username":"alice","login_name":"alice.login","role":"user","profile":{"tier":"gold"}}""")
                        }
                        "GET /users" ->
                            json(200, """[
                                {"node_id":4096,"user_id":1025,"username":"alice","login_name":"alice.login","role":"user","profile":{"display_name":"Alice Visible"}},
                                {"node_id":4096,"user_id":1027,"username":"carol","role":"user","profile":{"display_name":"Carol Visible"}}
                            ]""".trimIndent())
                        "GET /users?name=carol+visible" ->
                            json(200, """[
                                {"node_id":4096,"user_id":1027,"username":"carol","role":"user","profile":{"display_name":"Carol Visible"}}
                            ]""".trimIndent())
                        "GET /users?uid=4096%3A1027" ->
                            json(200, """[
                                {"node_id":4096,"user_id":1027,"username":"carol","role":"user","profile":{"display_name":"Carol Visible"}}
                            ]""".trimIndent())
                        "GET /users?name=carol&uid=4096%3A1027" ->
                            json(200, """[
                                {"node_id":4096,"user_id":1027,"username":"carol","role":"user","profile":{"display_name":"Carol Visible"}}
                            ]""".trimIndent())
                        "GET /cluster/nodes/4096/logged-in-users" ->
                            json(200, """{"items":[{"node_id":4096,"user_id":1025,"username":"alice","login_name":"alice.login"}],"count":1}""")
                        "GET /nodes/4096/users/1025/metadata/settings.theme" -> {
                            json(200, """{"owner":{"node_id":4096,"user_id":1025},"key":"settings.theme","value":"AQI=","updated_at":"hlc-meta-1","expires_at":"2026-05-01T00:00:00Z","origin_node_id":4096}""")
                        }
                        "PUT /nodes/4096/users/1025/metadata/settings.theme" -> {
                            val body = mapper.readTree(request.body.readUtf8())
                            assertEquals("AQI=", body.path("value").asText())
                            assertEquals("2026-05-01T00:00:00Z", body.path("expires_at").asText())
                            json(201, """{"owner":{"node_id":4096,"user_id":1025},"key":"settings.theme","value":"AQI=","updated_at":"hlc-meta-2","expires_at":"2026-05-01T00:00:00Z","origin_node_id":4096}""")
                        }
                        "DELETE /nodes/4096/users/1025/metadata/settings.theme" -> {
                            json(200, """{"owner":{"node_id":4096,"user_id":1025},"key":"settings.theme","value":"AQI=","updated_at":"hlc-meta-2","deleted_at":"hlc-meta-3","expires_at":"2026-05-01T00:00:00Z","origin_node_id":4096}""")
                        }
                        "GET /nodes/4096/users/1025/metadata?prefix=settings.&after=settings.theme&limit=2" -> {
                            json(200, """{"items":[{"owner":{"node_id":4096,"user_id":1025},"key":"settings.theme","value":"AwQ=","updated_at":"hlc-meta-4","origin_node_id":4096}],"count":1,"next_after":"settings.theme"}""")
                        }
                        "GET /nodes/4096/users/1025/messages?limit=20" -> {
                            json(200, """{"items":[{"recipient":{"node_id":4096,"user_id":1025},"node_id":4096,"seq":3,"sender":{"node_id":4096,"user_id":1},"body":"/wA=","created_at":"hlc1"}]}""")
                        }
                        "GET /nodes/4096/users/1025/messages?limit=20&peer_node_id=100&peer_user_id=200" -> {
                            json(200, """{"items":[{"recipient":{"node_id":4096,"user_id":1025},"node_id":4096,"seq":3,"sender":{"node_id":100,"user_id":200},"body":"/wA=","created_at":"hlc1"}]}""")
                        }
                        "GET /nodes/0/users/0/messages" -> {
                            json(200, """{"items":[]}""")
                        }
                        "POST /nodes/4096/users/1025/messages" -> {
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
            val loginNameToken = client.login("alice.login", "root")
            assertEquals("login-name-token", loginNameToken)

            val user = client.createUser(
                token,
                CreateUserRequest("alice", plainPassword("alice-password"), """{"tier":"gold"}""".encodeToByteArray(), "user", "alice.login")
            )
            assertEquals(4096, user.nodeId)
            assertEquals("alice.login", user.loginName)

            val visibleUsers = client.listUsers(token)
            assertEquals(2, visibleUsers.size)
            assertEquals("alice.login", visibleUsers.first().loginName)

            val zeroUidUsers = client.listUsers(token, UserListFilter(uid = UserRef(0, 0)))
            assertEquals(2, zeroUidUsers.size)

            val filteredByName = client.listUsers(token, UserListFilter(name = "  carol visible  "))
            assertEquals(1, filteredByName.size)
            assertEquals("carol", filteredByName.single().username)
            assertEquals("", filteredByName.single().loginName)

            val filteredByUid = client.listUsers(token, UserListFilter(uid = UserRef(4096, 1027)))
            assertEquals(listOf(1027L), filteredByUid.map { it.userId })

            val filteredByNameAndUid = client.listUsers(token, UserListFilter(name = "carol", uid = UserRef(4096, 1027)))
            assertEquals(listOf(1027L), filteredByNameAndUid.map { it.userId })

            assertFailsWith<IllegalArgumentException> {
                client.listUsers(token, UserListFilter(uid = UserRef(4096, 0)))
            }

            val loggedInUsers = client.listNodeLoggedInUsers(token, 4096)
            assertEquals(listOf("alice.login"), loggedInUsers.map { it.loginName })

            val metadata = client.getUserMetadata(token, UserRef(4096, 1025), "settings.theme")
            assertContentEquals(byteArrayOf(1, 2), metadata.value)
            assertNull(metadata.typedValue)
            assertEquals("2026-05-01T00:00:00Z", metadata.expiresAt)

            val upserted = client.upsertUserMetadata(token, UserRef(4096, 1025), "settings.theme", byteArrayOf(1, 2), "2026-05-01T00:00:00Z")
            assertEquals("hlc-meta-2", upserted.updatedAt)
            assertNull(upserted.typedValue)

            val deleted = client.deleteUserMetadata(token, UserRef(4096, 1025), "settings.theme")
            assertEquals("hlc-meta-3", deleted.deletedAt)
            assertNull(deleted.typedValue)

            val scan = client.scanUserMetadata(token, UserRef(4096, 1025), prefix = "settings.", after = "settings.theme", limit = 2)
            assertEquals(1, scan.count)
            assertEquals("settings.theme", scan.nextAfter)
            assertContentEquals(byteArrayOf(3, 4), scan.items.single().value)
            assertNull(scan.items.single().typedValue)

            val items = client.listMessages(token, UserRef(4096, 1025), 20)
            assertEquals(1, items.size)
            assertContentEquals(byteArrayOf(0xff.toByte(), 0x00), items.first().body)

            val peerItems = client.listMessages(token, UserRef(4096, 1025), 20, peerNodeId = 100, peerUserId = 200)
            assertEquals(1, peerItems.size)
            assertEquals(100, peerItems.first().sender.nodeId)
            assertEquals(200, peerItems.first().sender.userId)

            // UserRef(0, 0) 作为"当前用户"sentinel，不再被 validateUserRef 拦截
            val currentUserItems = client.listMessages(token, UserRef(0, 0))
            assertEquals(0, currentUserItems.size)

            val created = client.postMessage(token, UserRef(4096, 1025), byteArrayOf(0xff.toByte(), 0x00))
            assertEquals(4, created.seq)
        }
    }

    @Test
    fun typedMetadataViewsSupportChannelOwners() = runTest {
        MockWebServer().use { server ->
            server.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    return when ("${request.method} ${request.path}") {
                        "GET /nodes/4096/users/2048/metadata/system.visible_to_others" ->
                            json(
                                200,
                                """{"owner":{"node_id":4096,"user_id":2048},"key":"system.visible_to_others","value":"${base64("false".encodeToByteArray())}","typed_value":{"kind":"bool","bool_value":false},"updated_at":"hlc-visible","origin_node_id":4096}"""
                            )
                        "PUT /nodes/4096/users/2048/metadata/system.visible_to_others" -> {
                            val body = mapper.readTree(request.body.readUtf8())
                            assertTrue(body.path("value").isMissingNode)
                            assertEquals("bool", body.path("typed_value").path("kind").asText())
                            assertFalse(body.path("typed_value").path("bool_value").asBoolean())
                            assertTrue(body.path("expires_at").isMissingNode)
                            json(
                                201,
                                """{"owner":{"node_id":4096,"user_id":2048},"key":"system.visible_to_others","value":"${base64("false".encodeToByteArray())}","typed_value":{"kind":"bool","bool_value":false},"updated_at":"hlc-visible-upsert","origin_node_id":4096}"""
                            )
                        }
                        "PUT /nodes/4096/users/2048/metadata/profile.score" -> {
                            val body = mapper.readTree(request.body.readUtf8())
                            assertTrue(body.path("value").isMissingNode)
                            assertEquals("number", body.path("typed_value").path("kind").asText())
                            assertEquals("7.5", body.path("typed_value").path("number_value").toString())
                            json(
                                201,
                                """{"owner":{"node_id":4096,"user_id":2048},"key":"profile.score","value":"${base64("7.5".encodeToByteArray())}","typed_value":{"kind":"number","number_value":7.5},"updated_at":"hlc-score","origin_node_id":4096}"""
                            )
                        }
                        "PUT /nodes/4096/users/2048/metadata/channel.config" -> {
                            val body = mapper.readTree(request.body.readUtf8())
                            assertTrue(body.path("value").isMissingNode)
                            assertEquals("json", body.path("typed_value").path("kind").asText())
                            assertEquals("blue", body.path("typed_value").path("json_value").path("theme").asText())
                            json(
                                201,
                                """{"owner":{"node_id":4096,"user_id":2048},"key":"channel.config","value":"${base64("""{"theme":"blue"}""".encodeToByteArray())}","typed_value":{"kind":"json","json_value":{"theme":"blue"}},"updated_at":"hlc-config","origin_node_id":4096}"""
                            )
                        }
                        "PUT /nodes/4096/users/2048/metadata/avatar.raw" -> {
                            val body = mapper.readTree(request.body.readUtf8())
                            assertTrue(body.path("value").isMissingNode)
                            assertEquals("bytes", body.path("typed_value").path("kind").asText())
                            assertEquals("AAE=", body.path("typed_value").path("bytes_value").asText())
                            json(
                                201,
                                """{"owner":{"node_id":4096,"user_id":2048},"key":"avatar.raw","value":"${base64(byteArrayOf(0, 1))}","updated_at":"hlc-avatar","origin_node_id":4096}"""
                            )
                        }
                        "GET /nodes/4096/users/2048/metadata?prefix=profile.&after=profile.nickname&limit=2" ->
                            json(
                                200,
                                """{"items":[{"owner":{"node_id":4096,"user_id":2048},"key":"profile.nickname","value":"${base64("\"Alice\"".encodeToByteArray())}","typed_value":{"kind":"string","string_value":"Alice"},"updated_at":"hlc-nickname","origin_node_id":4096},{"owner":{"node_id":4096,"user_id":2048},"key":"profile.score","value":"${base64("7.5".encodeToByteArray())}","typed_value":{"kind":"number","number_value":7.5},"updated_at":"hlc-score","origin_node_id":4096}],"count":2,"next_after":"profile.score"}"""
                            )
                        else -> MockResponse().setResponseCode(404)
                    }
                }
            }
            server.start()

            val client = TurntfHttpClient(server.url("/").toString())
            val owner = UserRef(4096, 2048)

            val visible = client.getUserMetadata("admin-token", owner, USER_METADATA_KEY_VISIBLE_TO_OTHERS)
            assertContentEquals("false".encodeToByteArray(), visible.value)
            assertFalse(assertIs<UserMetadataTypedValue.Bool>(visible.typedValue).value)

            val visibleUpsert = client.upsertUserMetadata("admin-token", owner, USER_METADATA_KEY_VISIBLE_TO_OTHERS, UserMetadataTypedValue.Bool(false))
            assertEquals("hlc-visible-upsert", visibleUpsert.updatedAt)
            assertFalse(assertIs<UserMetadataTypedValue.Bool>(visibleUpsert.typedValue).value)

            val score = client.upsertUserMetadata("admin-token", owner, "profile.score", UserMetadataTypedValue.NumberValue("7.5"))
            assertEquals("7.5", assertIs<UserMetadataTypedValue.NumberValue>(score.typedValue).literal)
            assertContentEquals("7.5".encodeToByteArray(), score.value)

            val config = client.upsertUserMetadata(
                "admin-token",
                owner,
                "channel.config",
                UserMetadataTypedValue.JsonValue(mapper.readTree("""{"theme":"blue"}"""))
            )
            assertContentEquals("""{"theme":"blue"}""".encodeToByteArray(), config.value)
            assertEquals("blue", assertIs<UserMetadataTypedValue.JsonValue>(config.typedValue).value.path("theme").asText())

            val avatar = client.upsertUserMetadata("admin-token", owner, "avatar.raw", UserMetadataTypedValue.Bytes(byteArrayOf(0, 1)))
            assertContentEquals(byteArrayOf(0, 1), avatar.value)
            assertNull(avatar.typedValue)

            val scan = client.scanUserMetadata("admin-token", owner, prefix = "profile.", after = "profile.nickname", limit = 2)
            assertEquals(2, scan.count)
            assertEquals("profile.score", scan.nextAfter)
            assertEquals("Alice", assertIs<UserMetadataTypedValue.StringValue>(scan.items.first().typedValue).value)
            assertEquals("7.5", assertIs<UserMetadataTypedValue.NumberValue>(scan.items.last().typedValue).literal)
        }
    }

    @Test
    fun visibilityMetadataHttpValidationRunsLocally() = runTest {
        val client = TurntfHttpClient("http://127.0.0.1")
        val owner = UserRef(4096, 2048)

        assertFailsWith<IllegalArgumentException> {
            client.upsertUserMetadata("admin-token", owner, USER_METADATA_KEY_VISIBLE_TO_OTHERS, UserMetadataTypedValue.StringValue("false"))
        }
        assertFailsWith<IllegalArgumentException> {
            client.upsertUserMetadata("admin-token", owner, USER_METADATA_KEY_VISIBLE_TO_OTHERS, UserMetadataTypedValue.Bool(false), "2026-05-01T00:00:00Z")
        }
        assertFailsWith<IllegalArgumentException> {
            client.upsertUserMetadata("admin-token", owner, USER_METADATA_KEY_VISIBLE_TO_OTHERS, byteArrayOf(0x00))
        }
    }

    private fun json(status: Int, body: String) = MockResponse()
        .setResponseCode(status)
        .setHeader("Content-Type", "application/json")
        .setBody(body)

    private fun base64(value: ByteArray): String = Base64.getEncoder().encodeToString(value)
}
