package io.github.tursom.turntf.kotlin

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.mindrot.jbcrypt.BCrypt
import okhttp3.OkHttpClient
import java.time.Duration

/** Indicates whether a password originated from plaintext input or from an existing hash. */
enum class PasswordSource {
    PLAIN,
    HASHED
}

/**
 * Password wrapper shared by HTTP and websocket flows.
 *
 * The SDK keeps the transport payload opaque while still preserving whether the value was hashed
 * locally or supplied pre-hashed by the caller.
 */
data class PasswordInput(
    val source: PasswordSource,
    val encoded: String
) {
    fun validate() {
        require(encoded.isNotEmpty()) { "password is required" }
    }

    fun wireValue(): String {
        validate()
        return encoded
    }
}

/** Hashes plaintext input with bcrypt for turntf login and user-management requests. */
fun hashPassword(plain: String): String {
    require(plain.isNotEmpty()) { "password is required" }
    return BCrypt.hashpw(plain, BCrypt.gensalt())
}

/** Creates a password payload from plaintext input by hashing it locally. */
fun plainPassword(plain: String): PasswordInput = PasswordInput(PasswordSource.PLAIN, hashPassword(plain))

/** Wraps an already-hashed password so the SDK can forward it without rehashing. */
fun hashedPassword(value: String): PasswordInput = PasswordInput(PasswordSource.HASHED, value)

/** Authenticated identity used by both HTTP login delegation and websocket login. */
data class Credentials(
    val nodeId: Long,
    val userId: Long,
    val password: PasswordInput
)

/**
 * Runtime configuration for [TurntfClient].
 *
 * The defaults are tuned for a long-lived session: reconnect enabled, 30-second ping interval,
 * 10-second RPC timeout, and automatic ack emission after durable message handling.
 */
data class Config(
    val baseUrl: String,
    val credentials: Credentials,
    val cursorStore: CursorStore = MemoryCursorStore(),
    val httpClient: OkHttpClient = OkHttpClient(),
    val reconnect: Boolean = true,
    val initialReconnectDelay: Duration = Duration.ofSeconds(1),
    val maxReconnectDelay: Duration = Duration.ofSeconds(30),
    val pingInterval: Duration = Duration.ofSeconds(30),
    val requestTimeout: Duration = Duration.ofSeconds(10),
    val ackMessages: Boolean = true,
    val transientOnly: Boolean = false,
    val realtimeStream: Boolean = false
)

data class UserRef(val nodeId: Long, val userId: Long)

data class SessionRef(val servingNodeId: Long, val sessionId: String) {
    fun isZero(): Boolean = servingNodeId == 0L && sessionId.isEmpty()
}

data class User(
    val nodeId: Long,
    val userId: Long,
    val username: String,
    val role: String,
    val profileJson: ByteArray = byteArrayOf(),
    val systemReserved: Boolean = false,
    val createdAt: String = "",
    val updatedAt: String = "",
    val originNodeId: Long = 0
)

/**
 * User-owned private metadata entry shared by HTTP and websocket APIs.
 *
 * The SDK always exposes [value] as raw bytes even though the HTTP transport serializes it as
 * base64, so callers can move between transports without changing their application model.
 */
data class UserMetadata(
    val owner: UserRef,
    val key: String,
    val value: ByteArray = byteArrayOf(),
    val updatedAt: String = "",
    val deletedAt: String = "",
    val expiresAt: String = "",
    val originNodeId: Long = 0
)

/**
 * Cursor-based scan result for user private metadata.
 *
 * [nextAfter] is fed back into the next scan call to continue from the last returned key.
 */
data class UserMetadataScanResult(
    val items: List<UserMetadata> = emptyList(),
    val count: Int = 0,
    val nextAfter: String = ""
)

data class MessageCursor(val nodeId: Long, val seq: Long)

enum class DeliveryMode(val wireValue: String) {
    UNSPECIFIED(""),
    BEST_EFFORT("best_effort"),
    ROUTE_RETRY("route_retry")
}

data class Message(
    val recipient: UserRef,
    val nodeId: Long,
    val seq: Long,
    val sender: UserRef,
    val body: ByteArray,
    val createdAtHlc: String
) {
    fun cursor(): MessageCursor = MessageCursor(nodeId, seq)
}

data class Packet(
    val packetId: Long,
    val sourceNodeId: Long,
    val targetNodeId: Long,
    val recipient: UserRef,
    val sender: UserRef,
    val body: ByteArray,
    val deliveryMode: DeliveryMode,
    val targetSession: SessionRef
)

data class RelayAccepted(
    val packetId: Long,
    val sourceNodeId: Long,
    val targetNodeId: Long,
    val recipient: UserRef,
    val deliveryMode: DeliveryMode,
    val targetSession: SessionRef
)

enum class AttachmentType(val wireValue: String) {
    CHANNEL_MANAGER("channel_manager"),
    CHANNEL_WRITER("channel_writer"),
    CHANNEL_SUBSCRIPTION("channel_subscription"),
    USER_BLACKLIST("user_blacklist")
}

data class Attachment(
    val owner: UserRef,
    val subject: UserRef,
    val attachmentType: AttachmentType,
    val configJson: ByteArray = byteArrayOf(),
    val attachedAt: String = "",
    val deletedAt: String = "",
    val originNodeId: Long = 0
)

data class Subscription(
    val subscriber: UserRef,
    val channel: UserRef,
    val subscribedAt: String = "",
    val deletedAt: String = "",
    val originNodeId: Long = 0
)

data class BlacklistEntry(
    val owner: UserRef,
    val blocked: UserRef,
    val blockedAt: String = "",
    val deletedAt: String = "",
    val originNodeId: Long = 0
)

data class Event(
    val sequence: Long,
    val eventId: Long,
    val eventType: String,
    val aggregate: String,
    val aggregateNodeId: Long,
    val aggregateId: Long,
    val hlc: String,
    val originNodeId: Long,
    val eventJson: ByteArray = byteArrayOf()
)

data class ClusterNode(
    val nodeId: Long,
    val isLocal: Boolean,
    val configuredUrl: String = "",
    val source: String = ""
)

data class LoggedInUser(
    val nodeId: Long,
    val userId: Long,
    val username: String
)

data class ResolvedUserSessions(
    val user: UserRef,
    val presence: List<OnlineNodePresence> = emptyList(),
    val sessions: List<ResolvedSession> = emptyList()
) {
    data class OnlineNodePresence(
        val servingNodeId: Long,
        val sessionCount: Int,
        val transportHint: String = ""
    )

    data class ResolvedSession(
        val session: SessionRef,
        val transport: String = "",
        val transientCapable: Boolean = false
    )
}

data class OperationsStatus(
    val nodeId: Long,
    val messageWindowSize: Int,
    val lastEventSequence: Long,
    val writeGateReady: Boolean,
    val conflictTotal: Long,
    val messageTrim: MessageTrimStatus = MessageTrimStatus(),
    val projection: ProjectionStatus = ProjectionStatus(),
    val peers: List<PeerStatus> = emptyList(),
    val eventLogTrim: EventLogTrimStatus = EventLogTrimStatus()
) {
    data class MessageTrimStatus(val trimmedTotal: Long = 0, val lastTrimmedAt: String = "")
    data class EventLogTrimStatus(val trimmedTotal: Long = 0, val lastTrimmedAt: String = "")
    data class ProjectionStatus(val pendingTotal: Long = 0, val lastFailedAt: String = "")
    data class PeerOriginStatus(
        val originNodeId: Long,
        val ackedEventId: Long,
        val appliedEventId: Long,
        val unconfirmedEvents: Long,
        val cursorUpdatedAt: String = "",
        val remoteLastEventId: Long,
        val pendingCatchup: Boolean
    )

    data class PeerStatus(
        val nodeId: Long,
        val configuredUrl: String = "",
        val source: String = "",
        val discoveredUrl: String = "",
        val discoveryState: String = "",
        val lastDiscoveredAt: String = "",
        val lastConnectedAt: String = "",
        val lastDiscoveryError: String = "",
        val connected: Boolean = false,
        val sessionDirection: String = "",
        val origins: List<PeerOriginStatus> = emptyList(),
        val pendingSnapshotPartitions: Int = 0,
        val remoteSnapshotVersion: String = "",
        val remoteMessageWindowSize: Int = 0,
        val clockOffsetMs: Long = 0,
        val lastClockSync: String = "",
        val snapshotDigestsSentTotal: Long = 0,
        val snapshotDigestsReceivedTotal: Long = 0,
        val snapshotChunksSentTotal: Long = 0,
        val snapshotChunksReceivedTotal: Long = 0,
        val lastSnapshotDigestAt: String = "",
        val lastSnapshotChunkAt: String = ""
    )
}

data class DeleteUserResult(val status: String, val user: UserRef)

data class LoginInfo(
    val user: User,
    val protocolVersion: String,
    val sessionRef: SessionRef
)

data class SendMessageInput(val target: UserRef, val body: ByteArray)

data class SendPacketInput(
    val target: UserRef,
    val body: ByteArray,
    val deliveryMode: DeliveryMode,
    val targetSession: SessionRef? = null
)

data class CreateUserRequest(
    val username: String,
    val password: PasswordInput? = null,
    val profileJson: ByteArray = byteArrayOf(),
    val role: String
)

data class UpdateUserRequest(
    val username: String? = null,
    val password: PasswordInput? = null,
    val profileJson: ByteArray? = null,
    val role: String? = null
)

/** Persists the durable cursors used by websocket reconnect and replay suppression. */
interface CursorStore {
    // Returned cursors are injected into the next websocket LoginRequest.seen_messages. Stores
    // should therefore preserve a stable set of durably handled persistent messages across restarts.
    suspend fun loadSeenMessages(): List<MessageCursor>
    suspend fun saveMessage(message: Message)
    suspend fun saveCursor(cursor: MessageCursor)
}

/** In-memory [CursorStore] implementation for tests, demos, and short-lived processes. */
class MemoryCursorStore : CursorStore {
    private val mutex = Mutex()
    private val messages = linkedMapOf<MessageCursor, Message>()
    private val order = mutableListOf<MessageCursor>()

    override suspend fun loadSeenMessages(): List<MessageCursor> = mutex.withLock { order.toList() }

    override suspend fun saveMessage(message: Message) {
        mutex.withLock {
            messages[message.cursor()] = message
        }
    }

    override suspend fun saveCursor(cursor: MessageCursor) {
        mutex.withLock {
            // A cursor can be acknowledged without the payload being available yet in custom store
            // implementations, so the in-memory variant keeps a placeholder entry to preserve that
            // contract and keep loadSeenMessages()/messages logically aligned.
            messages.putIfAbsent(cursor, Message(UserRef(0, 0), cursor.nodeId, cursor.seq, UserRef(0, 0), byteArrayOf(), ""))
            if (cursor !in order) {
                order += cursor
            }
        }
    }
}

/**
 * High-level realtime event stream emitted by [TurntfClient].
 *
 * Delivery events are published only after the SDK has completed the protocol bookkeeping
 * required for the corresponding frame.
 */
sealed interface ClientEvent {
    data class Login(val info: LoginInfo) : ClientEvent
    data class MessageReceived(val message: Message) : ClientEvent
    data class PacketReceived(val packet: Packet) : ClientEvent
    data class Error(val error: Throwable) : ClientEvent
    data class Disconnect(val error: Throwable) : ClientEvent
}

/** Connection lifecycle snapshot exposed by [TurntfClient.connectionState]. */
enum class ConnectionState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    CLOSED
}

/** Base exception type for SDK-level protocol, connection, and server failures. */
open class TurntfException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

class ServerError(
    val code: String,
    message: String,
    val requestId: Long
) : TurntfException(
    if (requestId == 0L) "turntf server error: $code ($message)"
    else "turntf server error: $code ($message), request_id=$requestId"
) {
    fun unauthorized(): Boolean = code == "unauthorized"
}

class ProtocolError(message: String) : TurntfException("turntf protocol error: $message")

class ConnectionError(val op: String, cause: Throwable) : TurntfException("turntf connection error during $op: ${cause.message}", cause)
