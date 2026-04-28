# turntf-kt

`turntf-kt` is a Kotlin/JVM SDK for turntf. It provides:

- suspend HTTP JSON client APIs
- coroutine/Flow-based realtime client APIs
- automatic reconnect and re-login
- `saveMessage -> saveCursor -> ack` message reliability

Quick start:

```kotlin
import io.github.tursom.turntf.kotlin.*

val client = TurntfClient(
    Config(
        baseUrl = "http://127.0.0.1:8080",
        credentials = Credentials(4096, 1025, plainPassword("alice-password"))
    )
)

client.connect()
client.sendMessage(SendMessageInput(UserRef(4096, 1025), "hello".encodeToByteArray()))
client.close()
```
