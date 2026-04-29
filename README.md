# turntf-kt

`turntf-kt` 是 `turntf` 的 Kotlin/JVM SDK，面向需要协程友好 API、长连接实时收发和可控重连语义的 JVM 客户端。

它同时提供两套互补能力：

- `TurntfHttpClient`：面向 HTTP JSON 管理/查询接口的 `suspend` API。
- `TurntfClient`：面向 WebSocket + Protobuf 长连接的协程/`Flow` API，负责登录、收发消息、自动重连、请求-响应关联和消息持久化顺序。

SDK 的公开模型尽量保持传输无关：

- `body`、`profileJson`、`configJson`、`eventJson` 等字段统一暴露为 `ByteArray`。
- 登录口令通过 `PasswordInput` 包装，支持“本地明文 bcrypt 后再发送”和“直接复用已有哈希”两种模式。
- 持久消息严格遵循 `saveMessage -> saveCursor -> ack` 顺序，避免断线后把尚未落盘的消息错误地声明为“已见”。

## 模块定位

`turntf-kt` 适合以下场景：

- Kotlin/JVM 服务或桌面客户端，需要以 `suspend` 风格访问 turntf 的 HTTP 管理接口。
- 需要通过 `SharedFlow`/`StateFlow` 消费实时消息、瞬时包和连接状态变化。
- 需要在重连后靠本地游标 `seen_messages` 恢复会话，避免重复消费已持久化消息。
- 需要按 `session_ref` 将 transient packet 定向发送到某个在线会话。

如果你的需求只是后台脚本或一次性管理任务，可以单独使用 `TurntfHttpClient`。如果需要长期在线、自动重连和实时推送，应直接使用 `TurntfClient`。

## 环境要求

- JDK 21
- Kotlin 2.1.10
- Gradle
- Protobuf 编译器通过 Gradle 插件自动拉起，生成 `lite` Java 类供 Kotlin 代码使用

模块的核心依赖来自 [build.gradle.kts](./build.gradle.kts)：

- `okhttp`：HTTP 与 WebSocket 传输
- `kotlinx-coroutines-core`：协程与 `Flow`
- `jbcrypt`：本地密码哈希
- `protobuf-javalite`：客户端 Protobuf 模型
- `jackson-databind` / `jackson-module-kotlin`：HTTP JSON 编解码

## 安装与构建

### 在 monorepo 内联调

在本仓库中，直接进入模块目录执行 Gradle 任务即可：

```bash
cd turntf-kt
gradle test
gradle build
```

### 发布到本地 Maven

如果你的业务工程不直接位于本 monorepo 中，可以先发布到本地 Maven，再在应用侧引入：

```bash
cd turntf-kt
gradle publishToMavenLocal
```

```kotlin
dependencies {
    implementation("io.github.tursom:turntf-kt:0.1.0")
}
```

如果你采用源码联调，也可以把 `turntf-kt` 作为 Gradle 复合构建或子模块引入。

## 快速开始

### 1. HTTP 登录并拿到 Bearer token

```kotlin
import io.github.tursom.turntf.kotlin.TurntfHttpClient

val http = TurntfHttpClient("http://127.0.0.1:8080")
val token = http.login(nodeId = 4096, userId = 1, password = "root")
```

`TurntfHttpClient.login()` 会先在本地对明文口令做 bcrypt，再调用 `POST /auth/login`。如果你已经持有哈希值，可以改用 `loginWithPassword()`。

### 2. 建立实时连接并发送持久消息

```kotlin
import io.github.tursom.turntf.kotlin.*
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

runBlocking {
    val client = TurntfClient(
        Config(
            baseUrl = "http://127.0.0.1:8080",
            credentials = Credentials(
                nodeId = 4096,
                userId = 1025,
                password = plainPassword("alice-password")
            )
        )
    )

    val eventJob = launch {
        client.events.collectLatest { event ->
            when (event) {
                is ClientEvent.Login -> println("session=${event.info.sessionRef}")
                is ClientEvent.MessageReceived -> println("message seq=${event.message.seq}")
                is ClientEvent.PacketReceived -> println("packet id=${event.packet.packetId}")
                is ClientEvent.Error -> println("stream error=${event.error.message}")
                is ClientEvent.Disconnect -> println("disconnected=${event.error.message}")
            }
        }
    }

    client.connect()
    client.sendMessage(
        SendMessageInput(
            target = UserRef(4096, 1025),
            body = "hello".encodeToByteArray()
        )
    )
    client.close()
    eventJob.cancel()
}
```

`connect()` 只有在第一条已认证会话可用后才会返回；`close()` 会停止重连、关闭当前 WebSocket，并让所有等待中的 RPC 以异常结束。

## API 概览

### `TurntfHttpClient`

所有 HTTP 方法都是 `suspend`，典型能力包括：

- 鉴权：`login()`、`loginWithPassword()`
- 用户管理：`createUser()`、`createChannel()`
- 消息收发：`listMessages()`、`postMessage()`、`postPacket()`
- 附件/订阅/黑名单：`upsertAttachment()`、`deleteAttachment()`、`listAttachments()`、`createSubscription()`、`blockUser()`、`unblockUser()`
- 集群查询：`listClusterNodes()`、`listNodeLoggedInUsers()`

HTTP 层会把 REST 中嵌套 JSON 的 `profile` / `config_json` 统一规整回 SDK 的 `ByteArray` 模型，因此应用层无需同时维护两套消息结构。

### `TurntfClient`

`TurntfClient` 在长连接上完成以下工作：

- 第一次登录与后续重登录
- `request_id` 关联下的 RPC 请求/响应
- `MessagePushed` 和 `sendMessageResponse.message` 的本地持久化
- `CursorStore` 管理、`seen_messages` 回放和自动 `AckMessage`
- `events` / `loginState` / `connectionState` 三套流式状态暴露
- 自动 ping、自动重连、登录失败停重试判定

`TurntfClient` 还内置 `val http = TurntfHttpClient(...)`，便于在同一份 `baseUrl` / `OkHttpClient` 配置上复用 HTTP 登录或后台接口。

## 文档导航

- [实时客户端与 HTTP API 说明](./docs/realtime-client.md)
- [开发、测试与 proto 同步说明](./docs/development.md)

## 共享语义速记

在阅读或扩展 SDK 时，下面几条语义最重要：

- `AckMessage` 只是连接内提示；真正决定重连去重的是下次登录时上报的 `LoginRequest.seen_messages`。
- SDK 收到持久消息后先 `saveMessage()`，再 `saveCursor()`，最后才尝试 `ack`。
- `sendMessage()` 的持久化回包也会推进本地游标，避免“自己刚发出的消息”在重连后又被当成未见消息重复消费。
- `PacketReceived` 不参与游标、历史补发和 ack；如果业务要暂存瞬时包，应按 `packet_id` 自行去重。
- `session_ref` 来自登录成功后的 `LoginResponse`，只能用于 transient packet 的会话定向发送。
- `realtimeStream = true` 时连接的是 `/ws/realtime`，能力面会比默认 `/ws/client` 更窄，只适合以瞬时互动为主的场景。
