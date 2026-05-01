# turntf-kt SDK 总体使用指南

本文档面向 SDK 的使用者，说明如何正确引入、配置和组合使用 `turntf-kt` 的两种客户端，以及推荐的生产实践。

## 安装

### Maven 依赖

```kotlin
repositories {
    mavenCentral()
    // 如果使用了本地发布
    mavenLocal()
}

dependencies {
    implementation("io.github.tursom:turntf-kt:0.1.0")
}
```

### 源码联调

在 monorepo 内直接执行 Gradle 任务：

```bash
cd turntf-kt
gradle build
```

也可以将 turntf-kt 作为 Gradle 复合构建或子模块引入业务工程。

## 核心依赖

SDK 的直接依赖由 Gradle 自动传递引入：

- `okhttp 4.12.0` — HTTP 与 WebSocket 传输
- `kotlinx-coroutines-core 1.10.1` — 协程与 Flow
- `jbcrypt 0.4` — 本地密码哈希
- `protobuf-javalite 4.29.3` — 客户端 Protobuf 模型
- `jackson-databind 2.18.2` + `jackson-module-kotlin` — HTTP JSON 编解码

## 选择正确的客户端

SDK 提供两个入口，根据使用场景选择：

| 场景 | 推荐客户端 |
|------|-----------|
| 后台脚本/一次性管理任务 | `TurntfHttpClient` 单独使用 |
| 长期在线、实时推送、自动重连 | `TurntfClient` |
| 先用 HTTP 拿 token，再建立长连接 | `TurntfClient` 内置的 `http` 属性 |
| 只发瞬时包，不需要持久消息 | `TurntfClient` + `transientOnly = true` |
| 纯 WebSocket 实时互动 | `TurntfClient` + `realtimeStream = true` |

### 何时只使用 HTTP 客户端

- 需要脚本化查询集群状态或用户列表。
- 需要向某个用户发送一条持久消息或瞬时包，但不维持长连接。
- 需要执行管理操作（创建用户、管理订阅、操作黑名单）。

### 何时使用实时客户端

- 服务需要持续接收 `MessagePushed` / `PacketPushed` 推送。
- 需要在断线后自动重连并恢复会话，避免重复消费已持久化的消息。
- 需要统一消费实时事件流和连接状态流。
- 需要在 WebSocket 连接上复用 turntf 的管理/查询 RPC，减少 HTTP 开销。

## 密码处理

SDK 提供两种口令模式，使用 `PasswordInput` 包装：

```kotlin
// 明文模式 — SDK 内部立即做 bcrypt，只发送哈希值
val plain = plainPassword("my-secret")

// 哈希透传模式 — 调用方已持有 bcrypt 哈希，SDK 直接发送
val hashed = hashedPassword("$2a$10$...")
```

推荐做法：
- 应用首次拿到用户明文时使用 `plainPassword()`。
- 如果哈希来自外部安全存储或其他认证系统，使用 `hashedPassword()`。
- `TurntfClient` 在登录帧中发送 bcrypt 哈希，HTTP 客户端同理。

## 配置模式

### TurntfHttpClient 配置

```kotlin
// 最简单的用法
val http = TurntfHttpClient("http://127.0.0.1:8080")

// 复用自定义 OkHttpClient（设置超时、代理、TLS 等）
val customClient = OkHttpClient.Builder()
    .connectTimeout(10, java.util.concurrent.TimeUnit.SECONDS)
    .build()
val http = TurntfHttpClient("http://127.0.0.1:8080", customClient)
```

### TurntfClient 配置

```kotlin
val client = TurntfClient(
    Config(
        baseUrl = "http://127.0.0.1:8080",
        credentials = Credentials(
            nodeId = 4096,
            userId = 1025,
            password = plainPassword("alice-password")
        ),
        // 生产环境建议自定义 CursorStore
        cursorStore = myDatabaseCursorStore,
        // 自动重连
        reconnect = true,
        initialReconnectDelay = Duration.ofSeconds(2),
        maxReconnectDelay = Duration.ofSeconds(60),
        // 自动 ack
        ackMessages = true,
        // 10 秒 RPC 超时
        requestTimeout = Duration.ofSeconds(10),
        // 30 秒 ping 间隔
        pingInterval = Duration.ofSeconds(30)
    )
)
```

如果服务端已经启用登录名双轨登录，也可以把 `credentials` 写成 `Credentials(loginName = "alice.login", password = plainPassword("alice-password"))`。

## 生命周期管理

### 典型启动顺序

```kotlin
val client = TurntfClient(config)

// 尽量在应用启动早期开始收集事件
val eventJob = scope.launch {
    client.events.collect { event ->
        when (event) {
            is ClientEvent.Login -> onLogin(event.info)
            is ClientEvent.MessageReceived -> onMessage(event.message)
            is ClientEvent.Disconnect -> onDisconnect(event.error)
            is ClientEvent.Error -> onError(event.error)
            else -> {}
        }
    }
}

// connect() 会挂起直到第一个已认证会话可用
client.connect()

// 现在可以安全地使用 client.sendMessage()、client.sendPacket() 等
```

### 关闭清理

```kotlin
client.close()  // 关闭连接、停止重连、清理资源
eventJob.cancel()
```

`close()` 是幂等的，多次调用安全。关闭后的客户端实例不可复用以连接。

## 消息持久化顺序（关键约定）

SDK 在收到持久消息推送时，严格按照以下顺序处理：

1. `cursorStore.saveMessage(message)` — 先保存消息正文
2. `cursorStore.saveCursor(message.cursor())` — 再保存游标
3. 如果 `ackMessages = true`，发送 `AckMessage` 给服务端
4. 发布 `ClientEvent.MessageReceived`

**原因**：`AckMessage` 只在当前连接内更新服务端的内存去重集合。重连去重的真正依据是下次登录时上报的 `LoginRequest.seen_messages`。如果先保存游标再保存正文，断线后可能出现"服务端认为你见过，但你本地没有内容"的坏状态。

同样的顺序也适用于 `sendMessage()` 的响应回包 — SDK 会在返回业务层之前完成本地持久化。

## CursorStore 生产实现

默认的 `MemoryCursorStore` **只适合测试和临时进程**。生产环境应使用数据库或 KV 存储实现：

```kotlin
class MyDatabaseCursorStore(private val db: MyDatabase) : CursorStore {
    override suspend fun loadSeenMessages(): List<MessageCursor> {
        return db.readSeenCursors()
    }

    override suspend fun saveMessage(message: Message) {
        db.insertMessage(message.nodeId, message.seq, message.body)
    }

    override suspend fun saveCursor(cursor: MessageCursor) {
        db.upsertCursor(cursor.nodeId, cursor.seq)
    }
}
```

要求：
- `loadSeenMessages()` 只返回"已可靠持久化完成"的游标。
- 跨进程重启后仍然稳定可用。
- `saveMessage()` 和 `saveCursor()` 应当是幂等的（或以游标为主键去重）。

## 实时事件流处理

### events（SharedFlow）

- 无 replay（新订阅者看不到历史事件）。
- 额外缓冲 64 个事件。
- 如果消费方长期不收集，极端情况下事件可能被丢弃。
- 建议在 `connect()` 之前就开始收集，以免错过 `Login` 事件。

### loginState（StateFlow）

- 当前已认证会话快照。
- 登录成功 → `LoginInfo`；断线/重连中 → `null`。
- 用于获取当前 `sessionRef`：`client.loginState.value?.sessionRef`。

### connectionState（StateFlow）

- 四种状态：`DISCONNECTED` → `CONNECTING` → `CONNECTED` → `CLOSED`。
- `CONNECTED` 表示收到 `LoginResponse`，而非底层 socket 已打开。
- 自动重连期间在 `DISCONNECTED → CONNECTING → CONNECTED` 之间循环。

## 自动重连行为

触发条件：
- `closed == false`
- `Config.reconnect == true`（默认为 true）
- 失败原因不是 `ServerError(code = "unauthorized")`

退避策略：
- 初始延迟：`initialReconnectDelay`（默认 1 秒）
- 每次失败翻倍
- 上限：`maxReconnectDelay`（默认 30 秒）
- 仅在"连接成功 + 登录成功"完整一次后才重置为初始值

重连流程：
1. 调用 `cursorStore.loadSeenMessages()` 获取已持久化游标
2. 将游标写入新的 `LoginRequest.seen_messages`
3. 重复发送同一份 `Credentials`
4. 如果服务端返回 `unauthorized`，`stopReconnect = true`，不再重试

## 错误处理

### 调用式接口

```kotlin
try {
    val user = client.getUser(UserRef(4096, 1025))
} catch (e: ServerError) {
    // 服务端返回的协议错误
} catch (e: ProtocolError) {
    // 协议帧非法、响应格式不对
} catch (e: ConnectionError) {
    // 网络层失败
}
```

### 流式错误

通过 `events` 持续收集：

```kotlin
client.events.collect { event ->
    when (event) {
        is ClientEvent.Error -> log.error("流错误", event.error)
        is ClientEvent.Disconnect -> log.warn("连接断开", event.error)
        else -> {}
    }
}
```

### 异常基类

所有 SDK 异常继承自 `TurntfException(RuntimeException)`。

## 最佳实践总结

### 推荐做法

- 自定义 `CursorStore`，使用持久化存储。
- 在 `connect()` 之前开始收集 `events`，避免错过早期事件。
- 依赖 `loginState` 而非 `events` 获取当前 `sessionRef`。
- 需要精确会话定向时，先 `resolveUserSessions()` 再 `sendPacket()`。
- 为 `events` 的 `collect` 和 `connect()` 分配独立的协程作用域以便独立管理。

### 避免的做法

- 不要把 `MemoryCursorStore` 用于长生命周期生产进程。
- 不要把 `AckMessage` 当作持久送达保证 — 重连去重依赖 `seen_messages`。
- 不要把 `PacketReceived` 写进持久消息游标表。
- 不要在 `realtimeStream` 连接上调用持久消息或管理类 RPC。
- 不要把 `PacketReceived` 当作业务送达确认 — 使用 `packet_id` 自行去重。
- 不要在 `realtimeStream` 连接上发送持久 `sendMessage()`。

## 相关文档

- [README.md](../README.md) — 快速入门与 API 概览
- [realtime-client.md](./realtime-client.md) — 实时客户端与 HTTP API 语义详解
- [http-client.md](./http-client.md) — HTTP 客户端使用指南
- [development.md](./development.md) — 开发、测试与 proto 同步说明
