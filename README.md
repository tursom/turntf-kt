# turntf-kt

## 概述

`turntf-kt` 是 turntf 的 Kotlin/JVM SDK，面向需要协程友好 API、长连接实时收发和可靠重连语义的 JVM 客户端。

SDK 提供两套互补的客户端：

- **TurntfHttpClient** -- 基于 OkHttp + kotlinx-coroutines 的 HTTP `suspend` API，用于管理类和查询类 REST 端点。所有方法均以挂起函数形式暴露，天然适配协程作用域。
- **TurntfClient** -- 基于 WebSocket + Protobuf 的实时长连接客户端，内置登录认证、自动重连（指数退避）、请求-响应关联、消息持久化去重和游标管理。通过 `SharedFlow` 暴露事件流，通过 `StateFlow` 暴露连接与登录状态。

SDK 公开模型的传输无关性设计：

- `body`、`profileJson`、`configJson`、`eventJson` 等字段统一暴露为 `ByteArray`，无论底层使用 HTTP JSON（可能经 base64 编码）还是 Protobuf 原始字节。
- 登录口令通过 `PasswordInput` 包装，通过 `plainPassword()` 和 `hashedPassword()` 分别支持"本地明文 bcrypt 后发送"和"直接复用已有哈希值"两种模式。
- 持久消息严格遵循 `saveMessage -> saveCursor -> ack` 顺序，避免断线重连后将尚未落盘的消息错误地声明为"已见"。

## 环境要求

- JDK 21
- Kotlin 2.1.10+
- Gradle（构建工具）
- Protobuf 编译器通过 Gradle 插件自动拉取，生成 `lite` Java 类供 Kotlin 使用

核心依赖：

| 依赖 | 用途 |
|------|------|
| `okhttp` | HTTP 与 WebSocket 传输 |
| `kotlinx-coroutines-core` | 协程与 Flow |
| `jbcrypt` | 本地密码 bcrypt 哈希 |
| `protobuf-javalite` | 客户端 Protobuf 模型 |
| `jackson-databind` / `jackson-module-kotlin` | HTTP JSON 编解码 |

## 安装与集成

### 在 monorepo 内联调

直接进入模块目录执行 Gradle 任务：

```bash
cd turntf-kt
gradle test
gradle build
```

### 发布到本地 Maven

若业务工程不位于本 monorepo 中，先发布到本地 Maven，再在应用侧引入：

```bash
cd turntf-kt
gradle publishToMavenLocal
```

```kotlin
dependencies {
    implementation("io.github.tursom:turntf-kt:0.1.0")
}
```

若采用源码联调，也可将 `turntf-kt` 作为 Gradle 复合构建或子模块引入。

## 快速开始

### TurntfHttpClient -- HTTP 管理接口

以下示例展示使用 `TurntfHttpClient` 完成登录和消息查询的典型流程。所有方法均为 `suspend` 函数，可直接在协程中调用。

```kotlin
import io.github.tursom.turntf.kotlin.TurntfHttpClient
import io.github.tursom.turntf.kotlin.UserRef
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    // 创建 HTTP 客户端，指定服务端基础 URL
    val http = TurntfHttpClient("http://127.0.0.1:8080")

    // 登录：方式一 -- 节点 ID + 用户 ID + 明文密码
    // SDK 会在发送前自动对明文密码进行 bcrypt 哈希
    val token = http.login(nodeId = 4096, userId = 1, password = "root")

    // 登录：方式二 -- 使用登录名
    // val token = http.login(loginName = "alice.login", password = "root")

    // 登录后，所有管理接口需要传入 Bearer token
    val messages = http.listMessages(
        token = token,
        target = UserRef(4096, 1025),
        limit = 20
    )
    println("共查询到 ${messages.size} 条消息")

    // 查看集群节点
    val nodes = http.listClusterNodes(token)
    nodes.forEach { node ->
        println("节点 ${node.nodeId}: isLocal=${node.isLocal}, url=${node.configuredUrl}")
    }
}
```

如果已持有 bcrypt 哈希值，可以改用 `loginWithPassword()`：

```kotlin
import io.github.tursom.turntf.kotlin.hashedPassword

val token = http.loginWithPassword(
    nodeId = 4096,
    userId = 1,
    password = hashedPassword("$2a$10$...")
)
```

### TurntfClient -- WebSocket 实时长连接

以下示例展示完整的 `TurntfClient` 生命周期：连接、事件收集、消息收发和优雅关闭。

```kotlin
import io.github.tursom.turntf.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

fun main() = runBlocking {
    // 配置客户端：指定服务端地址和凭据
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

    // 启动事件收集协程
    // events 为 SharedFlow，支持多个收集者并发消费
    val eventJob = launch {
        client.events.collect { event ->
            when (event) {
                is ClientEvent.Login -> {
                    println("登录成功，会话: ${event.info.sessionRef}")
                    println("协议版本: ${event.info.protocolVersion}")
                }
                is ClientEvent.MessageReceived -> {
                    val msg = event.message
                    println("收到持久消息: seq=${msg.seq}, body=${msg.body.decodeToString()}")
                }
                is ClientEvent.PacketReceived -> {
                    println("收到瞬时包: id=${event.packet.packetId}")
                }
                is ClientEvent.Error -> {
                    println("流错误: ${event.error.message}")
                }
                is ClientEvent.Disconnect -> {
                    println("连接断开: ${event.error.message}")
                }
            }
        }
    }

    // 连接 WebSocket 并等待首次登录认证完成
    // connect() 挂起直到第一个经过认证的会话就绪
    client.connect()

    // 通过 connectionState StateFlow 观察连接状态变化
    launch {
        client.connectionState.collect { state ->
            println("连接状态变化: $state")
        }
    }

    // 发送持久消息
    val message = client.sendMessage(
        SendMessageInput(
            target = UserRef(4096, 1025),
            body = "Hello, turntf!".encodeToByteArray()
        )
    )
    println("消息已发送，seq=${message.seq}")

    // 发送瞬时数据包（最佳投递模式）
    client.sendPacket(
        target = UserRef(4096, 1025),
        body = "ping".encodeToByteArray(),
        deliveryMode = DeliveryMode.BEST_EFFORT
    )

    // 查询用户信息
    val user = client.getUser(UserRef(4096, 1025))
    println("用户: ${user.username}, 角色: ${user.role}")

    // 查询运维状态
    val status = client.operationsStatus()
    println("消息窗口大小: ${status.messageWindowSize}")
    println("写门控就绪: ${status.writeGateReady}")

    // 优雅关闭：停止重连、关闭 WebSocket、取消待处理 RPC
    client.close()
    eventJob.cancel()
}
```

使用登录名认证时，改变 `Credentials` 的构建方式即可：

```kotlin
val client = TurntfClient(
    Config(
        baseUrl = "http://127.0.0.1:8080",
        credentials = Credentials(
            loginName = "alice.login",
            password = plainPassword("alice-password")
        )
    )
)
```

## API 概览

### TurntfHttpClient

所有方法均为 `suspend` 函数，构造函数接受 `baseUrl` 和可选的 `OkHttpClient` 实例。

| 分类 | 方法 | 说明 |
|------|------|------|
| 鉴权 | `login(nodeId, userId, password)` | 传统方式登录，明文密码自动 bcrypt |
| 鉴权 | `login(loginName, password)` | 登录名方式登录 |
| 鉴权 | `loginWithPassword(nodeId, userId, password: PasswordInput)` | 使用已包装的密码登录 |
| 鉴权 | `loginWithPassword(loginName, password: PasswordInput)` | 登录名 + 已包装密码 |
| 用户管理 | `createUser(token, request)` / `createChannel(token, request)` | 创建用户 / 频道 |
| 用户管理 | `listUsers(token, filter?)` | 列出当前调用者可通讯的活跃用户，支持 `name` / `uid` 过滤 |
| 元数据 | `getUserMetadata(token, owner, key)` | 读取单条元数据 |
| 元数据 | `upsertUserMetadata(token, owner, key, value, expiresAt?)` | 创建或替换元数据 |
| 元数据 | `deleteUserMetadata(token, owner, key)` | 删除元数据 |
| 元数据 | `scanUserMetadata(token, owner, prefix?, after?, limit?)` | 游标扫描元数据 |
| 消息 | `listMessages(token, target, limit?)` | 查询持久消息列表 |
| 消息 | `postMessage(token, target, body)` | 发送持久消息 |
| 消息 | `postPacket(token, targetNodeId, relayTarget, body, mode)` | 发送瞬时数据包 |
| 附件 | `upsertAttachment(token, owner, subject, type, configJson)` | 创建/替换附件 |
| 附件 | `deleteAttachment(token, owner, subject, type)` | 删除附件 |
| 附件 | `listAttachments(token, owner, type?)` | 列出附件 |
| 订阅 | `createSubscription(token, user, channel)` | 订阅频道 |
| 黑名单 | `blockUser(token, owner, blocked)` / `unblockUser(token, owner, blocked)` | 拉黑 / 解除拉黑 |
| 黑名单 | `listBlockedUsers(token, owner)` | 列出黑名单 |
| 集群 | `listClusterNodes(token)` | 列出集群节点 |
| 集群 | `listNodeLoggedInUsers(token, nodeId)` | 列出节点的已登录用户 |

### TurntfClient

`TurntfClient` 在 WebSocket 长连接上完成以下工作：

- 登录认证与重登录（断线时自动使用凭据重新登录）
- `request_id` 关联下的请求/响应 RPC
- `MessagePushed` 推送和 `sendMessageResponse` 回包的本地位移持久化
- `CursorStore` 管理、`seen_messages` 重放和自动 `AckMessage`
- 通过 `events` / `loginState` / `connectionState` 三套 Flow 暴露运行时状态
- 自动 Ping 保活、自动重连（指数退避）、登录失败停重试判定

**事件流 (`events: SharedFlow<ClientEvent>`)**

| 事件类型 | 说明 |
|----------|------|
| `ClientEvent.Login(info)` | 登录成功，包含用户信息和会话引用 |
| `ClientEvent.MessageReceived(message)` | 收到持久消息（已自动持久化并 ACK） |
| `ClientEvent.PacketReceived(packet)` | 收到瞬时数据包（不参与游标管理） |
| `ClientEvent.Error(error)` | SDK 内部错误 |
| `ClientEvent.Disconnect(error)` | 连接断开（自动重连前触发） |

**状态流 (`connectionState: StateFlow<ConnectionState>`)**

`DISCONNECTED -> CONNECTING -> CONNECTED -> CLOSED`，自动重连过程中会循环回到 `CONNECTING`。

**公开方法一览**

所有与服务器交互的方法均为 `suspend` 函数。

| 分类 | 方法 | 说明 |
|------|------|------|
| 生命周期 | `connect()` | 启动连接，挂起直到首次认证完成 |
| 生命周期 | `close()` | 停止重连、关闭 WebSocket、取消待处理 RPC |
| 鉴权 | `login(nodeId, userId, password)` / `login(loginName, password)` | HTTP 登录，委托给内置 `http` 客户端 |
| 鉴权 | `loginWithPassword(...)` | 使用已包装密码登录 |
| 保活 | `ping()` | 发送应用层心跳 |
| 消息 | `sendMessage(input: SendMessageInput): Message` | 发送持久消息（挂起直到本地持久化完成） |
| 消息 | `sendPacket(input: SendPacketInput): RelayAccepted` | 发送瞬时数据包 |
| 消息 | `sendPacket(target, body, deliveryMode, targetSession?)` | 发送瞬时数据包的便捷重载 |
| 用户 | `createUser(request)` / `createChannel(request)` | 创建用户/频道 |
| 用户 | `listUsers(filter?)` | 列出当前登录用户可通讯的活跃用户，支持 `name` / `uid` 过滤 |
| 用户 | `getUser(target)` | 获取用户信息 |
| 用户 | `updateUser(target, request)` | 部分更新用户信息 |
| 用户 | `deleteUser(target)` | 删除用户 |
| 元数据 | `getUserMetadata(owner, key)` / `upsertUserMetadata(...)` / `deleteUserMetadata(owner, key)` | CRUD 操作 |
| 元数据 | `scanUserMetadata(owner, prefix?, after?, limit?)` | 游标扫描 |
| 附件 | `upsertAttachment(...)` / `deleteAttachment(...)` / `listAttachments(owner, type?)` | 附件管理 |
| 频道 | `subscribeChannel(subscriber, channel)` | 订阅频道 |
| 频道 | `unsubscribeChannel(subscriber, channel)` | 取消订阅 |
| 频道 | `listSubscriptions(subscriber)` | 列出已订阅频道 |
| 黑名单 | `blockUser(owner, blocked)` / `unblockUser(owner, blocked)` / `listBlockedUsers(owner)` | 黑名单管理 |
| 消息 | `listMessages(target, limit)` | 列出持久消息 |
| 事件 | `listEvents(after, limit)` | 按序列号列出事件 |
| 集群 | `listClusterNodes()` | 列出集群节点 |
| 集群 | `listNodeLoggedInUsers(nodeId)` | 列出节点已登录用户 |
| 会话 | `resolveUserSessions(user)` | 解析用户在线会话 |
| 运维 | `operationsStatus()` | 查询节点运维状态 |
| 运维 | `metrics()` | 获取节点指标文本 |

`TurntfClient` 内置 `val http: TurntfHttpClient`，可在同一份 `baseUrl` / `OkHttpClient` 配置上直接复用 HTTP 接口：

```kotlin
val client = TurntfClient(config)
val token = client.login(4096, 1, "root")
val node = client.http.listClusterNodes(token)
```

`listUsers()` 的过滤条件通过 `UserListFilter` 表达。SDK 对外统一使用 `UserRef` 作为 `uid`：
- HTTP 会编码成 `node_id:user_id`
- WebSocket/proto 会编码成 `UserRef`
- `uid = UserRef(0, 0)` 或 `null` 表示“不按 uid 过滤”

普通用户通过用户列表看到他人时，服务端可能会隐藏 `login_name`，因此 `User.loginName` 允许为空字符串。

## 选型建议

| 场景 | 推荐客户端 |
|------|------------|
| 后台脚本、一次性管理任务 | `TurntfHttpClient` |
| 需要长期在线、自动重连和实时推送 | `TurntfClient` |
| 仅查询集群状态和历史消息 | `TurntfHttpClient` |
| 需要消费实时消息流和连接状态变化 | `TurntfClient`（通过 `SharedFlow`/`StateFlow`） |
| 需要 `session_ref` 按会话发送瞬时包 | `TurntfClient` |
| 纯实时互动（不关心持久消息） | `TurntfClient` + `realtimeStream = true` |

### 核心语义提醒

在使用或扩展 SDK 时，以下几点最为重要：

- `AckMessage` 仅是连接内的通知；真正决定重连去重的是下次登录时上报的 `LoginRequest.seen_messages`。
- SDK 收到持久消息后始终按 `saveMessage() -> saveCursor() -> ack` 顺序执行，确保断线后不会将未落盘的消息标记为已见。
- `sendMessage()` 的回包也会推进本地游标，避免"自己刚发出的消息"在重连后又被重复消费。
- `PacketReceived` 不参与游标、历史补发和 ack 流程；若业务需要暂存瞬时包，应自行按 `packet_id` 去重。
- `session_ref` 来自登录成功后的 `LoginResponse`，仅用于瞬时数据包的会话定向发送。
- `realtimeStream = true` 时连接的是 `/ws/realtime`，能力面比默认的 `/ws/client` 更窄，仅适合以瞬时互动为主的场景。

## 文档导航

- [SDK 总体使用指南](./docs/sdk-guide.md) -- 安装、配置选择、生命周期管理、最佳实践
- [实时客户端与 WebSocket API 说明](./docs/realtime-client.md) -- 实时客户端语义详解
- [HTTP 客户端使用指南](./docs/http-client.md) -- HTTP 客户端各 API 详细用法
- [开发、测试与 Proto 同步说明](./docs/development.md) -- 测试覆盖、Proto 同步、共享语义检查清单

## 构建与测试

```bash
# 运行测试
gradle test

# 完整构建
gradle build

# 发布到本地 Maven
gradle publishToMavenLocal
```

测试覆盖包括：序列化/反序列化测试、URL 生成逻辑测试、请求校验边界测试。详细的测试说明请参阅 [开发文档](./docs/development.md)。
