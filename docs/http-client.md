# turntf-kt HTTP 客户端使用指南

本文档详细说明 `TurntfHttpClient` 的使用方法、所有 API 端点、认证流程和错误处理。

## 概述

`TurntfHttpClient` 是面向 turntf HTTP JSON 管理/查询接口的 `suspend` 客户端。它适合：

- 后台脚本或一次性管理任务
- 不需要维持长连接的短暂操作
- 在建立 WebSocket 连接前先获取 Bearer token
- 单独使用，无需引入完整的 `TurntfClient` 实时客户端

## 创建客户端

```kotlin
import io.github.tursom.turntf.kotlin.TurntfHttpClient

// 使用默认 OkHttpClient
val http = TurntfHttpClient("http://127.0.0.1:8080")

// 使用自定义 OkHttpClient
val customClient = OkHttpClient.Builder()
    .connectTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
    .build()
val http = TurntfHttpClient("http://127.0.0.1:8080", customClient)
```

构造参数：
- `baseUrl`: HTTP 基地址，必须是非空 URL（如 `http://127.0.0.1:8080`）。
- `client`: 可选的 OkHttpClient 实例，用于自定义传输层配置。

## 认证

### 使用明文口令登录

```kotlin
val token = http.login(nodeId = 4096, userId = 1, password = "root")
```

也可以改用登录名认证：

```kotlin
val token = http.login(loginName = "alice.login", password = "root")
```

该方法在客户端本地使用 bcrypt 哈希明文口令，然后调用 `POST /auth/login`。返回的字符串是 Bearer token，需要在后续请求中携带。
认证 selector 必须二选一：传 `(nodeId, userId)` 或传 `loginName`，不能混用。

### 使用已有哈希登录

```kotlin
import io.github.tursom.turntf.kotlin.hashedPassword

val token = http.loginWithPassword(
    nodeId = 4096,
    userId = 1,
    password = hashedPassword("$2a$10$...")
)
```

适用于调用方已经持有 bcrypt 哈希值的场景，SDK 会直接透传，不会再次哈希。

## API 方法清单

所有方法都是 `suspend` 函数，需要在协程作用域内调用。每个方法都需要 `token` 参数（除 `login` 和 `loginWithPassword` 外）。

### 用户管理

#### 创建用户

```kotlin
val user = http.createUser(
    token = token,
    request = CreateUserRequest(
        username = "alice",
        password = plainPassword("alice-password"),
        profileJson = """{"tier":"gold"}""".encodeToByteArray(),
        role = "user",
        loginName = "alice.login"
    )
)
println("created user: nodeId=${user.nodeId}, userId=${user.userId}, loginName=${user.loginName}")
```

`profileJson` 使用 `ByteArray`，HTTP 层会在边界序列化为内嵌 JSON 对象。
`loginName` 留空表示创建时不绑定登录名。

#### 创建频道

```kotlin
val channel = http.createChannel(
    token = token,
    request = CreateUserRequest(
        username = "room-general",
        password = plainPassword("channel-password"),
        role = "" // 空值会被自动补为 "channel"
    )
)
```

`createChannel()` 是 `createUser()` 的封装，当 `role` 为空时自动补 `"channel"`。

#### 列出当前用户可通讯的活跃用户

```kotlin
val users = http.listUsers(
    token = token,
    filter = UserListFilter(
        name = "carol",
        uid = UserRef(4096, 1027)
    )
)
for (user in users) {
    println("userId=${user.userId}, username=${user.username}, loginName=${user.loginName}")
}
```

`name` 会在服务端做大小写不敏感子串匹配，匹配范围仅限“当前调用者可通讯的用户集合”。

SDK 对外统一使用 `UserRef` 表达 `uid`：

- HTTP 查询参数里会自动编码成 `node_id:user_id`
- `uid = null` 或 `UserRef(0, 0)` 表示“不按 uid 过滤”
- 半空 `uid`（如只有 `nodeId` 没有 `userId`）会在本地抛出 `IllegalArgumentException`

普通用户查看其他联系人时，服务端可能会隐藏 `login_name`，因此返回的 `User.loginName` 允许为空字符串。

### 用户元数据

用户私有元数据是 turntf 的键值对存储系统，每项由 `(owner, key)` 唯一标识。

#### 读取元数据

```kotlin
val metadata = http.getUserMetadata(
    token = token,
    owner = UserRef(4096, 1025),
    key = "settings.theme"
)
println("value=${metadata.value}") // ByteArray
```

#### 写入/更新元数据

```kotlin
val upserted = http.upsertUserMetadata(
    token = token,
    owner = UserRef(4096, 1025),
    key = "settings.theme",
    value = byteArrayOf(0x01, 0x02),
    expiresAt = "2026-12-31T23:59:59Z" // 可选，RFC3339 格式
)
```

`value` 使用 `ByteArray`，HTTP 层会以 base64 编码传输。

#### 删除元数据

```kotlin
val deleted = http.deleteUserMetadata(
    token = token,
    owner = UserRef(4096, 1025),
    key = "settings.theme"
)
```

删除操作返回 tombstoned 记录，包含 `deletedAt` 字段。

#### 扫描元数据

```kotlin
val scan = http.scanUserMetadata(
    token = token,
    owner = UserRef(4096, 1025),
    prefix = "settings.", // 可选前缀过滤
    after = "settings.theme", // 可选游标续扫
    limit = 20 // 可选数量限制
)
for (item in scan.items) {
    println("${item.key} = ${item.value}")
}
println("next after: ${scan.nextAfter}") // 传给下一次调用的 after 参数
```

扫描结果使用 `nextAfter` 游标支持分页。HTTP 传输层会把响应数组或 `items` 节点统一为 `List<UserMetadata>`。

### 消息收发

#### 列出持久消息

```kotlin
val messages = http.listMessages(
    token = token,
    target = UserRef(4096, 1025),
    limit = 50
)
for (msg in messages) {
    println("seq=${msg.seq}, body=${msg.body}")
}
```

#### 发送持久消息

```kotlin
val message = http.postMessage(
    token = token,
    target = UserRef(4096, 1025),
    body = "hello".encodeToByteArray()
)
println("sent message seq=${message.seq}")
```

#### 发送瞬时包

```kotlin
http.postPacket(
    token = token,
    targetNodeId = 4096,
    relayTarget = UserRef(4096, 1025),
    body = "transient payload".encodeToByteArray(),
    mode = DeliveryMode.ROUTE_RETRY
)
```

约束：
- `targetNodeId` 必须等于 `relayTarget.nodeId`。
- `mode` 只能是 `BEST_EFFORT` 或 `ROUTE_RETRY`。
- 接口返回 `202 Accepted`，仅表示瞬时包已进入路由层，不代表目标用户已收到。

### 关联管理

子系统和黑名单都通过统一的附件（Attachment）机制实现。

#### 创建订阅

```kotlin
http.createSubscription(
    token = token,
    user = UserRef(4096, 1025),
    channel = UserRef(4096, 2048)
)
```

本质是写入 `CHANNEL_SUBSCRIPTION` 类型的附件。

#### 黑名单操作

```kotlin
// 拉黑用户
val entry = http.blockUser(
    token = token,
    owner = UserRef(4096, 1025),
    blocked = UserRef(4096, 9999)
)

// 取消拉黑
http.unblockUser(
    token = token,
    owner = UserRef(4096, 1025),
    blocked = UserRef(4096, 9999)
)

// 查看黑名单列表
val blocked = http.listBlockedUsers(
    token = token,
    owner = UserRef(4096, 1025)
)
```

#### 通用附件操作

```kotlin
// 创建附件
val attachment = http.upsertAttachment(
    token = token,
    owner = UserRef(4096, 1025),
    subject = UserRef(4096, 2048),
    attachmentType = AttachmentType.CHANNEL_MANAGER,
    configJson = """{"permission":"admin"}""".encodeToByteArray()
)

// 删除附件
val deleted = http.deleteAttachment(
    token = token,
    owner = UserRef(4096, 1025),
    subject = UserRef(4096, 2048),
    attachmentType = AttachmentType.CHANNEL_MANAGER
)

// 列出附件（可按类型过滤）
val attachments = http.listAttachments(
    token = token,
    owner = UserRef(4096, 1025),
    attachmentType = AttachmentType.CHANNEL_SUBSCRIPTION
)
```

### 集群查询

#### 列出集群节点

```kotlin
val nodes = http.listClusterNodes(token)
for (node in nodes) {
    println("nodeId=${node.nodeId}, isLocal=${node.isLocal}, url=${node.configuredUrl}")
}
```

响应可能是 JSON 数组或 `{"nodes": [...]}` 包裹格式，SDK 容错处理。

#### 查询已登录用户

```kotlin
val users = http.listNodeLoggedInUsers(
    token = token,
    nodeId = 4096
)
for (user in users) {
    println("userId=${user.userId}, username=${user.username}, loginName=${user.loginName}")
}
```

## 传输模型说明

### ByteArray 策略

HTTP REST 接口中，以下字段以 base64 或内嵌 JSON 传输，但 SDK 统一暴露为 `ByteArray`：

| 模型字段 | HTTP 表示 | SDK 类型 |
|---------|----------|---------|
| `body` | base64 | `ByteArray` |
| `profileJson` | 内嵌 JSON 对象 | `ByteArray` |
| `configJson` | 内嵌 JSON 对象 | `ByteArray` |
| `eventJson` | 内嵌 JSON 对象 | `ByteArray` |
| `value` (UserMetadata) | base64 | `ByteArray` |

这使得 HTTP 与 WebSocket 两种传输路径在模型层完全一致。

### 用户元数据中的时间字段

`expiresAt` 使用 RFC3339 格式字符串（如 `"2026-05-01T00:00:00Z"`），与 HTTP API 和 WebSocket API 保持一致。

## 错误处理

### HTTP 非预期状态码

当服务端返回的状态码不在调用方期望的集合中时，抛出 `ProtocolError`：

```kotlin
try {
    val user = http.createUser(token, request)
} catch (e: ProtocolError) {
    println("HTTP 错误: ${e.message}")
}
```

### 网络层错误

传输故障包装为 `ConnectionError`：

```kotlin
try {
    val nodes = http.listClusterNodes(token)
} catch (e: ConnectionError) {
    println("网络错误: ${e.message}")
}
```

### HTTP 登录错误

登录失败（非 200 状态码）会抛出 `ProtocolError`，需调用方自行处理：

```kotlin
try {
    val token = http.login(4096, 1, "wrong-password")
} catch (e: ProtocolError) {
    // 处理登录失败（密码错误、用户不存在等）
}
```

注意：HTTP 登录返回的是 Bearer token，与 WebSocket 登录返回 `LoginInfo` 不同。

## 与 TurntfClient 配合使用

`TurntfClient` 内置了 `http` 属性，共享同一份 `OkHttpClient`：

```kotlin
val client = TurntfClient(config)

// 使用内置 HTTP 客户端获取 token
val token = client.http.login(4096, 1, "root")
println("bearer token: $token")

// HTTP 和 WebSocket 使用相同的网络配置
// 在 WebSocket 连接上执行 RPC
client.connect()
val user = client.getUser(UserRef(4096, 1025))
```

这种方式避免了重复创建 OkHttpClient 和配置 baseUrl。

## 完整使用示例

```kotlin
import io.github.tursom.turntf.kotlin.*
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    // 1. 创建 HTTP 客户端
    val http = TurntfHttpClient("http://127.0.0.1:8080")

    // 2. 登录获取 token
    val token = http.login(nodeId = 4096, userId = 1, password = "root")

    // 3. 创建新用户
    val newUser = http.createUser(token, CreateUserRequest(
        username = "bob",
        password = plainPassword("bob-password"),
        role = "user"
    ))

    // 4. 发送持久消息
    val msg = http.postMessage(
        token = token,
        target = UserRef(newUser.nodeId, newUser.userId),
        body = "welcome!".encodeToByteArray()
    )
    println("message seq=${msg.seq}")

    // 5. 查询历史消息
    val messages = http.listMessages(
        token = token,
        target = UserRef(newUser.nodeId, newUser.userId)
    )
    println("total messages: ${messages.size}")

    // 6. 管理用户元数据
    http.upsertUserMetadata(
        token = token,
        owner = UserRef(newUser.nodeId, newUser.userId),
        key = "prefs.lang",
        value = "zh-CN".encodeToByteArray()
    )

    // 7. 查询集群
    val nodes = http.listClusterNodes(token)
    println("cluster nodes: ${nodes.size}")
}
```
