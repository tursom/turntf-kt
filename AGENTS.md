# turntf-kt SDK 开发指南

## 项目概览

`turntf-kt` 是 turntf 分布式通知服务的 Kotlin/JVM SDK，位于 monorepo 的 `sdk/turntf-kt/` 目录下，作为独立 Git 仓库通过 submodule 挂载到根仓库。

### 技术栈

- **语言**: Kotlin 2.1.10
- **目标平台**: JVM 21 (jvmToolchain(21))
- **构建工具**: Gradle（Kotlin JVM Plugin + java-library + maven-publish）
- **异步模型**: `kotlinx-coroutines-core 1.10.1` + `Flow`（SharedFlow / StateFlow）
- **HTTP 传输**: OkHttp 4.12.0
- **WebSocket 传输**: OkHttp（WebSocketListener 封装）
- **序列化**: Protobuf (protobuf-javalite 4.29.3, lite 模式)
- **JSON 编解码**: Jackson (jackson-databind + jackson-module-kotlin)
- **密码哈希**: jbcrypt 0.4
- **测试**: kotlin.test + kotlinx-coroutines-test + MockWebServer

### 架构定位

SDK 提供两套互补的客户端：

1. **`TurntfHttpClient`** — 面向 HTTP JSON 管理/查询接口的 `suspend` API。适合后台脚本、一次性管理任务或不需要长连接实时推送的场景。

2. **`TurntfClient`** — 面向 WebSocket + Protobuf 长连接的协程/Flow API。负责登录、收发消息、自动重连、请求-响应关联（request_id）和消息持久化顺序管理。适合需要长期在线、自动重连和实时推送的 JVM 服务或桌面客户端。

所有公开模型字段（body、profileJson、configJson、eventJson 等）统一暴露为 `ByteArray`，保持传输无关性。

### 组 ID 与版本

- **group**: `io.github.tursom`
- **artifact**: `turntf-kt`
- **version**: `0.1.0`

## 构建与测试命令

### 常用 Gradle 任务

```bash
# 运行全部测试（自动触发编译和 Proto 生成）
gradle test

# 完整构建
gradle build

# 仅生成 Protobuf Java 代码
gradle generateProto

# 发布到本地 Maven 仓库
gradle publishToMavenLocal

# 清理构建产物
gradle clean
```

### 测试说明

- 测试框架使用 `kotlin.test` + JUnit Platform。
- 协程测试通过 `runTest` 运行。
- HTTP 层测试使用 OkHttp 的 `MockWebServer` 模拟服务端行为。
- WebSocket 层测试通过 `MockResponse.withWebSocketUpgrade()` 创建 mock WebSocket 后端。

测试覆盖以下语义：

- **TurntfHttpClientTest**: HTTP 登录 bcrypt、用户创建、用户元数据 CRUD、消息收发、JSON 编解码
- **TurntfClientTest**: WebSocket 登录、loginState/connectionState 更新、自动 ack、sendMessage 回包映射、ping/pong 关联、close 状态清理、用户元数据 RPC

核心区域（重连退避、CursorStore 自定义实现、realtimeStream 裁剪、会话定向 packet）当前依赖源码约束而非单测覆盖，修改涉及这些区域时建议补充测试。

### 环境要求

- JDK 21
- Gradle（推荐 8.x；通过 Gradle Wrapper 拉取）
- 无需手动安装 Protobuf 编译器，通过 `com.google.protobuf` Gradle 插件自动管理

## Proto 生成说明

### 本地 Proto 定义

协议定义位于 `proto/client.proto`，属于 `package notifier.client.v1`。

### 生成方式

通过 `com.google.protobuf` Gradle 插件（版本 0.9.5）集成：

```groovy
protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.29.3"
    }
    generateProtoTasks {
        all().configureEach {
            builtins {
                named("java") {
                    option("lite")
                }
            }
        }
    }
}
```

关键配置：
- 使用 **protobuf-javalite** 模式（lite），减小生成的代码体积。
- Kotlin 代码通过 `notifier.client.v1.Client` 包路径访问生成类。
- 生成文件位于 `build/generated/` 目录下，**不应手工编辑**。

### 同步流程

1. 修改 `proto/client.proto`。
2. 如果变更属于共享客户端协议，同步更新：
   - `turntf/proto/client.proto`（Go 服务端主参考实现）
   - `turntf-java/proto/client.proto`（Java SDK）
3. 运行 `gradle generateProto`。
4. 运行 `gradle test`。
5. 检查 Kotlin 源码中对 `Client.*` 生成类的调用是否需要同步调整。
6. 如果协议语义变化影响对外使用方式，同步更新 README 和 docs/ 文档。

## 包结构

```
src/
├── main/
│   └── kotlin/
│       └── io/github/tursom/turntf/kotlin/
│           ├── Models.kt              # 公共模型、Config、Credentials、CursorStore、异常体系、事件类型
│           ├── TurntfHttpClient.kt    # HTTP JSON 客户端（suspend API）
│           ├── TurntfClient.kt        # 实时客户端（WebSocket + Protobuf，协程/Flow API）
│           └── internal/
│               └── Internal.kt        # HTTP/Proto 映射函数、地址校验、枚举转换、工具方法
└── test/
    └── kotlin/
        └── io/github/tursom/turntf/kotlin/
            ├── TurntfHttpClientTest.kt  # HTTP 层测试
            └── TurntfClientTest.kt      # 实时客户端测试

proto/
└── client.proto              # 客户端协议定义（protobuf3，lite 模式生成）

build.gradle.kts              # 构建配置
settings.gradle.kts           # 模块名称（turntf-kt）
docs/
├── realtime-client.md        # 实时客户端与 HTTP API 说明
├── development.md            # 开发、测试与 proto 同步说明
├── sdk-guide.md              # SDK 总体使用指南
└── http-client.md            # HTTP 客户端使用指南
```

## 关键 API 表面

### Models（Models.kt）

所有公开数据类均为 `data class`，不可变设计。

#### 认证相关

| 类型 | 说明 |
|------|------|
| `PasswordInput` | 口令包装，区分明文（`plainPassword()`）和已有哈希（`hashedPassword()`）两种来源 |
| `Credentials` | 登录身份（nodeId + userId + password） |
| `LoginInfo` | 登录成功信息（user + protocolVersion + sessionRef） |

#### 配置

| 类型 | 说明 |
|------|------|
| `Config` | TurntfClient 运行时配置（20 个字段），包括 baseUrl、credentials、cursorStore、reconnect、pingInterval 等 |

#### 传输模型

| 类型 | 说明 |
|------|------|
| `UserRef` | 用户引用 (nodeId, userId) |
| `SessionRef` | 会话引用 (servingNodeId, sessionId)，isZero() 判断空值 |
| `User` | 用户信息 |
| `Message` | 持久消息 (recipient, nodeId, seq, sender, body, createdAtHlc)，提供 `cursor()` 方法 |
| `MessageCursor` | 消息游标 (nodeId, seq) |
| `Packet` | 瞬时包 (packetId, sourceNodeId, targetNodeId, recipient, sender, body, deliveryMode, targetSession) |
| `RelayAccepted` | 瞬时包路由接受确认 |
| `DeliveryMode` | 投递模式枚举 (UNSPECIFIED, BEST_EFFORT, ROUTE_RETRY) |

#### 管理模型

| 类型 | 说明 |
|------|------|
| `UserMetadata` | 用户元数据 (owner, key, value: ByteArray, expiresAt) |
| `UserMetadataScanResult` | 元数据扫描结果（含 nextAfter 游标） |
| `Attachment` | 用户间关联关系（订阅、黑名单等） |
| `AttachmentType` | 关联类型枚举 (CHANNEL_MANAGER, CHANNEL_WRITER, CHANNEL_SUBSCRIPTION, USER_BLACKLIST) |
| `Subscription` | 频道订阅 |
| `BlacklistEntry` | 黑名单条目 |
| `Event` | 系统事件 |
| `ClusterNode` | 集群节点信息 |
| `LoggedInUser` | 已登录用户信息 |
| `ResolvedUserSessions` | 用户在线会话解析结果（含 OnlineNodePresence 和 ResolvedSession） |
| `OperationsStatus` | 集群运维状态（含 PeerStatus、PeerOriginStatus 等嵌套结构） |
| `DeleteUserResult` | 删除用户结果 |

#### 请求输入

| 类型 | 说明 |
|------|------|
| `SendMessageInput` | 发送持久消息输入 (target, body) |
| `SendPacketInput` | 发送瞬时包输入 (target, body, deliveryMode, targetSession?) |
| `CreateUserRequest` | 创建用户请求 (username, password?, profileJson, role) |
| `UpdateUserRequest` | 更新用户请求（所有字段可选） |

#### CursorStore 接口

```kotlin
interface CursorStore {
    suspend fun loadSeenMessages(): List<MessageCursor>
    suspend fun saveMessage(message: Message)
    suspend fun saveCursor(cursor: MessageCursor)
}
```

- `MemoryCursorStore`: 默认实现（仅适合测试/Demo/短生命周期进程）
- 生产环境应替换为数据库或 KV 存储实现

#### 事件体系

```kotlin
sealed interface ClientEvent {
    data class Login(val info: LoginInfo) : ClientEvent
    data class MessageReceived(val message: Message) : ClientEvent
    data class PacketReceived(val packet: Packet) : ClientEvent
    data class Error(val error: Throwable) : ClientEvent
    data class Disconnect(val error: Throwable) : ClientEvent
}

enum class ConnectionState { DISCONNECTED, CONNECTING, CONNECTED, CLOSED }
```

#### 异常体系

| 类型 | 说明 |
|------|------|
| `TurntfException` | 基础异常（继承 RuntimeException） |
| `ServerError` | 服务端返回错误（含 code、message、requestId），提供 `unauthorized()` 判断 |
| `ProtocolError` | 协议层错误（非法帧、缺字段、HTTP 状态码异常等） |
| `ConnectionError` | 网络/拨号层失败 |

### TurntfHttpClient

位于 `io.github.tursom.turntf.kotlin` 包，构造接收 baseUrl 和可选的 OkHttpClient。

所有公开方法都是 `suspend` 函数，在 `Dispatchers.IO` 上执行。

认证方法：
- `login(nodeId, userId, password: String): String` — 本地 bcrypt 后调用 POST /auth/login，返回 Bearer token
- `loginWithPassword(nodeId, userId, password: PasswordInput): String`

用户管理：
- `createUser(token, request): User`
- `createChannel(token, request): User` — 同 createUser，role 默认补 channel

用户元数据 CRUD：
- `getUserMetadata(token, owner, key): UserMetadata`
- `upsertUserMetadata(token, owner, key, value, expiresAt?): UserMetadata`
- `deleteUserMetadata(token, owner, key): UserMetadata`
- `scanUserMetadata(token, owner, prefix?, after?, limit): UserMetadataScanResult`

消息收发：
- `listMessages(token, target, limit): List<Message>`
- `postMessage(token, target, body): Message`
- `postPacket(token, targetNodeId, relayTarget, body, mode)` — 仅瞬时包，返回 202

关联管理：
- `upsertAttachment(token, owner, subject, attachmentType, configJson): Attachment`
- `deleteAttachment(token, owner, subject, attachmentType): Attachment`
- `listAttachments(token, owner, attachmentType?): List<Attachment>`
- `createSubscription(token, user, channel)`
- `blockUser(token, owner, blocked): BlacklistEntry`
- `unblockUser(token, owner, blocked): BlacklistEntry`
- `listBlockedUsers(token, owner): List<BlacklistEntry>`

集群查询：
- `listClusterNodes(token): List<ClusterNode>`
- `listNodeLoggedInUsers(token, nodeId): List<LoggedInUser>`

### TurntfClient

构造接收 `Config`，是协程优先的实时客户端。

#### 生命周期

| 方法 | 说明 |
|------|------|
| `connect()` | 启动 WebSocket 生命周期，挂起直到第一个已认证会话可用 |
| `close()` | 停止重连、关闭 WebSocket、取消 ping、使所有挂起 RPC 异常结束 |

#### Flow 暴露

| 属性 | 类型 | 说明 |
|------|------|------|
| `events` | `SharedFlow<ClientEvent>` | 热流，无 replay，额外缓冲 64 |
| `loginState` | `StateFlow<LoginInfo?>` | 当前已认证会话快照，断线/重连中回到 null |
| `connectionState` | `StateFlow<ConnectionState>` | 连接生命周期快照 |

#### HTTP 委托

| 属性/方法 | 说明 |
|-----------|------|
| `http: TurntfHttpClient` | 内置 HTTP 客户端，复用同一份 OkHttpClient |
| `login(nodeId, userId, password)` | 委托给 http.login |
| `loginWithPassword(...)` | 委托给 http.loginWithPassword |

#### WebSocket RPC

以下方法通过 `request_id` 关联，在已认证连接上发送 `ClientEnvelope`，挂起等待 `ServerEnvelope` 响应：

消息/包收发：
- `sendMessage(input: SendMessageInput): Message` — 持久消息（响应后本地持久化并推进游标）
- `sendPacket(input: SendPacketInput): RelayAccepted` — 瞬时包

用户管理：
- `createUser(request: CreateUserRequest): User`
- `createChannel(request: CreateUserRequest): User`
- `getUser(target: UserRef): User`
- `updateUser(target, request: UpdateUserRequest): User`
- `deleteUser(target: UserRef): DeleteUserResult`

用户元数据 CRUD：
- `getUserMetadata(owner, key): UserMetadata`
- `upsertUserMetadata(owner, key, value, expiresAt?): UserMetadata`
- `deleteUserMetadata(owner, key): UserMetadata`
- `scanUserMetadata(owner, prefix?, after?, limit): UserMetadataScanResult`

关联管理：
- `upsertAttachment(owner, subject, attachmentType, configJson): Attachment`
- `deleteAttachment(owner, subject, attachmentType): Attachment`
- `listAttachments(owner, attachmentType?): List<Attachment>`
- `subscribeChannel(subscriber, channel): Subscription`
- `unsubscribeChannel(subscriber, channel): Subscription`
- `listSubscriptions(subscriber): List<Subscription>`
- `blockUser(owner, blocked): BlacklistEntry`
- `unblockUser(owner, blocked): BlacklistEntry`
- `listBlockedUsers(owner): List<BlacklistEntry>`

消息/事件/集群查询：
- `listMessages(target: UserRef, limit: Int): List<Message>`
- `listEvents(after: Long, limit: Int): List<Event>`
- `listClusterNodes(): List<ClusterNode>`
- `listNodeLoggedInUsers(nodeId: Long): List<LoggedInUser>`
- `resolveUserSessions(user: UserRef): ResolvedUserSessions`
- `operationsStatus(): OperationsStatus`
- `metrics(): String`

连接维护：
- `ping()` — 应用层 ping RPC

## 发布说明（Maven）

### 本地发布

```bash
cd turntf-kt
gradle publishToMavenLocal
```

发布后的坐标：
```kotlin
implementation("io.github.tursom:turntf-kt:0.1.0")
```

### 远程发布准备

`build.gradle.kts` 已配置 `maven-publish` 插件。完整远程发布需要补充：

1. 在 `publishing` 块中配置 `repositories`（如 Maven Central、GitHub Packages）。
2. 补充 javadoc 和 sources jar 任务。
3. 配置签名（GPG signing）用于 Maven Central 发布。
4. 设置 publishing 仓库的认证凭据（环境变量或 gradle.properties）。

当前远程仓库暂未配置，等待仓库 Owner 统一处理。

## 代码规范

### 文档注释

- **KDoc 是公开 API 的第一等公民**：所有 `public` / `protected` 类、接口、方法、属性必须有 KDoc 注释。
  - `data class` 可以按字段粒度注释，或是整体注释 + 字段命名自文档化。
  - `enum` 常量需逐项说明。
- **实现注释**：以下区域即使是非公开方法也必须有详细行内注释：
  - 协议映射（Proto ↔ Kotlin 模型的转换逻辑）
  - 连接状态机（runLoop、connectAttempt、重连退避判断）
  - 自动重连策略（指数退避、unauthorized 停重试、seen_messages 恢复）
  - pending RPC 生命周期（request_id 生成、超时、断线清理、并发安全）
  - 消息持久化顺序（saveMessage → saveCursor → ack 的完整约因）
  - Ack 时机（当前连接内提示 vs. 重连去重的实质依据）
  - 协程/Flow 语义（orderedDispatcher 的单线程顺序保证、events 热流与背压）
  - 错误处理边界（request_id == 0 的流级错误 vs. 具体 RPC 错误）
- **禁止低价值注释**：不要给简单的 getter/setter、显而易见的赋值操作或纯模板代码加注释。注释应帮助理解「为什么这么做」，而非「做了什么」。

### 异常处理

- `ServerError` 用于服务端返回的协议错误。
- `ProtocolError` 用于协议帧非法、缺字段、HTTP 状态码异常。
- `ConnectionError` 用于网络/拨号层失败。
- 所有挂起的 RPC 在断线时必须被异常终止（`failAllPending`）。
- `unauthorized` 错误会阻止自动重连，避免对服务端无意义爆破。

### 协程与并发

- 认证后的服务器帧通过单线程 `orderedDispatcher` 顺序处理，保证持久化 → ack → 事件发布的线序。
- `pending` 表使用 `ConcurrentHashMap`，`stateLock` 保护共享连接状态。
- `events` 使用 `MutableSharedFlow(extraBufferCapacity = 64)`，无 replay，消费方需保证不长期阻塞。
- `connect()` 和 `close()` 都是挂起函数，调用方需在合适的作用域内管理协程生命周期。

### 命名约定

- Kotlin 包名使用全小写：`io.github.tursom.turntf.kotlin`
- 公开 API 遵循 Kotlin 惯例：类名 PascalCase，方法/属性 camelCase。
- Proto 生成类保留 Java 风格（`ClientEnvelope`, `LoginRequest` 等），在 Internal.kt 中映射为 Kotlin 模型。
- HTTP JSON 字段使用 snake_case（服务端 REST 风格），映射时在 Internal.kt 中统一处理。

### 传输无关性

- 所有公开模型字段使用 `ByteArray` 而不是特定编码（UTF-8 字符串、base64）。
- HTTP 与 WebSocket 两种传输路径共享同一套模型类。
- HTTP 在边界做 JSON parse/serialize 的转换，WebSocket 在边界做 Proto ↔ Kotlin data class 的映射。

## 提交规范

### 作者身份

所有 git 提交的作者必须为：
```
tursom <tursom@foxmail.com>
```

不允许以其他身份（包括 Claude）提交。

### 提交信息风格

参考仓库现有提交历史，遵循常规提交格式：

```
<type>: <简短描述>

<详细说明（可选）>
```

type 取值示例：`feat`、`fix`、`chore`、`docs`、`test`、`refactor`。

### 提交前检查清单

1. 运行 `gradle test` 确认所有测试通过。
2. 检查 proto 生成代码是否与本地定义同步（`gradle generateProto`）。
3. 如果涉及协议变更，检查是否触及其他 SDK 的同步需求。
4. 更新 README 和 docs/ 文档，确保对外描述与实际实现一致。
5. 检查 `development.md` 中的共享语义检查清单。

### 跨模块改动注意事项

- 协议字段变更 → 同时检查 `turntf/`（服务端）、`turntf-java`、`turntf-kt` 的 proto 定义。
- 持久化顺序/ack 语义变更 → 检查所有 SDK 是否需要同步更新。
- 连接状态机/重连策略变更 → 在 KDoc 和实现注释中充分说明。

## 参考文档

- [README.md](./README.md) — 模块入口文档与快速开始
- [docs/realtime-client.md](./docs/realtime-client.md) — 实时客户端与 HTTP API 语义说明
- [docs/development.md](./docs/development.md) — 开发、测试与 proto 同步说明
- [docs/sdk-guide.md](./docs/sdk-guide.md) — SDK 总体使用指南
- [docs/http-client.md](./docs/http-client.md) — HTTP 客户端使用指南
- [根仓库 AGENTS.md](../../AGENTS.md) — 仓库级通用指南
