# turntf-kt 开发、测试与 proto 同步说明

本文档面向维护 `turntf-kt` 的开发者，说明模块结构、测试覆盖点、proto 生成流程，以及与 turntf 服务端共享的关键语义检查项。

## 1. 模块结构

核心文件如下：

- [README.md](../README.md)：模块入口文档
- [src/main/kotlin/io/github/tursom/turntf/kotlin/Models.kt](../src/main/kotlin/io/github/tursom/turntf/kotlin/Models.kt)
  定义公共模型、`Config`、`Credentials`、`CursorStore`、异常和事件类型
- [src/main/kotlin/io/github/tursom/turntf/kotlin/TurntfHttpClient.kt](../src/main/kotlin/io/github/tursom/turntf/kotlin/TurntfHttpClient.kt)
  HTTP JSON 客户端
- [src/main/kotlin/io/github/tursom/turntf/kotlin/TurntfClient.kt](../src/main/kotlin/io/github/tursom/turntf/kotlin/TurntfClient.kt)
  实时客户端、登录/重连/RPC/消息持久化主逻辑
- [src/main/kotlin/io/github/tursom/turntf/kotlin/internal/Internal.kt](../src/main/kotlin/io/github/tursom/turntf/kotlin/internal/Internal.kt)
  HTTP/Proto 映射、地址校验、枚举转换和工具函数
- [proto/client.proto](../proto/client.proto)
  Kotlin/JVM SDK 本地协议定义
- [src/test/kotlin/io/github/tursom/turntf/kotlin/TurntfHttpClientTest.kt](../src/test/kotlin/io/github/tursom/turntf/kotlin/TurntfHttpClientTest.kt)
  HTTP 层测试
- [src/test/kotlin/io/github/tursom/turntf/kotlin/TurntfClientTest.kt](../src/test/kotlin/io/github/tursom/turntf/kotlin/TurntfClientTest.kt)
  实时客户端测试

## 2. 构建与测试

模块要求：

- JDK 21
- Gradle

常用命令：

```bash
cd turntf-kt
gradle test
gradle build
gradle generateProto
gradle publishToMavenLocal
```

说明：

- `test` 会同时触发编译和 Proto 生成，适合日常验证。
- `generateProto` 只更新生成代码，适合协议修改后的快速同步。
- 生成物位于 `build/generated/` 下，不要手工编辑。

## 3. 当前测试覆盖点

### `TurntfHttpClientTest`

这组测试目前验证了以下语义：

- `login()` 会先在本地对明文口令做 bcrypt，再发往 `/auth/login`
- `createUser()` 会对创建用户时的明文口令做本地 bcrypt
- `body: ByteArray` 在 HTTP JSON 中按 base64 传输，并能回转成原始字节
- `listMessages()` / `postMessage()` 的 JSON 编解码结果与 SDK 公开模型一致

### `TurntfClientTest`

这组测试当前覆盖：

- WebSocket 登录阶段会发送本地 bcrypt 后的口令
- `connect()` 只有在拿到 `LoginResponse` 后才返回
- `loginState` / `connectionState` 在成功登录后会更新
- 收到 `MessagePushed` 后，SDK 会自动发送 `AckMessage`
- `sendMessage()` 的回包会被正常映射回 `Message`
- `ping()` / `pong()` 走完整的 `request_id` 关联路径
- `close()` 后连接状态会变成 `CLOSED`

### 目前未被测试直接覆盖的点

维护时应额外留意下列语义，目前它们更多依赖源码约束而不是单测：

- 指数退避重连和 `unauthorized` 停止重试
- `realtimeStream = true` 的能力裁剪
- 自定义 `CursorStore` 出错时的传播边界
- `resolveUserSessions()` 与会话定向 `sendPacket()` 的联动
- `events` 作为无 replay 热流时的背压与丢事件边界

如果后续修改触及这些部分，建议补单测。

## 4. Proto 同步工作流

`turntf-kt` 依赖本模块自己的 [proto/client.proto](../proto/client.proto)，但它承载的是 turntf 客户端共享协议，因此维护时要把“本地生成同步”和“跨模块共享语义同步”分开看。

### 4.1 什么时候需要改 proto

以下变化通常需要同步调整 proto：

- `LoginRequest` / `LoginResponse` 字段变化
- 新增或修改客户端 RPC
- `Message`、`Packet`、`SessionRef`、`MessageCursor` 等共享模型变化
- 错误返回、运维查询或会话解析结构变化

### 4.2 同步步骤

1. 修改 [proto/client.proto](../proto/client.proto)。
2. 如果变更属于共享协议，同时检查并同步：
   - `turntf/proto/client.proto`
   - `turntf-java/proto/client.proto`
3. 运行：

```bash
cd turntf-kt
gradle generateProto
```

4. 再运行：

```bash
cd turntf-kt
gradle test
```

5. 检查 Kotlin 代码里对 `notifier.client.v1.Client` 的调用点是否需要同步调整。
6. 如果协议语义变化影响对外使用方式，同步更新 [README.md](../README.md) 和 [realtime-client.md](./realtime-client.md)。

### 4.3 生成代码注意点

- 本模块通过 `com.google.protobuf` Gradle 插件生成 `lite` Java 类。
- Kotlin 代码通过 `notifier.client.v1.Client` 访问这些生成结果。
- 生成文件不应手改，必须以 `proto/client.proto` 为唯一来源。

## 5. 共享语义检查清单

在修改 `TurntfClient`、`Models`、`Internal` 或 proto 时，至少逐条核对以下共享语义。

### 5.1 持久消息可靠性

- 收到 `MessagePushed` 时必须先 `saveMessage()`，再 `saveCursor()`，最后才允许 `AckMessage`
- `AckMessage` 只是连接内提示，不是服务端落库状态
- 重连去重的真实依据是下一次登录携带的 `seen_messages`

### 5.2 回包也要推进游标

- `sendMessageResponse.message` 是持久消息，必须像普通推送一样推进 `CursorStore`
- 否则客户端在“发送成功后立刻断线”的窗口里可能会重复消费自己刚发出的消息

### 5.3 `session_ref` 只服务在线会话

- `LoginResponse.session_ref` 用来标识某一条在线连接
- `target_session` 只允许出现在 transient packet 路径
- `SessionRef(0, "")` / `null` 表示“不定向到具体会话”

### 5.4 packet 不参与持久消息语义

- `PacketPushed` 没有 `(node_id, seq)` 游标
- packet 不参与 `seen_messages`
- packet 不参与 `AckMessage`
- 如果应用要暂存或去重，应按 `packet_id` 自行处理

### 5.5 `realtimeStream` 与 `transientOnly`

- `realtimeStream = true` 必须连接 `/ws/realtime`
- `/ws/realtime` 只适合瞬时互动，不应暴露持久消息或大部分管理 RPC
- `transientOnly = true` 是登录能力声明，不等于 `/ws/realtime`

### 5.6 事件流与状态流

- `events` 是热流、无 replay，不适合承担“当前状态快照”的职责
- `loginState` 才是当前登录会话快照
- `connectionState` 才是连接生命周期快照
- 已认证后的服务器帧必须保持顺序处理，否则会破坏“持久化 -> ack -> 发布事件”的线序

### 5.7 `request_id` 约束

- SDK 本地生成的 `request_id` 必须保持正数
- 协议里是 `uint64`，但 Kotlin 公开模型用 `Long`
- 对超出有符号 `Long` 范围的服务端值，SDK 当前会抛 `ProtocolError`

## 6. 修改文档或实现时建议一起检查的内容

- README 示例是否仍能反映当前公开 API
- `realtime-client.md` 中的 `Config` 字段说明是否与默认值一致
- `TurntfHttpClient` 的 HTTP 能力清单是否与真实方法列表一致
- `TurntfClient` 支持的 RPC 是否与 proto 和服务端当前实现一致
- JVM 侧 proto 是否与服务端主参考实现保持同步

## 7. 推荐的维护顺序

当你需要修改 Kotlin SDK 的实时语义时，推荐按下面顺序进行：

1. 先确认服务端共享协议和语义是否已经确定。
2. 修改 `proto/client.proto` 和映射函数。
3. 修改 `TurntfClient` / `TurntfHttpClient` / `Models`。
4. 补单测，优先覆盖重连、游标、packet 和错误处理边界。
5. 最后更新 README 与使用文档，确保对外描述与实现一致。
