# turntf-kt 实时客户端与 HTTP API 说明

本文档从实现语义出发，说明 `turntf-kt` 的两个客户端入口、协程/`Flow` 行为、`CursorStore` 契约，以及与 turntf 服务端共享的关键协议约束。

## 1. 两类客户端的职责边界

### `TurntfHttpClient`

`TurntfHttpClient` 是纯 HTTP JSON 客户端，适合：

- 通过 `POST /auth/login` 获取 Bearer token
- 管理用户、channel、订阅、黑名单
- 查询历史消息或集群节点
- 在不维持长连接的脚本/后台任务中调用 turntf

它的特征是：

- 所有公开方法都是 `suspend`
- 公共模型使用 `ByteArray`，避免把 REST 的 base64 / 内嵌 JSON 细节泄漏到业务层
- HTTP 非预期状态码会抛出 `ProtocolError`
- 传输故障会包装为 `ConnectionError`

### `TurntfClient`

`TurntfClient` 是协程优先的实时客户端，适合：

- 长时间在线并持续接收 `MessagePushed` / `PacketPushed`
- 在断线后自动重连、自动重登录
- 通过 `Flow` 统一消费事件和连接状态
- 在 WebSocket 上复用 turntf 的用户管理、列表查询和运维 RPC

它本质上是“长连接管理器 + RPC 多路复用器 + 消息持久化协调器”。

## 2. 认证模型：`PasswordInput`、`Credentials`

### `PasswordInput`

SDK 把口令分成两种来源：

- `plainPassword("secret")`
  语义：明文只在本地出现，SDK 立即做 bcrypt，再把哈希发送给 turntf。
- `hashedPassword("$2a$...")`
  语义：调用方已经持有 bcrypt 哈希，SDK 直接透传，不再重复哈希。

推荐做法：

- 业务刚拿到用户明文时，用 `plainPassword()`。
- 如果密码哈希来自安全存储或其他认证系统，用 `hashedPassword()`。

### `Credentials`

`Credentials(nodeId, userId, password)` 只用于长连接登录：

- `nodeId` / `userId` 对应 turntf 登录用户身份
- `password` 必须是 `PasswordInput`
- `TurntfClient.connect()` 建立连接后，第一帧就是带这组凭据的 `LoginRequest`

## 3. `Config` 字段逐项说明

`TurntfClient` 的行为全部由 `Config` 驱动。

| 字段 | 作用 | 默认值 / 注意点 |
| --- | --- | --- |
| `baseUrl` | HTTP 基地址，同时也是 WebSocket 地址的来源 | 必填；`http` 会映射成 `ws`，`https` 会映射成 `wss` |
| `credentials` | 长连接登录身份 | 必填 |
| `cursorStore` | 已持久化消息游标的读写接口 | 默认 `MemoryCursorStore()`，只适合测试、演示和短生命周期进程 |
| `httpClient` | 复用的 `OkHttpClient` | 默认新建 |
| `reconnect` | 是否自动重连 | 默认 `true` |
| `initialReconnectDelay` | 首次重连等待时间 | 默认 1 秒 |
| `maxReconnectDelay` | 指数退避上限 | 默认 30 秒 |
| `pingInterval` | 自动 ping 间隔 | 默认 30 秒 |
| `requestTimeout` | RPC 超时时间 | 默认 10 秒 |
| `ackMessages` | 持久消息落盘后是否自动发 `AckMessage` | 默认 `true` |
| `transientOnly` | 登录时声明“我不需要持久消息推送” | 默认 `false` |
| `realtimeStream` | 是否连接 `/ws/realtime` 而不是 `/ws/client` | 默认 `false` |

有两个字段特别容易混淆：

### `transientOnly`

这是登录帧上的能力声明，不改变连接地址。

效果是：

- 服务端不会为该会话补发历史持久消息
- 服务端不会为该会话开启后续持久消息推送
- 适合“只发 transient packet / 只做瞬时互动”的连接

### `realtimeStream`

这是连接级开关，会把 WebSocket 地址从 `/ws/client` 改成 `/ws/realtime`。

它不仅意味着 `transientOnly` 语义，还会让服务端拒绝大部分管理/查询 RPC。`/ws/realtime` 适合：

- 临时在线会话
- 只关心 `PacketPushed`
- 需要 `resolveUserSessions()` 后定向发包，但不需要持久消息补发

## 4. Suspend HTTP API 语义

### 登录

- `login(nodeId, userId, password: String)`
  先本地 bcrypt，再调用 `/auth/login`
- `loginWithPassword(nodeId, userId, password: PasswordInput)`
  适合你已经决定好口令输入形式时使用

HTTP 登录返回的是 Bearer token，而不是 `LoginInfo`。这点和 WebSocket 登录不同。

### 用户与 channel

- `createUser()`
- `createChannel()`

`createChannel()` 只是把 `role` 默认补成 `channel`，其余行为与 `createUser()` 一致。

### 消息与 packet

- `listMessages(token, target, limit)`
- `postMessage(token, target, body)`
- `postPacket(token, targetNodeId, relayTarget, body, mode)`

其中 `postPacket()` 只对应 transient packet：

- `targetNodeId` 必须等于 `relayTarget.nodeId`
- `mode` 只能是 `BEST_EFFORT` 或 `ROUTE_RETRY`
- 返回 `202 Accepted` 表示已进入瞬时路由层，不代表目标用户已经收到

### 附件、订阅、黑名单

HTTP 层没有再暴露单独的“订阅实体协议类型”，而是复用附件语义：

- `createSubscription()` 本质上会写入 `CHANNEL_SUBSCRIPTION`
- `blockUser()` / `unblockUser()` 会映射到 `USER_BLACKLIST`

### JSON 与字节数组的边界

对业务层最关键的约定是：

- `body` 一律视为原始字节数组，不要求 UTF-8
- `profileJson`、`configJson` 在 SDK 内也是 `ByteArray`
- HTTP REST 需要内嵌 JSON 时，SDK 在边界做一次 parse / serialize，业务层仍面对字节数组

这使得 HTTP 与 WebSocket 在模型层几乎一致，业务代码可以共用数据结构。

## 5. 实时客户端生命周期

### `connect()`

`connect()` 的语义不是“TCP/WS 已打开”，而是：

1. WebSocket 已建立
2. 第一帧 `LoginRequest` 已发出
3. 服务端已经返回 `LoginResponse`
4. 当前会话已进入可用状态

只有满足上述条件后，`connect()` 才会返回。

如果此时已经处于已认证状态，再次调用 `connect()` 会直接返回，不会新建连接。

### `close()`

`close()` 会：

1. 停止后续重连
2. 关闭当前 WebSocket
3. 取消自动 ping
4. 让所有等待中的 RPC 以异常结束
5. 关闭内部协程作用域

关闭后，`connectionState` 会变成 `CLOSED`，客户端实例不可再次复用。

## 6. `Flow` 暴露面：事件流与状态流

### `events: SharedFlow<ClientEvent>`

`events` 是一个热流：

- 无 replay
- 额外缓冲容量为 64
- 由 SDK 内部通过 `tryEmit()` 推送

这意味着：

- 新订阅者看不到历史事件
- 如果消费方长期不收集，极端情况下事件可能被丢弃
- 想拿当前快照时，应优先读 `loginState` / `connectionState`

事件类型包括：

- `ClientEvent.Login`
- `ClientEvent.MessageReceived`
- `ClientEvent.PacketReceived`
- `ClientEvent.Error`
- `ClientEvent.Disconnect`

### `loginState: StateFlow<LoginInfo?>`

这是“当前已认证会话”的快照：

- 登录成功后更新为 `LoginInfo`
- 断线或重连中会回到 `null`
- 可用于拿当前 `sessionRef`

### `connectionState: StateFlow<ConnectionState>`

连接状态只有四种：

- `DISCONNECTED`
- `CONNECTING`
- `CONNECTED`
- `CLOSED`

注意：

- `CONNECTED` 表示收到了 `LoginResponse`，不是单纯的底层 socket open
- 自动重连期间，状态会在 `DISCONNECTED -> CONNECTING -> CONNECTED` 之间循环

### 事件顺序保证

SDK 为已认证后的服务器帧专门使用了单线程 dispatcher 顺序处理，因此：

- 消息持久化
- 自动 ack
- RPC 完成
- `events` 发布

都会按服务端的线序执行。这样做的目的是避免 OkHttp 并发回调打乱“先落盘、再确认、再通知业务”的顺序。

## 7. `CursorStore` 契约与持久化顺序

`CursorStore` 是 Kotlin SDK 最关键的可插拔接口之一：

```kotlin
interface CursorStore {
    suspend fun loadSeenMessages(): List<MessageCursor>
    suspend fun saveMessage(message: Message)
    suspend fun saveCursor(cursor: MessageCursor)
}
```

### `loadSeenMessages()`

返回值会直接放进下一次 `LoginRequest.seen_messages`。因此它必须满足：

- 只返回“已经可靠持久化完成”的持久消息游标
- 跨进程重启仍然稳定可用
- 不依赖瞬时内存状态

### `saveMessage()` 与 `saveCursor()`

SDK 收到持久消息时，固定顺序是：

1. `saveMessage(message)`
2. `saveCursor(message.cursor())`
3. 如果 `ackMessages = true`，发送 `AckMessage`
4. 发布 `ClientEvent.MessageReceived`

这个顺序不能反过来，原因是：

- `AckMessage` 只在当前连接内更新服务端的内存去重集合
- 真正决定重连补发去重的是下次登录时的 `seen_messages`
- 如果先保存 cursor、甚至先 ack，再写消息正文，断线后就可能出现“服务端认为你见过，但你本地没有内容”的坏状态

### `sendMessage()` 的特殊点

`sendMessage()` 的响应如果返回的是持久 `Message`，SDK 也会先执行：

1. `saveMessage(response.message)`
2. `saveCursor(response.message.cursor())`
3. 然后才让 `sendMessage()` 的挂起调用恢复

这样做是为了防止客户端“刚发完消息立刻断线重连”，然后把自己刚发出的回包再次当作未见历史消息补回来。

### `MemoryCursorStore`

默认实现 `MemoryCursorStore` 只适合：

- 测试
- Demo
- 短生命周期工具

它不会跨进程保存状态，因此生产环境通常应替换为数据库、KV 或日志型持久化实现。

## 8. 重连、重登录与超时

### 自动重连策略

当连接中断后，如果满足以下条件，SDK 会自动重连：

- `closed == false`
- `Config.reconnect == true`
- 失败原因不是登录阶段的 `ServerError(code = "unauthorized")`

退避策略：

- 从 `initialReconnectDelay` 开始
- 每次失败翻倍
- 上限为 `maxReconnectDelay`
- 只有在一次完整的“连接成功 + 登录成功”之后才重置为初始值

### 重登录时会做什么

每次重连前，SDK 都会：

1. 调用 `cursorStore.loadSeenMessages()`
2. 把结果写入新的 `LoginRequest.seen_messages`
3. 重新发送同一份 `Credentials`

也就是说，去重恢复完全依赖 `CursorStore` 的内容，而不是依赖上一条连接上的 ack 历史。

### 自动 ping

登录成功后，SDK 会启动后台 ping 循环：

- 每隔 `pingInterval` 调一次 `ping()`
- `ping()` 本身也是一个带 `request_id` 的 RPC
- 如果 ping 失败，但错误属于“连接已断开/已关闭”，SDK 不会额外制造噪音；其他错误会通过 `ClientEvent.Error` 抛给业务层

### RPC 超时与断线

每个 RPC 都会：

- 生成正数 `request_id`
- 在 `pending` 表中登记一个 `Deferred`
- 启动超时协程，超时后抛 `TimeoutException`

如果连接断开：

- 所有挂起中的 RPC 都会立即失败
- `loginState` 清空
- `connectionState` 退回 `DISCONNECTED`
- SDK 发送 `ClientEvent.Disconnect`

## 9. `session_ref`、会话定向 packet 与在线会话查询

### `session_ref` 从哪里来

每次登录成功，服务端都会在 `LoginResponse` 中返回 `session_ref`。Kotlin SDK 将其暴露为：

- `LoginInfo.sessionRef`
- `client.loginState.value?.sessionRef`

`SessionRef` 表示“当前这个在线会话”的服务端身份，不等同于用户身份。

### `resolveUserSessions()`

如果你需要把瞬时包定向到某个在线会话，而不是“发给该用户的任意一个在线连接”，可以先调用：

```kotlin
val resolved = client.resolveUserSessions(UserRef(4096, 1025))
```

返回结果里有两类信息：

- `presence`：按服务节点聚合后的在线数量
- `sessions`：具体会话列表，每项都带 `SessionRef`、传输类型和 `transientCapable`

### `sendPacket()` 与 `targetSession`

`sendPacket()` 是 Kotlin SDK 对 transient `send_message` 的语义包装：

```kotlin
client.sendPacket(
    SendPacketInput(
        target = UserRef(4096, 1025),
        body = payload,
        deliveryMode = DeliveryMode.ROUTE_RETRY,
        targetSession = sessionRef
    )
)
```

关键约束：

- `targetSession` 只允许用于 transient packet
- 持久 `sendMessage()` 不允许携带 `targetSession`
- `SessionRef(0, "")` 或 `null` 表示“不指定具体会话”

### `RelayAccepted` 与 `PacketReceived` 的区别

- `RelayAccepted`：发送端收到，表示该 packet 已被本地/路由层接受
- `PacketReceived`：接收端收到，表示当前在线会话真正拿到了 packet

二者之间可能存在时间差，也可能只有前者没有后者。因此：

- 不要把 `RelayAccepted` 当成业务送达确认
- 如果业务要对瞬时包去重，应按 `packetId` 自己做

## 10. `realtimeStream` 的能力裁剪

当 `Config.realtimeStream = true` 时，SDK 会连到 `/ws/realtime`。该模式的共享语义是：

- 只支持 transient `send_message`
- 不提供持久消息历史补发
- 不提供持久消息推送
- 适合只做在线会话互动或临时 presence 探测

根据当前服务端实现，`/ws/realtime` 下允许或常见的能力主要是：

- `sendPacket()` / transient `send_message`
- `resolveUserSessions()`
- `listClusterNodes()`
- `listNodeLoggedInUsers()`
- `ping()`

会被服务端拒绝的典型能力包括：

- 持久 `sendMessage()`
- `createUser()` / `getUser()` / `updateUser()` / `deleteUser()`
- `listMessages()`
- `upsertAttachment()` / `deleteAttachment()` / `listAttachments()`
- `listEvents()`
- `operationsStatus()`
- `metrics()`

如果你的连接只需要 transient 能力，但仍想使用 `/ws/client`，可以只开启 `transientOnly = true`；这比 `realtimeStream = true` 的能力面更宽。

## 11. 错误处理约定

SDK 的异常基类是 `TurntfException`，主要派生类型如下：

- `ServerError`
  服务端返回了 `ServerEnvelope.error`
- `ProtocolError`
  协议帧非法、响应缺字段、HTTP 状态码不符合预期等
- `ConnectionError`
  网络层或拨号层失败

### 登录阶段错误

如果登录阶段收到 `ServerError(code = "unauthorized")`：

- `connect()` 会失败
- SDK 会把 `stopReconnect` 置为 `true`
- 后续不会自动重试，避免对服务端做无意义爆破

### 已登录阶段错误

服务端在已认证阶段发回 `ServerEnvelope.error` 时：

- `request_id != 0`：SDK 会把对应挂起中的 RPC 直接失败
- `request_id == 0`：说明它不是某个具体 RPC 的结果，而是流级错误；SDK 会发出 `ClientEvent.Error`

### 业务侧的推荐处理方式

- 对调用式接口：`try/catch` `ServerError`、`ProtocolError`、`ConnectionError`
- 对流式错误：持续收集 `events`，处理 `ClientEvent.Error` 和 `ClientEvent.Disconnect`
- 对连接快照：把 `loginState` / `connectionState` 绑定到你的 UI、生命周期或健康检查逻辑

## 12. 推荐使用模式

### 生产环境建议

- 自定义 `CursorStore`，使用数据库或 KV 做持久化
- 在应用启动早期就开始收集 `events`
- 依赖 `loginState` 获取当前 `sessionRef`
- 需要精确会话定向时，先 `resolveUserSessions()` 再 `sendPacket()`

### 避免的做法

- 不要把 `MemoryCursorStore` 用在长生命周期生产进程
- 不要把 `AckMessage` 当作持久送达保证
- 不要把 `PacketReceived` 写进持久消息游标表
- 不要在 `realtimeStream` 连接上调用持久消息或管理类 RPC
