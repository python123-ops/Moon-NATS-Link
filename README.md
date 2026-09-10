# Moon NATS Link

Moon NATS Link 是用 MoonBit 编写的 Core NATS 客户端。协议内核把 TCP 分块还原为二进制安全的 `MSG/HMSG`，native 客户端再用一条连接承载发布订阅、队列任务和请求应答。

![从 TCP 分块到类型化请求结果](docs/message-path.svg)

## 第一个请求应答

先在 Linux 或 macOS 启动 `nats-server v2.14.6`：

```bash
nats-server -p 4222
```

另开终端运行请求应答示例：

```bash
moon run src/examples/request_reply --target native
```

```text
request/reply ok: headers + 503 no responders + timeout
```

这个示例先订阅 `rpc.echo`，携带 `Trace-Id` 发出请求，再从随机 reply inbox 收到带 `Handled-By` 的响应。随后它请求一个无人订阅的 subject，确认服务端 503 被映射为 `NoResponders`；最后让一个已订阅的处理者保持沉默，确认 120 ms 后得到 `Timeout`，并完成 inbox 取消与 flush 屏障。

每次请求使用 12 个系统随机字节形成 inbox 前缀，再附加连接内递增序号。reply SID 在发布前发送 `UNSUB sid 1`，因此服务器只会投递第一份响应。

## 两种消息分发方式

`event_fanout` 在同一连接上建立精确订阅 `events.binary` 和通配订阅 `events.*`，随后发布 `00 0d 0a ff`。payload 中间的 CRLF 不会被误认成协议边界，两份订阅各收到一份内容相同的消息。

```bash
moon run src/examples/event_fanout --target native
```

```text
event fanout ok: 00 0d 0a ff reached both subscriptions
```

`queue_workers` 为 `jobs.build` 建立两个同名为 `workers` 的队列订阅。服务端从组内选择一个成员交付 `task-a`，示例通过并发等待两个邮箱来接收被选中的那一份，不假定成员之间存在固定轮转顺序。

```bash
moon run src/examples/queue_workers --target native
```

```text
queue group ok: one worker claimed task-a
```

## 一条连接中发生的事

连接先读取 `INFO`，发出启用 headers 与 No Responders 的 `CONNECT`，再用一次 `PING/PONG` 确认握手写入已经到达服务器。`with_client` 随后启动唯一的 reader 和 writer；发布、订阅、取消与 PONG 响应都经过 writer 邮箱。

`wire.Decoder` 在控制行状态和定长 body 状态之间切换。`HMSG` 的 header length 切出完整 `NATS/1.0` header block，total length 决定 payload 终点；消息体不会经过 UTF-8 转换。客户端解析 header 时保留字段大小写、重复值与到达顺序，并拒绝 CRLF 注入或非 ASCII 字段。

`Client::flush` 将等待者排入 FIFO 队列后发送 `PING`。reader 收到相应的 `PONG` 才唤醒等待者，因此 `Subscription::unsubscribe` 可以先发 `UNSUB`，跨过这个服务器处理屏障，再移除本地 SID。屏障前已进入邮箱的消息仍可读取，耗尽后返回 `SubscriptionClosed`。

只观察握手时序可以运行：

```bash
moon run src/examples/handshake --target native
```

```text
handshake ok: INFO -> CONNECT -> PING/PONG
```

连接与邮箱之间更细的所有权取舍记录在 [一条连接里的消息时序](docs/connection-lifecycle.md)。

## 互操作与协议测试

```bash
moon fmt --check
moon check src/wire --target all --deny-warn --warn-list +73
moon test src/wire --target all --deny-warn --warn-list +73
moon check src/client --target native --deny-warn --warn-list +73
moon test src/client --target native --deny-warn --warn-list +73
```

2026-09-10 使用 MoonBit `0.1.20260904` 在 wasm、wasm-gc、js、native 四个后端分别运行 21 个 `wire` 测试；Ubuntu 24.04 WSL 使用同版工具链运行 15 个 native 客户端测试。两组测试全部通过。客户端连接校验过 SHA-256 的官方 `nats-server v2.14.6` 后，实际完成二进制扇出、队列领取、带 headers 的 request/reply、503 No Responders、超时清理、flush 与 unsubscribe。

## License

Apache-2.0
