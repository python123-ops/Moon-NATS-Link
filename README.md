# Moon NATS Link

Moon NATS Link 正在用 MoonBit 实现 Core NATS 客户端。连接先读取 `INFO`，发出 `CONNECT`，再用一次 `PING/PONG` 确认此前的写入已经到达服务器；握手完成后，发布和订阅共用这条长连接。

## 跑通第一条连接

先在 Linux 或 macOS 启动 `nats-server v2.14.6`：

```bash
nats-server -p 4222
```

另开终端运行 native 握手探针：

```bash
moon run src/examples/handshake --target native
```

连接成功时输出：

```text
handshake ok: INFO -> CONNECT -> PING/PONG
```

探针固定连接 `127.0.0.1:4222`。它会保留服务器发来的原始 `INFO` JSON，响应握手过程中的服务端 `PING`，并把 `-ERR` 作为连接失败返回。

## 发出一条二进制事件

`event_fanout` 在同一连接上建立精确订阅 `events.binary` 和通配订阅 `events.*`，随后发布 `00 0d 0a ff`。payload 中间的 CRLF 不会被误认成协议边界，两份订阅各收到一份内容相同的消息。

```bash
moon run src/examples/event_fanout --target native
```

```text
event fanout ok: 00 0d 0a ff reached both subscriptions
```

## 让队列组领取任务

`queue_workers` 为 `jobs.build` 建立两个同名为 `workers` 的队列订阅。服务端从组内选择一个成员交付 `task-a`，示例通过并发等待两个邮箱来接收被选中的那一份；它没有假定成员之间存在固定轮转顺序。

```bash
moon run src/examples/queue_workers --target native
```

```text
queue group ok: one worker claimed task-a
```

## 字节流先于连接封装

NATS 控制行以 CRLF 结束，但一次 socket read 不保证对应一行。`wire.Decoder` 接受 `BytesView`，把未完成的尾部留在自身缓冲区；关键字、JSON、payload 以及结尾 CRLF 都可以跨 read。`MSG` 的控制行经过 UTF-8 检查后切换到定长 payload 状态，因此消息体可以保留零字节、非 UTF-8 数据和内嵌 CRLF。

```moonbit
let decoder = @wire.Decoder::new()
let first = decoder.feed(b"PI".view()).unwrap()
let second = decoder.feed(b"NG\r\n".view()).unwrap()
```

编码端提供 `CONNECT`、`PING`、`PONG`、`SUB` 和 `PUB`。`CONNECT` 内容由 JSON 值生成，客户端名不会直接拼进协议行；`PUB` 只为控制行编码文本，payload 保持原始字节。

连接内 reader、writer 与订阅邮箱的所有权记录在 [一条连接里的消息时序](docs/connection-lifecycle.md)。

## 运行协议测试

```bash
moon fmt --check
moon check src/wire --target all --deny-warn --warn-list +73
moon test src/wire --target all --deny-warn --warn-list +73
moon check src/client --target native --deny-warn --warn-list +73
moon test src/client --target native --deny-warn --warn-list +73
```

2026-09-10 使用 MoonBit `0.1.20260904` 在 wasm、wasm-gc、js、native 四个后端分别运行 17 个 `wire` 测试，全部通过；Ubuntu 24.04 WSL 使用同版工具链运行 7 个 native 客户端测试，并连接校验过 SHA-256 的官方 `nats-server v2.14.6`，依次得到上面两个示例输出。

## License

Apache-2.0
