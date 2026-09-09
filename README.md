# Moon NATS Link

Moon NATS Link 正在用 MoonBit 实现 Core NATS 客户端。仓库从连接建立时最容易被 TCP 分块干扰的一段开始：读取 `INFO`，发出 `CONNECT`，再用一次 `PING/PONG` 确认此前的写入已经到达服务器。

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

## 字节流先于连接封装

NATS 控制行以 CRLF 结束，但一次 socket read 不保证对应一行。`wire.Decoder` 接受 `BytesView`，把未完成的尾部留在自身缓冲区；关键字、JSON 以及 CRLF 都可以跨 read。控制行先经过严格 UTF-8 和 CRLF 检查，未知命令不会被跳过。

```moonbit
let decoder = @wire.Decoder::new()
let first = decoder.feed(b"PI".view()).unwrap()
let second = decoder.feed(b"NG\r\n".view()).unwrap()
```

编码端目前提供握手所需的 `CONNECT`、`PING` 与 `PONG`。`CONNECT` 内容由 JSON 值生成，客户端名不会直接拼进协议行。

连接的所有权与当前时序记录在 [握手阶段的连接时序](docs/connection-lifecycle.md)。带 payload 的 `MSG` 和 `HMSG` 需要长度驱动的状态机，本次代码没有把它们伪装成普通控制行。

## 运行协议测试

```bash
moon fmt --check
moon check src/wire --target all --deny-warn
moon test src/wire --target all --deny-warn
```

2026-09-10 使用 MoonBit `0.1.20260904` 在 wasm、wasm-gc、js、native 四个后端运行 9 个 `wire` 测试，四个后端均通过；Ubuntu 24.04 WSL 使用同版工具链复跑 native 测试并连接校验过 SHA-256 的官方 `nats-server v2.14.6`，握手输出与上文一致。

## License

Apache-2.0
