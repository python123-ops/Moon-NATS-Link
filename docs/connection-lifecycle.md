# 握手阶段的连接时序

第一条 native 连接采用单任务顺序读写。探针先等待服务器的 `INFO`，随后连续写入 `CONNECT` 与 `PING`，直到读到对应的 `PONG` 才报告成功。没有在收到 `INFO` 前抢先发送客户端参数，也没有把一次 `read_some` 当作一条完整协议消息。

socket 读出的每个 `BytesView` 立即交给 `Decoder`。Decoder 自己持有未完成的尾部，因此分块可以落在关键字中间，也可以恰好落在 `\r` 与 `\n` 之间。读循环不保存指向 socket 缓冲区的视图。

服务器在握手中发送 `PING` 时，由同一个连接任务直接写回 `PONG`。这个探针没有后台 reader 或 writer，也没有借它提前定义完整 `Client` 生命周期；当前唯一的连接所有者会在退出时关闭 socket。

本次读取路径只接受 `INFO`、`+OK`、`-ERR`、`PING` 和 `PONG` 控制行。带 payload 的 `MSG`、`HMSG` 不能套用按 CRLF 截行的办法，因而没有混入这个解析分支。
