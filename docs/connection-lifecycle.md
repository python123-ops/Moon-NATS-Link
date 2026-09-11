# 一条连接里的消息时序

连接建立阶段仍然顺序读写：先等待服务器的 `INFO`，随后写入启用 headers 与 No Responders 的 `CONNECT` 和一个 `PING`，直到读到对应的 `PONG` 才把 `Client` 交给调用者。token 或成对的用户名密码只在这一步写入 CONNECT，不进入服务器地址；授权失败也发生在 `Client` 交付之前。这样后台任务启动时，连接已经越过握手边界，订阅邮箱也不会收到握手前的消息。

握手后，一条连接只有一个 reader 和一个 writer。`publish`、`subscribe` 以及 reader 对服务端 `PING` 的响应都写入 writer 邮箱；发送调用等待该命令完成一次 socket write，因此同一连接上的 `SUB` 会先于紧随其后的 `PUB`。这个完成信号不表示服务端已经处理命令。

reader 把每个 socket 分块立即交给 `Decoder`。控制行可以在任意字节处分割；读到 `MSG` 或 `HMSG` 头后，Decoder 改按声明长度等待 body 和结尾 CRLF，不在消息体里搜索分隔符。HMSG 的 header length 与 total length 分别切出原始 header block 和 payload，随后客户端才解释 `NATS/1.0` 状态行与字段。完成的消息按 SID 放入对应订阅邮箱，未知 SID 被视为连接级协议错误。

每个订阅邮箱最多积压 1024 条消息或 8 MiB 待处理字节；MSG 计 payload，HMSG 计原始 header block 与 payload。reader 只做非阻塞投递，任一上限被下一条消息越过时关闭该邮箱并记录 `SlowConsumer`，已经入队的消息仍可先被取走。writer 随后把该 SID 的 `UNSUB` 和 `PING` 作为一次退休动作发出；对应 `PONG` 到达后才移除 SID，因此屏障前已经在 TCP 流中的消息不会升级成未知 SID 协议错误。处于退休状态的 SID 不再接收新消息，其他订阅继续路由。

主动 flush 不复用普通写入完成信号。它作为一条完整命令进入 writer 邮箱；writer 处理到这条命令时，先把对应等待邮箱排入 FIFO，再立即发送 `PING`。登记与写入不会被另一个 flush 穿插，reader 收到 `PONG` 后只唤醒队首。

普通 unsubscribe 与 drain 在这里分开。unsubscribe 先把邮箱标成 `Detached`，不再接纳随后到达的消息，再由一条 writer 命令连续写 `UNSUB`、登记 SID 清理、写 `PING`；PONG 到达前 SID 仍留在 Map 中，因此在途 MSG 不会变成 unknown SID。订阅 drain 使用另一条 writer 命令连续写 `UNSUB`、登记 `FinishSubscriptionDrain`、写 `PING`，状态在此期间是 `DrainingServer`，reader 仍把在途消息放入邮箱。PONG 将 SID 移出 Map、关闭但不清空邮箱；消费者处理最后一条消息后再次调用 `next`，观察到 `SubscriptionClosed` 时所有 drain 等待者才一起完成。协议动作属于 writer/reader，取消某个等待任务不会取消这次收尾。

request 为每次调用建立一个精确 inbox 订阅。inbox 前缀来自 12 个系统随机字节，连接内序号区分后续请求；`UNSUB sid 1` 在带 reply subject 的 PUB 或 HPUB 之前写入。同一条响应路径处理普通 MSG、带 headers 的 HMSG 和服务端 503。等待超过 `Options` 中的毫秒数时，请求先取消 inbox 并跨过 PING/PONG 屏障，再把 `Timeout` 交给调用者。

连接 drain 先把状态切到 `DrainingSubscriptions`，所以新的 SUB 和 request 会失败，但处理已收消息的任务仍可 PUB 回复。它为所有活跃 SID 共享一个服务器屏障，等各消费者观察到邮箱关闭后再切到 `DrainingPublishes`；第二个屏障确认最后的发布已经被服务器处理，此后所有公开操作返回 `ConnectionClosed`。

`with_client` 是连接的所有者。回调正常返回时，它关闭仍存活的订阅邮箱，并让任务组携带回调结果立即收尾；阻塞在 socket read 的 reader 与等待邮箱的 writer 都会收到取消，随后连接句柄关闭。没有显式调用 `Client::drain` 时，这里仍执行作用域关闭，不伪装成一次协议 drain；但逃出回调的订阅句柄也不会再永久阻塞在 `next`。
