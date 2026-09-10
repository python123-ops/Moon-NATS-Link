///|
fn client_for_routing_test() -> Client {
  let writer : @aqueue.Queue[WriterCommand] = Queue(kind=Unbounded)
  {
    writer,
    subscriptions: {},
    flush_waiters: Queue(kind=Unbounded),
    request_timeout_ms: 20,
    inbox_prefix: "_INBOX.MOONBIT.test",
    next_sid: 1,
    next_request: 1,
    closing: false,
  }
}

///|
async test "route owned binary payload by server SID" {
  let client = client_for_routing_test()
  let inbox : @aqueue.Queue[Message] = Queue(kind=Unbounded)
  client.subscriptions[9] = inbox
  route_frame(
    client,
    Msg(subject="events.binary", sid=9, reply_to=Some("reply.9"), payload=[
      0x00, 0xFF,
    ]),
  )
  @debug.assert_eq(inbox.get(), {
    subject: "events.binary",
    reply_to: Some("reply.9"),
    headers: None,
    payload: [0x00, 0xFF],
  })
}

///|
async test "route HMSG fields without folding duplicate header names" {
  let client = client_for_routing_test()
  let inbox : @aqueue.Queue[Message] = Queue(kind=Unbounded)
  client.subscriptions[4] = inbox
  route_frame(
    client,
    HMsg(
      subject="rpc.echo",
      sid=4,
      reply_to=None,
      headers=@utf8.encode(
        "NATS/1.0\r\nTrace-Id: first\r\nTrace-Id: second\r\n\r\n",
      ),
      payload=b"reply",
    ),
  )
  let message = inbox.get()
  guard message.headers is Some(headers) else {
    fail("expected parsed HMSG headers")
  }
  @debug.assert_eq(headers.values("Trace-Id"), ["first", "second"])
  @debug.assert_eq(message.payload, b"reply")
}

///|
async test "match PONG to flush waiters in insertion order" {
  let client = client_for_routing_test()
  let first : @aqueue.Queue[Unit] = Queue(kind=Blocking(1))
  let second : @aqueue.Queue[Unit] = Queue(kind=Blocking(1))
  client.flush_waiters.put(first)
  client.flush_waiters.put(second)
  route_frame(client, Pong)
  @debug.assert_eq(first.try_get(), Some(()))
  @debug.assert_eq(second.try_get(), None)
  route_frame(client, Pong)
  @debug.assert_eq(second.try_get(), Some(()))
}

///|
async test "flush reaches the writer as one ordered command" {
  let client = client_for_routing_test()
  @async.with_task_group(group => {
    let flush_task = group.spawn(() => client.flush())
    match client.writer.get() {
      Write(_, _) => fail("expected an atomic Flush command")
      Flush(waiter) => {
        guard client.flush_waiters.try_get() is None else {
          fail("flush waiter escaped the writer ordering point")
        }
        client.flush_waiters.put(waiter)
        route_frame(client, Pong)
        flush_task.wait()
      }
    }
  })
}

///|
async test "local subscription close preserves buffered messages" {
  let client = client_for_routing_test()
  let inbox : @aqueue.Queue[Message] = Queue(kind=Unbounded)
  client.subscriptions[6] = inbox
  let subscription : Subscription = { client, sid: 6, inbox, closed: false, }
  inbox.put({
    subject: "events.buffered",
    reply_to: None,
    headers: None,
    payload: b"last",
  })
  subscription.close_local()
  @debug.assert_eq(subscription.next().payload, b"last")
  try subscription.next() catch {
    SubscriptionClosed => ()
    _ => fail("expected SubscriptionClosed")
  } noraise {
    _ => fail("expected closed mailbox")
  }
}

///|
async test "reject a message for an unknown SID" {
  let client = client_for_routing_test()
  try
    route_frame(
      client,
      Msg(subject="events.orphan", sid=77, reply_to=None, payload=b"x"),
    )
  catch {
    ProtocolViolation(message) =>
      @debug.assert_eq(
        message, "server delivered an unknown subscription id 77",
      )
    _ => fail("expected ProtocolViolation")
  } noraise {
    _ => fail("expected unknown SID rejection")
  }
}
