///|
fn client_for_routing_test(
  subscription_message_limit? : Int = default_subscription_message_limit,
  subscription_byte_limit? : Int = default_subscription_byte_limit,
) -> Client {
  let writer : @aqueue.Queue[WriterCommand] = Queue(kind=Unbounded)
  {
    server_pool: { configured: ["127.0.0.1:4222"], discovered: [], },
    writer,
    connection_waiters: {},
    subscriptions: {},
    pong_actions: Queue(kind=Unbounded),
    request_timeout_ms: 20,
    subscription_message_limit,
    subscription_byte_limit,
    inbox_prefix: "_INBOX.MOONBIT.test",
    next_connection_waiter: 1,
    next_sid: 1,
    next_request: 1,
    status: Open,
  }
}

///|
fn attach_test_subscription(client : Client, sid : Int) -> Subscription {
  let state = new_subscription_state(client.subscription_message_limit, {
    subject: "test.\{sid}",
    queue_group: None,
    max_messages: None,
    recover_on_reconnect: true,
  })
  client.subscriptions[sid] = state
  { client, sid, state, }
}

///|
async test "route owned binary payload by server SID" {
  let client = client_for_routing_test()
  let subscription = attach_test_subscription(client, 9)
  route_frame(
    client,
    Msg(subject="events.binary", sid=9, reply_to=Some("reply.9"), payload=[
      0x00, 0xFF,
    ]),
  )
  @debug.assert_eq(subscription.next(), {
    subject: "events.binary",
    reply_to: Some("reply.9"),
    headers: None,
    payload: [0x00, 0xFF],
  })
}

///|
async test "apply asynchronous INFO topology updates to the connection pool" {
  let client = client_for_routing_test()
  route_frame(
    client,
    Info("{\"connect_urls\":[\"127.0.0.1:4333\",\"127.0.0.1:4222\"]}"),
  )
  @debug.assert_eq(client.server_pool.addresses(), [
    "127.0.0.1:4222", "127.0.0.1:4333",
  ])
  route_frame(client, Info("{\"connect_urls\":[\"127.0.0.1:4555\"]}"))
  @debug.assert_eq(client.server_pool.addresses(), [
    "127.0.0.1:4222", "127.0.0.1:4555",
  ])
}

///|
async test "route HMSG fields without folding duplicate header names" {
  let client = client_for_routing_test()
  let subscription = attach_test_subscription(client, 4)
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
  let message = subscription.next()
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
  client.pong_actions.put(CompleteFlush(first))
  client.pong_actions.put(CompleteFlush(second))
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
      Flush(CompleteFlush(waiter)) => {
        guard client.pong_actions.try_get() is None else {
          fail("flush waiter escaped the writer ordering point")
        }
        client.pong_actions.put(CompleteFlush(waiter))
        route_frame(client, Pong)
        flush_task.wait()
      }
      Subscribe(_, _, _)
      | Flush(ForgetSubscription(_))
      | Flush(FinishSubscriptionDrain(_, _))
      | Unsubscribe(_, _)
      | DrainSubscription(_, _)
      | RetireSlowSubscription(_) => fail("expected a client flush command")
    }
  })
}

///|
async test "local subscription close preserves buffered messages" {
  let client = client_for_routing_test()
  let subscription = attach_test_subscription(client, 6)
  route_frame(
    client,
    Msg(subject="events.buffered", sid=6, reply_to=None, payload=b"last"),
  )
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
