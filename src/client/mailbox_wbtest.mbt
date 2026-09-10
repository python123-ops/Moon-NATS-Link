///|
async test "message quota stops one slow SID without blocking another" {
  let client = client_for_routing_test(
    subscription_message_limit=2,
    subscription_byte_limit=64,
  )
  let slow = attach_test_subscription(client, 9)
  let healthy = attach_test_subscription(client, 10)
  route_frame(
    client,
    Msg(subject="events.slow", sid=9, reply_to=None, payload=b"first"),
  )
  route_frame(
    client,
    Msg(subject="events.slow", sid=9, reply_to=None, payload=b"second"),
  )
  route_frame(
    client,
    Msg(subject="events.slow", sid=9, reply_to=None, payload=b"overflow"),
  )
  route_frame(
    client,
    Msg(subject="events.live", sid=10, reply_to=None, payload=b"delivered"),
  )
  @debug.assert_eq(healthy.next().payload, b"delivered")
  match client.writer.try_get() {
    Some(RetireSlowSubscription(9)) => ()
    _ => fail("expected the slow SID to be retired")
  }
  try slow.unsubscribe() catch {
    SlowConsumer => ()
    _ => fail("expected unsubscribe to preserve the slow-consumer cause")
  } noraise {
    _ => fail("expected the over-limit subscription to stay terminated")
  }
  @debug.assert_eq(slow.next().payload, b"first")
  @debug.assert_eq(slow.next().payload, b"second")
  try slow.next() catch {
    SlowConsumer => ()
    _ => fail("expected SlowConsumer after buffered messages")
  } noraise {
    _ => fail("expected the slow mailbox to close")
  }
}

///|
async test "header bytes count toward the subscription byte quota" {
  let client = client_for_routing_test(
    subscription_message_limit=8,
    subscription_byte_limit=11,
  )
  let subscription = attach_test_subscription(client, 4)
  route_frame(
    client,
    HMsg(
      subject="events.headers",
      sid=4,
      reply_to=None,
      headers=b"NATS/1.0\r\n\r\n",
      payload=b"",
    ),
  )
  try subscription.next() catch {
    SlowConsumer => ()
    _ => fail("expected header bytes to exceed the byte quota")
  } noraise {
    _ => fail("expected the oversized message to be rejected")
  }
}

///|
async test "reading a message returns both mailbox quotas" {
  let client = client_for_routing_test(
    subscription_message_limit=1,
    subscription_byte_limit=4,
  )
  let subscription = attach_test_subscription(client, 5)
  route_frame(
    client,
    Msg(subject="events.reuse", sid=5, reply_to=None, payload=b"1234"),
  )
  @debug.assert_eq(subscription.next().payload, b"1234")
  route_frame(
    client,
    Msg(subject="events.reuse", sid=5, reply_to=None, payload=b"next"),
  )
  @debug.assert_eq(subscription.next().payload, b"next")
}

///|
async test "PONG retires a slow SID after the server barrier" {
  let client = client_for_routing_test()
  ignore(attach_test_subscription(client, 12))
  client.pong_actions.put(ForgetSubscription(12))
  route_frame(client, Pong)
  guard client.subscriptions.get(12) is None else {
    fail("expected PONG to forget the retired SID")
  }
}
