///|
async test "subscription drain keeps frames accepted before its PONG" {
  let client = client_for_routing_test()
  let subscription = attach_test_subscription(client, 7)
  @async.with_task_group(group => {
    let drain_task = group.spawn(() => subscription.drain())
    match client.writer.get() {
      DrainSubscription(7, state) => {
        guard physical_equal(state, subscription.state) else {
          fail("expected the subscription state in its drain command")
        }
        route_frame(
          client,
          Msg(
            subject="events.drain",
            sid=7,
            reply_to=None,
            payload=b"in-flight",
          ),
        )
        client.pong_actions.put(FinishSubscriptionDrain(7, state))
        route_frame(client, Pong)
      }
      _ => fail("expected an atomic subscription drain command")
    }
    @debug.assert_eq(subscription.next().payload, b"in-flight")
    try subscription.next() catch {
      SubscriptionClosed => ()
      _ => fail("expected the consumer to observe drain completion")
    } noraise {
      _ => fail("expected the drained mailbox to close")
    }
    drain_task.wait()
    guard client.subscriptions.get(7) is None else {
      fail("expected the drained SID to leave the router")
    }
    try subscription.next() catch {
      SubscriptionClosed => ()
      _ => fail("expected the drained mailbox to close")
    } noraise {
      _ => fail("expected the drained mailbox to be exhausted")
    }
  })
}

///|
async test "unsubscribe stops local delivery before its retirement PONG" {
  let client = client_for_routing_test()
  let subscription = attach_test_subscription(client, 8)
  route_frame(
    client,
    Msg(subject="events.unsubscribe", sid=8, reply_to=None, payload=b"buffered"),
  )
  @async.with_task_group(group => {
    let unsubscribe_task = group.spawn(() => subscription.unsubscribe())
    match client.writer.get() {
      Unsubscribe(8, written) => {
        client.pong_actions.put(ForgetSubscription(8))
        written.put(())
      }
      _ => fail("expected the writer-owned unsubscribe command")
    }
    unsubscribe_task.wait()
    route_frame(
      client,
      Msg(
        subject="events.unsubscribe",
        sid=8,
        reply_to=None,
        payload=b"ignored-after-unsubscribe",
      ),
    )
    @debug.assert_eq(subscription.next().payload, b"buffered")
    try subscription.next() catch {
      SubscriptionClosed => ()
      _ => fail("expected the unsubscribed mailbox to close")
    } noraise {
      _ => fail("expected no delivery after unsubscribe")
    }
    route_frame(client, Pong)
    guard client.subscriptions.get(8) is None else {
      fail("expected PONG to retire the unsubscribed SID")
    }
  })
}

///|
async test "client drain closes subscriptions before its publish barrier" {
  let client = client_for_routing_test()
  let subscription = attach_test_subscription(client, 11)
  @async.with_task_group(group => {
    let drain_task = group.spawn(() => client.drain())
    match client.writer.get() {
      Write(bytes, None) =>
        @debug.assert_eq(bytes, @wire.encode_unsubscribe(11))
      _ => fail("expected a batched subscription UNSUB")
    }
    match client.writer.get() {
      Flush(action) => {
        route_frame(
          client,
          Msg(
            subject="rpc.work",
            sid=11,
            reply_to=None,
            payload=b"accepted-before-pong",
          ),
        )
        client.pong_actions.put(action)
        route_frame(client, Pong)
      }
      _ => fail("expected the subscription drain barrier")
    }
    @debug.assert_eq(subscription.next().payload, b"accepted-before-pong")
    try subscription.next() catch {
      SubscriptionClosed => ()
      _ => fail("expected the consumer to finish the drained mailbox")
    } noraise {
      _ => fail("expected the drained mailbox to close")
    }
    match client.writer.get() {
      Flush(action) => {
        guard client.status is DrainingPublishes else {
          fail("expected publishes to close after subscription mailboxes drain")
        }
        client.pong_actions.put(action)
        route_frame(client, Pong)
      }
      _ => fail("expected the final publish barrier")
    }
    drain_task.wait()
    guard client.status is Closed else {
      fail("expected the drained client to close")
    }
    try client.publish("events.closed", b"late") catch {
      ConnectionClosed => ()
      _ => fail("expected ConnectionClosed after client drain")
    } noraise {
      _ => fail("expected publish after drain to fail")
    }
  })
}

///|
async test "scope close releases subscription readers" {
  let client = client_for_routing_test()
  let subscription = attach_test_subscription(client, 13)
  client.close_local()
  route_frame(
    client,
    Msg(subject="events.after-scope", sid=13, reply_to=None, payload=b"ignored"),
  )
  try subscription.next() catch {
    SubscriptionClosed => ()
    _ => fail("expected scope close to terminate the mailbox")
  } noraise {
    _ => fail("expected an escaped subscription reader to stop")
  }
}

///|
async test "subscription drain protocol survives waiter cancellation" {
  let client = client_for_routing_test()
  let subscription = attach_test_subscription(client, 14)
  @async.with_task_group(group => {
    let drain_task = group.spawn(() => subscription.drain())
    match client.writer.get() {
      DrainSubscription(14, state) => {
        drain_task.cancel()
        try drain_task.wait() catch {
          _ => ()
        } noraise {
          _ => fail("expected the drain waiter to be cancelled")
        }
        client.pong_actions.put(FinishSubscriptionDrain(14, state))
        route_frame(client, Pong)
      }
      _ => fail("expected the writer-owned drain command")
    }
    guard subscription.state.status is Drained else {
      fail("expected PONG to finish the cancelled caller's drain")
    }
    guard client.subscriptions.get(14) is None else {
      fail("expected PONG to retire the SID after caller cancellation")
    }
  })
}

///|
async test "unsubscribe protocol survives waiter cancellation" {
  let client = client_for_routing_test()
  let subscription = attach_test_subscription(client, 15)
  @async.with_task_group(group => {
    let unsubscribe_task = group.spawn(() => subscription.unsubscribe())
    match client.writer.get() {
      Unsubscribe(15, written) => {
        unsubscribe_task.cancel()
        try unsubscribe_task.wait() catch {
          _ => ()
        } noraise {
          _ => fail("expected the unsubscribe waiter to be cancelled")
        }
        client.pong_actions.put(ForgetSubscription(15))
        written.put(())
        route_frame(client, Pong)
      }
      _ => fail("expected the writer-owned unsubscribe command")
    }
    guard client.subscriptions.get(15) is None else {
      fail("expected PONG to retire the SID after caller cancellation")
    }
    try subscription.next() catch {
      SubscriptionClosed => ()
      _ => fail("expected unsubscribe to close the mailbox before cancellation")
    } noraise {
      _ => fail("expected the cancelled unsubscribe mailbox to stay closed")
    }
  })
}
