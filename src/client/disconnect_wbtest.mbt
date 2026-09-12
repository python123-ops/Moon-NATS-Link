///|
suberror ForcedWriteFailure

///|
struct FailingWriter {}

///|
impl @io.Writer for FailingWriter with fn write_once(
  _self,
  _data,
  offset~,
  len~,
) {
  let _ = (offset, len)
  raise ForcedWriteFailure
}

///|
async test "disconnect wakes a publish waiting on the old writer" {
  let client = client_for_routing_test()
  @async.with_task_group(group => {
    let publish = group.spawn(allow_failure=true, () => {
      client.publish("events.pending", b"payload")
    })
    match client.writer.get() {
      Write(_, Some(_)) => ()
      _ => fail("expected a publish completion waiter")
    }
    client.mark_disconnected()
    try publish.wait() catch {
      Disconnected => ()
      _ => fail("expected the pending publish to observe disconnection")
    } noraise {
      _ => fail("expected the pending publish to fail")
    }
    @debug.assert_eq(client.connection_waiters.length(), 0)
  })
}

///|
async test "writer failure marks the connection before releasing its caller" {
  let client = client_for_routing_test()
  let connection = FailingWriter::{ }
  @async.with_task_group(group => {
    let writer = group.spawn(allow_failure=true, () => {
      connection_writer_loop(connection, client)
    })
    let publish = group.spawn(allow_failure=true, () => {
      client.publish("events.write-failure", b"payload")
    })
    try writer.wait() catch {
      ForcedWriteFailure => ()
      _ => fail("expected the transport write failure")
    } noraise {
      _ => fail("expected the writer to fail")
    }
    try publish.wait() catch {
      Disconnected => ()
      _ => fail("expected the publish waiter to be released")
    } noraise {
      _ => fail("expected the publish to fail")
    }
    guard client.status is Reconnecting else {
      fail("expected writer failure to enter reconnecting state")
    }
  })
}

///|
async test "disconnect removes a subscription whose SUB was not written" {
  let client = client_for_routing_test()
  @async.with_task_group(group => {
    let installing = group.spawn(allow_failure=true, () => {
      client.subscribe("events.installing")
    })
    match client.writer.get() {
      Subscribe(1, state, _) => {
        guard state.status is Installing else {
          fail("expected the SID to remain installing")
        }
      }
      _ => fail("expected a subscription installation command")
    }
    client.mark_disconnected()
    try installing.wait() catch {
      Disconnected => ()
      _ => fail("expected the installing subscription to be interrupted")
    } noraise {
      _ => fail("expected the installing subscription to fail")
    }
    guard client.subscriptions.get(1) is None else {
      fail("expected no unowned SID to survive")
    }
  })
}

///|
async test "disconnect retains ordinary subscriptions and drops request inboxes" {
  let client = client_for_routing_test()
  let durable = attach_test_subscription(client, 7)
  let request_state = new_subscription_state(8, {
    subject: "_INBOX.MOONBIT.test.8",
    queue_group: None,
    max_messages: Some(1),
    recover_on_reconnect: false,
  })
  client.subscriptions[8] = request_state
  client.mark_disconnected()
  guard client.status is Reconnecting else {
    fail("expected the reconnecting state")
  }
  guard client.subscriptions.get(7) is Some(_) else {
    fail("expected the ordinary subscription to survive")
  }
  guard client.subscriptions.get(8) is None else {
    fail("expected the request inbox to be retired")
  }
  @debug.assert_eq(durable.state.connection_commands(7, replay=true), [
    @wire.encode_subscribe("test.7", 7),
  ])
  try request_state.inbox.get() catch {
    Disconnected => ()
    _ => fail("expected the request inbox to observe disconnection")
  } noraise {
    _ => fail("expected the request inbox to close")
  }
}

///|
async test "operations reject the reconnecting interval without queueing" {
  let client = client_for_routing_test()
  client.mark_disconnected()
  let operations : Array[async () -> Unit] = [
    () => client.publish("events.late", b"payload"),
    () => ignore(client.subscribe("events.late")),
    () => client.flush(),
    () => ignore(client.request("rpc.late", b"payload")),
    () => client.drain(),
  ]
  for operation in operations {
    try operation() catch {
      Disconnected => ()
      _ => fail("expected Disconnected during reconnect")
    } noraise {
      _ => fail("expected the operation to fail")
    }
  }
  guard client.writer.try_get() is None else {
    fail("expected no command to enter the inactive writer")
  }
}

///|
async test "disconnect aborts a subscription drain without replaying its SID" {
  let client = client_for_routing_test()
  let subscription = attach_test_subscription(client, 9)
  @async.with_task_group(group => {
    let draining = group.spawn(allow_failure=true, () => subscription.drain())
    match client.writer.get() {
      DrainSubscription(9, _) => ()
      _ => fail("expected a subscription drain command")
    }
    client.mark_disconnected()
    try draining.wait() catch {
      Disconnected => ()
      _ => fail("expected the drain to observe disconnection")
    } noraise {
      _ => fail("expected the drain to fail")
    }
    guard client.subscriptions.get(9) is None else {
      fail("expected the interrupted drain SID to be retired")
    }
    @debug.assert_eq(subscription.state.connection_commands(9, replay=true), [])
  })
}
