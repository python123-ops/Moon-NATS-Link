///|
fn replay_commands_for_test(client : Client) -> Array[Bytes] {
  let commands = []
  for sid, state in client.subscriptions {
    for bytes in state.connection_commands(sid, replay=true) {
      commands.push(bytes)
    }
  }
  commands
}

///|
struct RecordingWriter {
  writes : Array[Bytes]
}

///|
impl @io.Writer for RecordingWriter with fn write_once(
  self,
  data,
  offset~,
  len~,
) {
  self.writes.push(data[offset:offset + len].to_owned())
  len
}

///|
async test "writer owns the complete queue subscription installation" {
  let client = client_for_routing_test()
  @async.with_task_group(group => {
    let task = group.spawn(() => {
      client.install_subscription("jobs.build", queue_group="workers")
    })
    match client.writer.get() {
      Subscribe(1, state, written) => {
        let intent = state.intent
        @debug.assert_eq(intent.subject, "jobs.build")
        @debug.assert_eq(intent.queue_group, Some("workers"))
        @debug.assert_eq(intent.max_messages, None)
        state.status = Accepting
        written.put(())
      }
      _ => fail("expected one writer-owned subscription command")
    }
    let subscription = task.wait()
    @debug.assert_eq(subscription.sid, 1)
  })
}

///|
async test "writer keeps request SUB and auto-unsubscribe adjacent" {
  let connection : RecordingWriter = { writes: [], }
  let commands : @aqueue.Queue[WriterCommand] = Queue(kind=Unbounded)
  let pong_actions : @aqueue.Queue[PongAction] = Queue(kind=Unbounded)
  let state = new_subscription_state(8, {
    subject: "_INBOX.MOONBIT.test.7",
    queue_group: None,
    max_messages: Some(1),
    recover_on_reconnect: false,
  })
  state.status = Installing
  let written : @aqueue.Queue[Unit] = Queue(kind=Blocking(1))
  @async.with_task_group(group => {
    let writer = group.spawn(() => {
      writer_loop(connection, commands, pong_actions)
    })
    commands.put(Subscribe(7, state, written))
    ignore(written.get())
    guard state.status is Accepting else {
      fail("expected the writer to finish subscription installation")
    }
    @debug.assert_eq(connection.writes, [
      @wire.encode_subscribe("_INBOX.MOONBIT.test.7", 7),
      @wire.encode_unsubscribe(7, max_messages=1),
    ])
    writer.cancel()
    try writer.wait() catch {
      _ => ()
    } noraise {
      _ => fail("expected writer cancellation")
    }
  })
}

///|
async test "request inbox installation retains its one message ceiling" {
  let client = client_for_routing_test()
  @async.with_task_group(group => {
    let task = group.spawn(() => {
      client.install_subscription("_INBOX.MOONBIT.test.1", max_messages=1)
    })
    match client.writer.get() {
      Subscribe(1, state, written) => {
        let intent = state.intent
        @debug.assert_eq(intent.subject, "_INBOX.MOONBIT.test.1")
        @debug.assert_eq(intent.queue_group, None)
        @debug.assert_eq(intent.max_messages, Some(1))
        state.status = Accepting
        written.put(())
      }
      _ => fail("expected one writer-owned inbox command")
    }
    ignore(task.wait())
  })
}

///|
test "replay includes only subscriptions still accepting delivery" {
  let client = client_for_routing_test()
  let active = new_subscription_state(8, {
    subject: "events.live",
    queue_group: Some("workers"),
    max_messages: None,
    recover_on_reconnect: true,
  })
  let inbox = new_subscription_state(8, {
    subject: "_INBOX.MOONBIT.test.2",
    queue_group: None,
    max_messages: Some(1),
    recover_on_reconnect: false,
  })
  let draining = new_subscription_state(8, {
    subject: "events.retiring",
    queue_group: None,
    max_messages: None,
    recover_on_reconnect: true,
  })
  draining.status = DrainingServer
  let slow = new_subscription_state(8, {
    subject: "events.slow",
    queue_group: None,
    max_messages: None,
    recover_on_reconnect: true,
  })
  slow.status = OverLimit
  client.subscriptions[3] = active
  client.subscriptions[4] = inbox
  client.subscriptions[5] = draining
  client.subscriptions[6] = slow
  @debug.assert_eq(replay_commands_for_test(client), [
    @wire.encode_subscribe("events.live", 3, queue_group="workers"),
  ])
}
