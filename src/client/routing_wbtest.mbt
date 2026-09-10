///|
fn client_for_routing_test() -> Client {
  let writer : @aqueue.Queue[WriterCommand] = Queue(kind=Unbounded)
  { writer, subscriptions: {}, next_sid: 1, closing: false, }
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
    payload: [0x00, 0xFF],
  })
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
