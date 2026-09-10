///|
async fn main {
  @client.with_client(
    @client.Options::new(name="moon-nats-link-event-fanout"),
    connection => {
      let wildcard = connection.subscribe("events.*")
      let exact = connection.subscribe("events.binary")
      let binary : Bytes = [0x00, 0x0D, 0x0A, 0xFF]
      connection.publish("events.binary", binary.view())
      let wildcard_copy = wildcard.next()
      let exact_copy = exact.next()
      @debug.assert_eq(wildcard_copy.subject, "events.binary")
      @debug.assert_eq(wildcard_copy.payload, binary)
      @debug.assert_eq(exact_copy, wildcard_copy)
      println("event fanout ok: 00 0d 0a ff reached both subscriptions")
    },
  )
}
