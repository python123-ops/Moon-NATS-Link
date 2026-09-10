///|
/// Errors defined at the Core NATS client boundary.
pub(all) suberror ClientError {
  InvalidSubject(String)
  InvalidQueueGroup(String)
  ProtocolViolation(String)
  ConnectionClosed
  ServerRejected(String)
} derive(Debug)

///|
/// Connection values used by `with_client` in the current native client.
pub struct Options {
  priv server : String
  priv name : String
}

///|
/// Create connection options for a single Core NATS server.
pub fn Options::new(
  server? : String = "127.0.0.1:4222",
  name? : String = "moon-nats-link",
) -> Options {
  { server, name, }
}

///|
/// A Core NATS message. Payload ownership is transferred out of the wire
/// decoder, so its bytes remain valid after the next socket read.
pub(all) struct Message {
  subject : String
  reply_to : String?
  payload : Bytes
} derive(Eq, Debug)

///|
priv enum WriterCommand {
  Write(Bytes, @aqueue.Queue[Unit]?)
}

///|
/// A live native client managed by `with_client`.
pub struct Client {
  priv writer : @aqueue.Queue[WriterCommand]
  priv subscriptions : Map[Int, @aqueue.Queue[Message]]
  priv mut next_sid : Int
  priv mut closing : Bool
}

///|
/// A subscription mailbox associated with one server-side SID.
pub struct Subscription {
  priv inbox : @aqueue.Queue[Message]
}

///|
/// Wait for the next message routed to this subscription.
pub async fn Subscription::next(self : Subscription) -> Message {
  self.inbox.get()
}
