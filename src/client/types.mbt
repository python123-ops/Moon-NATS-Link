///|
/// Errors defined at the Core NATS client boundary.
pub(all) suberror ClientError {
  InvalidSubject(String)
  InvalidQueueGroup(String)
  InvalidHeader(String)
  InvalidTimeout(Int)
  ProtocolViolation(String)
  ConnectionClosed
  ServerRejected(String)
  NoResponders
  Timeout
  SlowConsumer
  SubscriptionClosed
  InboxEntropyUnavailable
} derive(Debug)

///|
/// Connection values used by `with_client` in the current native client.
pub struct Options {
  priv server : String
  priv name : String
  priv request_timeout_ms : Int
}

///|
/// Create connection options for a single Core NATS server.
pub fn Options::new(
  server? : String = "127.0.0.1:4222",
  name? : String = "moon-nats-link",
  request_timeout_ms? : Int = 2000,
) -> Options raise ClientError {
  if request_timeout_ms <= 0 {
    raise InvalidTimeout(request_timeout_ms)
  }
  { server, name, request_timeout_ms, }
}

///|
/// A Core NATS message. Payload ownership is transferred out of the wire
/// decoder, so its bytes remain valid after the next socket read.
pub(all) struct Message {
  subject : String
  reply_to : String?
  headers : Headers?
  payload : Bytes
} derive(Eq, Debug)

///|
priv enum WriterCommand {
  Write(Bytes, @aqueue.Queue[Unit]?)
  Flush(PongAction)
  RetireSlowSubscription(Int)
}

///|
priv enum PongAction {
  CompleteFlush(@aqueue.Queue[Unit])
  ForgetSubscription(Int)
}

///|
/// A live native client managed by `with_client`.
pub struct Client {
  priv writer : @aqueue.Queue[WriterCommand]
  priv subscriptions : Map[Int, SubscriptionState]
  priv pong_actions : @aqueue.Queue[PongAction]
  priv request_timeout_ms : Int
  priv subscription_message_limit : Int
  priv subscription_byte_limit : Int
  priv inbox_prefix : String
  priv mut next_sid : Int
  priv mut next_request : Int
  priv mut closing : Bool
}

///|
/// A subscription mailbox associated with one server-side SID.
pub struct Subscription {
  priv client : Client
  priv sid : Int
  priv state : SubscriptionState
}
