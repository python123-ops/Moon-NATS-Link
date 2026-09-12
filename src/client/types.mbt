///|
/// Errors defined at the Core NATS client boundary.
pub(all) suberror ClientError {
  InvalidSubject(String)
  InvalidQueueGroup(String)
  InvalidHeader(String)
  InvalidTimeout(Int)
  InvalidCredentials(String)
  InvalidServerList(String)
  InvalidTlsConfiguration(String)
  ProtocolViolation(String)
  ConnectionClosed
  AuthenticationFailed
  TlsHandshakeFailed(String)
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
  priv server_pool : ServerPool
  priv name : String
  priv request_timeout_ms : Int
  priv auth : @wire.ConnectAuth?
  priv tls : TlsOptions?
}

///|
/// Create connection options for one Core NATS server or an ordered pool.
/// `token` is mutually exclusive with `username` and `password`; the latter
/// must be supplied as a pair. `server` preserves the single-address call
/// shape and cannot be combined with `servers`.
pub fn Options::new(
  server? : String,
  servers? : Array[String],
  name? : String = "moon-nats-link",
  request_timeout_ms? : Int = 2000,
  token? : String,
  username? : String,
  password? : String,
  tls? : TlsOptions,
) -> Options raise ClientError {
  if request_timeout_ms <= 0 {
    raise InvalidTimeout(request_timeout_ms)
  }
  let server_pool = ServerPool::new(server, servers)
  let auth = validate_credentials(token, username, password)
  { server_pool, name, request_timeout_ms, auth, tls, }
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
  Subscribe(Int, SubscriptionState, @aqueue.Queue[Unit])
  Flush(PongAction)
  Unsubscribe(Int, @aqueue.Queue[Unit])
  DrainSubscription(Int, SubscriptionState)
  RetireSlowSubscription(Int)
}

///|
priv enum PongAction {
  CompleteFlush(@aqueue.Queue[Unit])
  ForgetSubscription(Int)
  FinishSubscriptionDrain(Int, SubscriptionState)
}

///|
priv enum ClientStatus {
  Open
  DrainingSubscriptions
  DrainingPublishes
  Closed
}

///|
/// A live native client managed by `with_client`.
pub struct Client {
  priv server_pool : ServerPool
  priv writer : @aqueue.Queue[WriterCommand]
  priv subscriptions : Map[Int, SubscriptionState]
  priv pong_actions : @aqueue.Queue[PongAction]
  priv request_timeout_ms : Int
  priv subscription_message_limit : Int
  priv subscription_byte_limit : Int
  priv inbox_prefix : String
  priv mut next_sid : Int
  priv mut next_request : Int
  priv mut status : ClientStatus
}

///|
/// A subscription mailbox associated with one server-side SID.
pub struct Subscription {
  priv client : Client
  priv sid : Int
  priv state : SubscriptionState
}
