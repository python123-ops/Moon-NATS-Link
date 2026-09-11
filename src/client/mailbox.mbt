///|
let default_subscription_message_limit : Int = 1024

///|
let default_subscription_byte_limit : Int = 8 * 1024 * 1024

///|
priv struct PendingMessage {
  message : Message
  wire_bytes : Int
}

///|
priv enum SubscriptionStatus {
  Accepting
  DrainingServer
  DrainingBuffered
  Drained
  Detached
  OverLimit
}

///|
priv struct SubscriptionState {
  inbox : @aqueue.Queue[PendingMessage]
  mut pending_messages : Int
  mut pending_bytes : Int
  mut status : SubscriptionStatus
  drain_waiters : Array[@aqueue.Queue[Unit]]
}

///|
fn new_subscription_state(message_limit : Int) -> SubscriptionState {
  {
    inbox: Queue(kind=Blocking(message_limit)),
    pending_messages: 0,
    pending_bytes: 0,
    status: Accepting,
    drain_waiters: [],
  }
}
