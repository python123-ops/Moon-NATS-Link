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
priv struct SubscriptionIntent {
  subject : String
  queue_group : String?
  max_messages : Int?
}

///|
fn SubscriptionState::replay_commands(
  self : SubscriptionState,
  sid : Int,
) -> Array[Bytes] {
  guard self.status is Accepting else { return [] }
  let queue_group = self.intent.queue_group
  let commands = [
    @wire.encode_subscribe(self.intent.subject, sid, queue_group?),
  ]
  if self.intent.max_messages is Some(limit) {
    commands.push(@wire.encode_unsubscribe(sid, max_messages=limit))
  }
  commands
}

///|
priv struct SubscriptionState {
  intent : SubscriptionIntent
  inbox : @aqueue.Queue[PendingMessage]
  mut pending_messages : Int
  mut pending_bytes : Int
  mut status : SubscriptionStatus
  drain_waiters : Array[@aqueue.Queue[Unit]]
}

///|
fn new_subscription_state(
  message_limit : Int,
  intent : SubscriptionIntent,
) -> SubscriptionState {
  {
    intent,
    inbox: Queue(kind=Blocking(message_limit)),
    pending_messages: 0,
    pending_bytes: 0,
    status: Accepting,
    drain_waiters: [],
  }
}
