///|
/// Wait for the next buffered or newly delivered message. A closed mailbox
/// yields its buffered messages before `SubscriptionClosed` or `SlowConsumer`.
pub async fn Subscription::next(self : Subscription) -> Message {
  let pending = self.state.inbox.get()
  self.state.pending_messages -= 1
  self.state.pending_bytes -= pending.wire_bytes
  pending.message
}

///|
fn Subscription::close_local(self : Subscription) -> Unit {
  if self.state.status is Accepting {
    self.state.status = Detached
    self.client.subscriptions.remove(self.sid)
    self.state.inbox.close(error=SubscriptionClosed)
  }
}

///|
/// Stop server delivery, cross a PING/PONG processing barrier, then detach the
/// local SID. Messages buffered before the barrier remain available to `next`.
pub async fn Subscription::unsubscribe(self : Subscription) -> Unit {
  match self.state.status {
    OverLimit => raise SlowConsumer
    Detached => raise SubscriptionClosed
    Accepting => ()
  }
  self.state.status = Detached
  send(self.client, @wire.encode_unsubscribe(self.sid))
  self.client.flush()
  self.client.subscriptions.remove(self.sid)
  self.state.inbox.close(error=SubscriptionClosed)
}
