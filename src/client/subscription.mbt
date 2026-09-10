///|
/// Wait for the next buffered or newly delivered message. After unsubscribe,
/// already buffered messages remain readable before `SubscriptionClosed`.
pub async fn Subscription::next(self : Subscription) -> Message {
  self.inbox.get()
}

///|
fn Subscription::close_local(self : Subscription) -> Unit {
  if !self.closed {
    self.closed = true
    self.client.subscriptions.remove(self.sid)
    self.inbox.close(error=SubscriptionClosed)
  }
}

///|
/// Stop server delivery, cross a PING/PONG processing barrier, then detach the
/// local SID. Messages buffered before the barrier remain available to `next`.
pub async fn Subscription::unsubscribe(self : Subscription) -> Unit {
  if self.closed {
    raise SubscriptionClosed
  }
  self.closed = true
  send(self.client, @wire.encode_unsubscribe(self.sid))
  self.client.flush()
  self.client.subscriptions.remove(self.sid)
  self.inbox.close(error=SubscriptionClosed)
}
