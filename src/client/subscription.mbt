///|
/// Wait for the next buffered or newly delivered message. A closed mailbox
/// yields its buffered messages before `SubscriptionClosed` or `SlowConsumer`.
pub async fn Subscription::next(self : Subscription) -> Message {
  let pending = self.state.inbox.get() catch {
    SubscriptionClosed => {
      finish_buffered_drain(self.state)
      raise SubscriptionClosed
    }
    error => raise error
  }
  self.state.pending_messages -= 1
  self.state.pending_bytes -= pending.wire_bytes
  pending.message
}

///|
async fn finish_buffered_drain(state : SubscriptionState) -> Unit {
  if state.status is DrainingBuffered && state.pending_messages == 0 {
    state.status = Drained
    for waiter in state.drain_waiters {
      waiter.put(())
    }
    state.drain_waiters.clear()
  }
}

///|
fn fail_drain_waiters(state : SubscriptionState, error : Error) -> Unit {
  for waiter in state.drain_waiters {
    waiter.close(error~)
  }
  state.drain_waiters.clear()
}

///|
fn begin_subscription_drain(
  state : SubscriptionState,
) -> (@aqueue.Queue[Unit], Bool) raise ClientError {
  match state.status {
    Installing | Accepting => {
      state.status = DrainingServer
      let waiter : @aqueue.Queue[Unit] = Queue(kind=Blocking(1))
      state.drain_waiters.push(waiter)
      (waiter, true)
    }
    DrainingServer | DrainingBuffered => {
      let waiter : @aqueue.Queue[Unit] = Queue(kind=Blocking(1))
      state.drain_waiters.push(waiter)
      (waiter, false)
    }
    Drained | Detached => raise SubscriptionClosed
    OverLimit => raise SlowConsumer
  }
}

///|
async fn finish_server_drain(
  client : Client,
  sid : Int,
  state : SubscriptionState,
) -> Unit {
  if state.status is DrainingServer {
    client.subscriptions.remove(sid)
    state.status = DrainingBuffered
    state.inbox.close(error=SubscriptionClosed)
    finish_buffered_drain(state)
  }
}

///|
fn Subscription::close_local(self : Subscription) -> Unit {
  match self.state.status {
    Installing => {
      self.state.status = Detached
      self.client.subscriptions.remove(self.sid)
      self.state.inbox.close(error=SubscriptionClosed)
    }
    Accepting => {
      self.state.status = Detached
      self.client.subscriptions.remove(self.sid)
      self.state.inbox.close(error=SubscriptionClosed)
    }
    DrainingServer | DrainingBuffered => {
      self.state.status = Detached
      self.client.subscriptions.remove(self.sid)
      self.state.inbox.close(error=SubscriptionClosed)
      fail_drain_waiters(self.state, SubscriptionClosed)
    }
    Drained | Detached | OverLimit => ()
  }
}

///|
/// Stop local delivery after the UNSUB write. The SID stays in the router until
/// a later PONG proves the server has processed that command.
pub async fn Subscription::unsubscribe(self : Subscription) -> Unit {
  ensure_subscription_control_allowed(self.client)
  match self.state.status {
    Installing => raise SubscriptionClosed
    OverLimit => raise SlowConsumer
    Drained | Detached | DrainingServer | DrainingBuffered =>
      raise SubscriptionClosed
    Accepting => ()
  }
  self.state.status = Detached
  self.state.inbox.close(error=SubscriptionClosed)
  let (waiter_id, written) = self.client.new_connection_waiter()
  defer self.client.connection_waiters.remove(waiter_id)
  self.client.writer.put(Unsubscribe(self.sid, written))
  ignore(written.get())
}

///|
/// Stop server delivery at a PING/PONG barrier and wait until every message
/// accepted before that barrier has been removed from this subscription's
/// mailbox. A concurrent consumer should keep calling `next` through the final
/// `SubscriptionClosed`, which marks its previously returned message handled.
pub async fn Subscription::drain(self : Subscription) -> Unit {
  ensure_subscription_control_allowed(self.client)
  let (waiter, owns_protocol) = begin_subscription_drain(self.state)
  if owns_protocol {
    self.client.writer.put(DrainSubscription(self.sid, self.state))
  }
  ignore(waiter.get())
}
