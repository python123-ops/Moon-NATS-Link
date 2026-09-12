///|
async fn[Connection : @io.Reader + @io.Writer] read_server_info(
  connection : Connection,
  decoder : @wire.Decoder,
) -> Unit {
  let mut received_info = false
  while !received_info {
    match @io.Reader::read_some(connection) {
      None => raise ConnectionClosed
      Some(chunk) =>
        match decoder.feed(chunk) {
          Err(error) => raise ProtocolViolation(@debug.to_string(error))
          Ok(frames) =>
            for frame in frames {
              match frame {
                Info(_) => received_info = true
                Ping => @io.Writer::write(connection, @wire.encode_pong())
                Error(message) => raise ServerRejected(message)
                Ok | Pong => ()
                Msg(..) | HMsg(..) =>
                  raise ProtocolViolation("received MSG before CONNECT")
              }
            }
        }
    }
  }
}

///|
async fn[Connection : @io.Reader + @io.Writer] finish_handshake(
  connection : Connection,
  decoder : @wire.Decoder,
  options : Options,
) -> Unit {
  let auth = options.auth
  @io.Writer::write(
    connection,
    @wire.encode_connect(options.name, headers=true, no_responders=true, auth?),
  )
  @io.Writer::write(connection, @wire.encode_ping())
  let mut received_pong = false
  while !received_pong {
    match @io.Reader::read_some(connection) {
      None => raise ConnectionClosed
      Some(chunk) =>
        match decoder.feed(chunk) {
          Err(error) => raise ProtocolViolation(@debug.to_string(error))
          Ok(frames) =>
            for frame in frames {
              match frame {
                Pong => received_pong = true
                Ping => @io.Writer::write(connection, @wire.encode_pong())
                Error(message) => raise handshake_error(message)
                Info(_) | Ok => ()
                Msg(..) | HMsg(..) =>
                  raise ProtocolViolation(
                    "received MSG before subscriptions exist",
                  )
              }
            }
        }
    }
  }
}

///|
async fn[Connection : @io.Writer] writer_loop(
  connection : Connection,
  commands : @aqueue.Queue[WriterCommand],
  pong_actions : @aqueue.Queue[PongAction],
) -> Unit {
  for ;; {
    match commands.get() {
      Write(bytes, completion) => {
        connection.write(bytes)
        if completion is Some(done) {
          done.put(())
        }
      }
      Flush(action) => {
        pong_actions.put(action)
        connection.write(@wire.encode_ping())
      }
      Unsubscribe(sid, written) => {
        connection.write(@wire.encode_unsubscribe(sid))
        pong_actions.put(ForgetSubscription(sid))
        connection.write(@wire.encode_ping())
        written.put(())
      }
      DrainSubscription(sid, state) => {
        connection.write(@wire.encode_unsubscribe(sid))
        pong_actions.put(FinishSubscriptionDrain(sid, state))
        connection.write(@wire.encode_ping())
      }
      RetireSlowSubscription(sid) => {
        connection.write(@wire.encode_unsubscribe(sid))
        pong_actions.put(ForgetSubscription(sid))
        connection.write(@wire.encode_ping())
      }
    }
  }
}

///|
async fn send(client : Client, bytes : Bytes) -> Unit {
  let completion : @aqueue.Queue[Unit] = Queue(kind=Blocking(1))
  client.writer.put(Write(bytes, Some(completion)))
  ignore(completion.get())
}

///|
fn ensure_subscription_control_allowed(
  client : Client,
) -> Unit raise ClientError {
  match client.status {
    Open | DrainingSubscriptions => ()
    DrainingPublishes | Closed => raise ConnectionClosed
  }
}

///|
fn ensure_publish_allowed(client : Client) -> Unit raise ClientError {
  match client.status {
    Open | DrainingSubscriptions => ()
    DrainingPublishes | Closed => raise ConnectionClosed
  }
}

///|
async fn flush_barrier(client : Client) -> Unit {
  let waiter : @aqueue.Queue[Unit] = Queue(kind=Blocking(1))
  client.writer.put(Flush(CompleteFlush(waiter)))
  ignore(waiter.get())
}

///|
async fn route_frame(client : Client, frame : @wire.ServerFrame) -> Unit {
  if client.status is Closed {
    return
  }
  match frame {
    Msg(subject~, sid~, reply_to~, payload~) =>
      route_message(
        client,
        subject,
        sid,
        reply_to,
        None,
        payload,
        payload.length(),
      )
    HMsg(subject~, sid~, reply_to~, headers=raw_headers, payload~) =>
      route_message(
        client,
        subject,
        sid,
        reply_to,
        Some(decode_headers(raw_headers.view())),
        payload,
        raw_headers.length() + payload.length(),
      )
    Ping => client.writer.put(Write(@wire.encode_pong(), None))
    Pong =>
      match client.pong_actions.try_get() {
        None => ()
        Some(CompleteFlush(waiter)) => waiter.put(())
        Some(ForgetSubscription(sid)) => client.subscriptions.remove(sid)
        Some(FinishSubscriptionDrain(sid, state)) =>
          finish_server_drain(client, sid, state)
      }
    Error(message) => raise ServerRejected(message)
    Info(_) | Ok => ()
  }
}

///|
async fn stop_slow_subscription(
  client : Client,
  sid : Int,
  state : SubscriptionState,
) -> Unit {
  state.status = OverLimit
  state.inbox.close(error=SlowConsumer)
  fail_drain_waiters(state, SlowConsumer)
  client.writer.put(RetireSlowSubscription(sid))
}

///|
async fn route_message(
  client : Client,
  subject : String,
  sid : Int,
  reply_to : String?,
  headers : Headers?,
  payload : Bytes,
  wire_bytes : Int,
) -> Unit {
  match client.subscriptions.get(sid) {
    None =>
      raise ProtocolViolation(
        "server delivered an unknown subscription id \{sid}",
      )
    Some(state) =>
      if state.status is (Accepting | DrainingServer) {
        let exceeds_messages = state.pending_messages >=
          client.subscription_message_limit
        let exceeds_bytes = wire_bytes > client.subscription_byte_limit ||
          state.pending_bytes > client.subscription_byte_limit - wire_bytes
        if exceeds_messages || exceeds_bytes {
          stop_slow_subscription(client, sid, state)
        } else {
          let pending : PendingMessage = {
            message: { subject, reply_to, headers, payload, },
            wire_bytes,
          }
          if state.inbox.try_put(pending) {
            state.pending_messages += 1
            state.pending_bytes += wire_bytes
          } else {
            stop_slow_subscription(client, sid, state)
          }
        }
      }
  }
}

///|
async fn[Connection : @io.Reader] reader_loop(
  connection : Connection,
  decoder : @wire.Decoder,
  client : Client,
) -> Unit {
  for ;; {
    let chunk = connection.read_some() catch {
      error => if client.status is Closed { None } else { raise error }
    }
    match chunk {
      None => {
        if client.status is Closed {
          return
        }
        match decoder.finish() {
          Err(error) => raise ProtocolViolation(@debug.to_string(error))
          Ok(_) => raise ConnectionClosed
        }
      }
      Some(bytes) =>
        match decoder.feed(bytes) {
          Err(error) => raise ProtocolViolation(@debug.to_string(error))
          Ok(frames) =>
            for frame in frames {
              route_frame(client, frame)
            }
        }
    }
  }
}

///|
/// Publish raw bytes to a concrete subject.
pub async fn Client::publish(
  self : Client,
  subject : String,
  payload : BytesView,
  reply_to? : String,
  headers? : Headers,
) -> Unit {
  ensure_publish_allowed(self)
  validate_publish_subject(subject)
  if reply_to is Some(reply) {
    validate_publish_subject(reply)
  }
  let bytes = match headers {
    None => @wire.encode_publish(subject, payload, reply_to?)
    Some(values) =>
      @wire.encode_header_publish(
        subject,
        encode_headers(values).view(),
        payload,
        reply_to?,
      )
  }
  send(self, bytes)
}

///|
/// Wait until the server has processed every command written before this
/// call's PING.
pub async fn Client::flush(self : Client) -> Unit {
  match self.status {
    Open | DrainingSubscriptions => flush_barrier(self)
    DrainingPublishes | Closed => raise ConnectionClosed
  }
}

///|
/// Create a subscription and wait until its SUB command reaches the writer.
pub async fn Client::subscribe(
  self : Client,
  subject : String,
  queue_group? : String,
) -> Subscription {
  guard self.status is Open else { raise ConnectionClosed }
  validate_subscription_subject(subject)
  if queue_group is Some(group) {
    validate_queue_group(group)
  }
  let sid = self.next_sid
  self.next_sid = sid + 1
  let state = new_subscription_state(self.subscription_message_limit)
  self.subscriptions[sid] = state
  send(self, @wire.encode_subscribe(subject, sid, queue_group?))
  { client: self, sid, state, }
}

///|
priv struct DrainTarget {
  sid : Int
  state : SubscriptionState
  waiter : @aqueue.Queue[Unit]
  send_unsubscribe : Bool
}

///|
fn Client::close_local(self : Client) -> Unit {
  self.status = Closed
  for _, state in self.subscriptions {
    match state.status {
      Accepting | DrainingServer | DrainingBuffered => {
        state.status = Detached
        state.inbox.close(error=SubscriptionClosed)
        fail_drain_waiters(state, ConnectionClosed)
      }
      Drained | Detached | OverLimit => ()
    }
  }
  self.subscriptions.clear()
}

///|
/// Stop new subscriptions and requests, retain publishes while subscription
/// mailboxes drain, then cross a final publish barrier. Once this returns, all
/// client operations fail with `ConnectionClosed`; `with_client` closes the
/// socket when its callback returns.
pub async fn Client::drain(self : Client) -> Unit {
  guard self.status is Open else { raise ConnectionClosed }
  self.status = DrainingSubscriptions
  errdefer self.close_local()
  let targets : Array[DrainTarget] = []
  for sid, state in self.subscriptions {
    match state.status {
      Accepting | DrainingServer | DrainingBuffered => {
        let (waiter, send_unsubscribe) = begin_subscription_drain(state)
        targets.push({ sid, state, waiter, send_unsubscribe, })
      }
      Drained | Detached | OverLimit => ()
    }
  }
  let mut sent_unsubscribe = false
  for target in targets {
    if target.send_unsubscribe {
      sent_unsubscribe = true
      self.writer.put(Write(@wire.encode_unsubscribe(target.sid), None))
    }
  }
  if sent_unsubscribe {
    flush_barrier(self)
    for target in targets {
      if target.send_unsubscribe {
        finish_server_drain(self, target.sid, target.state)
      }
    }
  }
  for target in targets {
    ignore(target.waiter.get())
  }
  self.status = DrainingPublishes
  flush_barrier(self)
  self.status = Closed
}

///|
fn new_inbox_prefix() -> String raise ClientError {
  let entropy = match @env.rand(12) {
    None => raise InboxEntropyUnavailable
    Some(bytes) => bytes
  }
  let parts = [ for byte in entropy => byte.to_int().to_string() ]
  "_INBOX.MOONBIT.\{parts.join("-")}"
}

///|
async fn[X, Connection : @io.Reader + @io.Writer] run_client(
  connection : Connection,
  decoder : @wire.Decoder,
  options : Options,
  block : async (Client) -> X,
) -> X {
  finish_handshake(connection, decoder, options)
  @async.with_task_group(group => {
    let writer : @aqueue.Queue[WriterCommand] = Queue(kind=Unbounded)
    let client : Client = {
      writer,
      subscriptions: {},
      pong_actions: Queue(kind=Unbounded),
      request_timeout_ms: options.request_timeout_ms,
      subscription_message_limit: default_subscription_message_limit,
      subscription_byte_limit: default_subscription_byte_limit,
      inbox_prefix: new_inbox_prefix(),
      next_sid: 1,
      next_request: 1,
      status: Open,
    }
    group.spawn_bg(() => writer_loop(connection, writer, client.pong_actions))
    group.spawn_bg(() => reader_loop(connection, decoder, client))
    errdefer client.close_local()
    let result = block(client)
    client.close_local()
    group.return_immediately(result)
    result
  })
}

///|
async fn[X] with_server(
  server : String,
  options : Options,
  block : async (Client) -> X,
) -> X {
  let connection = @socket.Tcp::connect(@socket.Addr::parse(server))
  defer connection.close()
  match options.tls {
    None => {
      let decoder = @wire.Decoder::new()
      read_server_info(connection, decoder)
      run_client(connection, decoder, options, block)
    }
    Some(tls_options) =>
      if tls_options.handshake_first {
        let secure = open_tls(connection, tls_options)
        defer secure.close()
        let decoder = @wire.Decoder::new()
        read_server_info(secure, decoder)
        run_client(secure, decoder, options, block)
      } else {
        let decoder = @wire.Decoder::new()
        read_server_info(connection, decoder)
        let secure = open_tls(connection, tls_options)
        defer secure.close()
        run_client(secure, decoder, options, block)
      }
  }
}

///|
/// Open one scoped native connection. Servers are attempted in their supplied
/// order until INFO, CONNECT and the initial PING/PONG have completed. With TLS
/// in traditional NATS mode, INFO is read before certificate negotiation;
/// TLS-first performs certificate negotiation before any protocol bytes. Once
/// the callback begins, its work is never replayed on another server.
pub async fn[X] with_client(
  options : Options,
  block : async (Client) -> X,
) -> X {
  let last_error : Ref[Error?] = Ref(None)
  for server in options.servers {
    let entered_callback = Ref(false)
    try
      with_server(server, options, client => {
        entered_callback.val = true
        block(client)
      })
    catch {
      error => {
        if entered_callback.val || @async.is_being_cancelled() {
          raise error
        }
        match error {
          InboxEntropyUnavailable => raise error
          _ => last_error.val = Some(error)
        }
      }
    } noraise {
      result => return result
    }
  }
  match last_error.val {
    Some(error) => raise error
    None => raise InvalidServerList("server pool must not be empty")
  }
}
