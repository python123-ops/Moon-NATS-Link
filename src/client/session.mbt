///|
async fn handshake(connection : @socket.Tcp, name : String) -> @wire.Decoder {
  let decoder = @wire.Decoder::new()
  let mut received_info = false
  while !received_info {
    match connection.read_some() {
      None => raise ConnectionClosed
      Some(chunk) =>
        match decoder.feed(chunk) {
          Err(error) => raise ProtocolViolation(@debug.to_string(error))
          Ok(frames) =>
            for frame in frames {
              match frame {
                Info(_) => received_info = true
                Ping => connection.write(@wire.encode_pong())
                Error(message) => raise ServerRejected(message)
                Ok | Pong => ()
                Msg(..) | HMsg(..) =>
                  raise ProtocolViolation("received MSG before CONNECT")
              }
            }
        }
    }
  }
  connection.write(@wire.encode_connect(name, headers=true, no_responders=true))
  connection.write(@wire.encode_ping())
  let mut received_pong = false
  while !received_pong {
    match connection.read_some() {
      None => raise ConnectionClosed
      Some(chunk) =>
        match decoder.feed(chunk) {
          Err(error) => raise ProtocolViolation(@debug.to_string(error))
          Ok(frames) =>
            for frame in frames {
              match frame {
                Pong => received_pong = true
                Ping => connection.write(@wire.encode_pong())
                Error(message) => raise ServerRejected(message)
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
  decoder
}

///|
async fn writer_loop(
  connection : @socket.Tcp,
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
  if client.closing {
    raise ConnectionClosed
  }
  let completion : @aqueue.Queue[Unit] = Queue(kind=Blocking(1))
  client.writer.put(Write(bytes, Some(completion)))
  ignore(completion.get())
}

///|
async fn route_frame(client : Client, frame : @wire.ServerFrame) -> Unit {
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
      if state.status is Accepting {
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
async fn reader_loop(
  connection : @socket.Tcp,
  decoder : @wire.Decoder,
  client : Client,
) -> Unit {
  for ;; {
    let chunk = connection.read_some() catch {
      error => if client.closing { None } else { raise error }
    }
    match chunk {
      None => {
        if client.closing {
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
  if self.closing {
    raise ConnectionClosed
  }
  let waiter : @aqueue.Queue[Unit] = Queue(kind=Blocking(1))
  self.writer.put(Flush(CompleteFlush(waiter)))
  ignore(waiter.get())
}

///|
/// Create a subscription and wait until its SUB command reaches the writer.
pub async fn Client::subscribe(
  self : Client,
  subject : String,
  queue_group? : String,
) -> Subscription {
  validate_subscription_subject(subject)
  if queue_group is Some(group) {
    validate_queue_group(group)
  }
  if self.closing {
    raise ConnectionClosed
  }
  let sid = self.next_sid
  self.next_sid = sid + 1
  let state = new_subscription_state(self.subscription_message_limit)
  self.subscriptions[sid] = state
  send(self, @wire.encode_subscribe(subject, sid, queue_group?))
  { client: self, sid, state, }
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
/// Open one scoped native connection. The callback runs only after INFO,
/// CONNECT and the initial PING/PONG have completed. Returning from the
/// callback closes the writer and waits for the reader to terminate.
pub async fn[X] with_client(
  options : Options,
  block : async (Client) -> X,
) -> X {
  let connection = @socket.Tcp::connect(@socket.Addr::parse(options.server))
  defer connection.close()
  let decoder = handshake(connection, options.name)
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
      closing: false,
    }
    group.spawn_bg(() => writer_loop(connection, writer, client.pong_actions))
    group.spawn_bg(() => reader_loop(connection, decoder, client))
    errdefer {
      client.closing = true
    }
    let result = block(client)
    client.closing = true
    group.return_immediately(result)
    result
  })
}
