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
                Msg(..) =>
                  raise ProtocolViolation("received MSG before CONNECT")
              }
            }
        }
    }
  }
  connection.write(@wire.encode_connect(name))
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
                Msg(..) =>
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
) -> Unit {
  for ;; {
    match commands.get() {
      Write(bytes, completion) => {
        connection.write(bytes)
        if completion is Some(done) {
          done.put(())
        }
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
      match client.subscriptions.get(sid) {
        None =>
          raise ProtocolViolation(
            "server delivered an unknown subscription id \{sid}",
          )
        Some(inbox) => inbox.put({ subject, reply_to, payload, })
      }
    Ping => client.writer.put(Write(@wire.encode_pong(), None))
    Error(message) => raise ServerRejected(message)
    Info(_) | Ok | Pong => ()
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
) -> Unit {
  validate_publish_subject(subject)
  if reply_to is Some(reply) {
    validate_publish_subject(reply)
  }
  send(self, @wire.encode_publish(subject, payload, reply_to?))
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
  let inbox : @aqueue.Queue[Message] = Queue(kind=Unbounded)
  self.subscriptions[sid] = inbox
  send(self, @wire.encode_subscribe(subject, sid, queue_group?))
  { inbox, }
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
      next_sid: 1,
      closing: false,
    }
    group.spawn_bg(() => writer_loop(connection, writer))
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
