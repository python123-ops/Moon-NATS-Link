///|
async fn accept_frames(
  connection : @socket.Tcp,
  decoder : @wire.Decoder,
  chunk : BytesView,
  waiting_for_info~ : Bool,
) -> (Bool, Bool) {
  let mut saw_info = false
  let mut saw_pong = false
  let frames = match decoder.feed(chunk) {
    Ok(frames) => frames
    Err(error) =>
      fail("server sent an invalid control line: \{@debug.to_string(error)}")
  }
  for frame in frames {
    match frame {
      @wire.Info(_) => saw_info = true
      @wire.Pong => saw_pong = true
      @wire.Ping => connection.write(@wire.encode_pong())
      @wire.Error(message) => fail("server rejected handshake: \{message}")
      @wire.Ok => ()
    }
  }
  if waiting_for_info && saw_pong {
    fail("received PONG before the initial INFO")
  }
  (saw_info, saw_pong)
}

///|
async fn main {
  let connection = @socket.Tcp::connect(@socket.Addr::parse("127.0.0.1:4222"))
  defer connection.close()
  let decoder = @wire.Decoder::new()
  let mut received_info = false
  while !received_info {
    match connection.read_some() {
      None => fail("server closed the connection before INFO")
      Some(chunk) => {
        let (saw_info, _) = accept_frames(
          connection,
          decoder,
          chunk,
          waiting_for_info=true,
        )
        received_info = saw_info
      }
    }
  }
  connection.write(@wire.encode_connect("moon-nats-link-handshake"))
  connection.write(@wire.encode_ping())
  let mut received_pong = false
  while !received_pong {
    match connection.read_some() {
      None => fail("server closed the connection before PONG")
      Some(chunk) => {
        let (_, saw_pong) = accept_frames(
          connection,
          decoder,
          chunk,
          waiting_for_info=false,
        )
        received_pong = saw_pong
      }
    }
  }
  println("handshake ok: INFO -> CONNECT -> PING/PONG")
}
