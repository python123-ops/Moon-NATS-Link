///|
test "decode several control lines from one transport chunk" {
  let decoder = @wire.Decoder::new()
  let frames = decoder
    .feed(
      @utf8.encode("INFO {\"server_id\":\"A\"}\r\n+OK\r\nPING\r\nPONG\r\n").view(),
    )
    .unwrap()
  @debug.assert_eq(frames, [Info("{\"server_id\":\"A\"}"), Ok, Ping, Pong])
  @debug.assert_eq(decoder.buffered_len(), 0)
}

///|
test "retain a line split between carriage return and line feed" {
  let decoder = @wire.Decoder::new()
  @debug.assert_eq(decoder.feed(@utf8.encode("PI").view()).unwrap(), [])
  @debug.assert_eq(decoder.feed(@utf8.encode("NG\r").view()).unwrap(), [])
  @debug.assert_eq(decoder.buffered_len(), 5)
  @debug.assert_eq(decoder.feed(@utf8.encode("\n").view()).unwrap(), [Ping])
  @debug.assert_eq(decoder.buffered_len(), 0)
}

///|
test "accept every split point in an INFO frame" {
  let input = @utf8.encode("INFO {\"server_id\":\"split\"}\r\n")
  for split = 0; split <= input.length(); split = split + 1 {
    let decoder = @wire.Decoder::new()
    let first = decoder.feed(input[0:split]).unwrap()
    let second = decoder.feed(input[split:]).unwrap()
    @debug.assert_eq(first + second, [Info("{\"server_id\":\"split\"}")])
  }
}

///|
test "decode a complete frame one byte at a time" {
  let input = @utf8.encode("PING\r\n")
  let decoder = @wire.Decoder::new()
  let mut frames : Array[@wire.ServerFrame] = []
  for index in 0..<input.length() {
    frames = frames + decoder.feed(input[index:index + 1]).unwrap()
  }
  @debug.assert_eq(frames, [Ping])
  @debug.assert_eq(decoder.buffered_len(), 0)
}

///|
test "reject LF without the protocol carriage return" {
  let decoder = @wire.Decoder::new()
  @debug.assert_eq(
    decoder.feed(@utf8.encode("PING\n").view()),
    Err(BareLineFeed),
  )
}

///|
test "reject an unknown server command" {
  let decoder = @wire.Decoder::new()
  @debug.assert_eq(
    decoder.feed(@utf8.encode("WAT\r\n").view()),
    Err(UnknownControlLine("WAT")),
  )
}

///|
test "reject a carriage return not followed by line feed" {
  let decoder = @wire.Decoder::new()
  @debug.assert_eq(
    decoder.feed(@utf8.encode("PING\rX").view()),
    Err(InvalidCarriageReturn),
  )
}

///|
test "reject non UTF-8 bytes in a control line" {
  let decoder = @wire.Decoder::new()
  let input = Bytes::from_array([0xFF, 0x0D, 0x0A])
  @debug.assert_eq(decoder.feed(input.view()), Err(InvalidUtf8))
}

///|
test "encode handshake commands without line-ending ambiguity" {
  @debug.assert_eq(@wire.encode_ping(), @utf8.encode("PING\r\n"))
  @debug.assert_eq(@wire.encode_pong(), @utf8.encode("PONG\r\n"))
  @debug.assert_eq(
    @wire.encode_connect("moon-nats-link"),
    @utf8.encode(
      "CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"name\":\"moon-nats-link\",\"lang\":\"moonbit\",\"version\":\"0.1.0\",\"protocol\":1,\"echo\":true}\r\n",
    ),
  )
}
