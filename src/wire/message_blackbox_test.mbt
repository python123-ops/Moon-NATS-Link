///|
test "preserve CRLF and non UTF-8 bytes inside MSG payload" {
  let payload : Bytes = [0x00, 0x0D, 0x0A, 0xFF]
  let input = @utf8.encode("MSG events.binary 7 replies.1 4\r\n") +
    payload +
    @utf8.encode("\r\n")
  let decoder = @wire.Decoder::new()
  @debug.assert_eq(
    decoder.feed(input.view()),
    Ok([
      Msg(subject="events.binary", sid=7, reply_to=Some("replies.1"), payload~),
    ]),
  )
  @debug.assert_eq(decoder.finish(), Ok(()))
}

///|
test "accept every split point across MSG header payload and terminator" {
  let payload : Bytes = [0x10, 0x00, 0xFE]
  let input = @utf8.encode("MSG jobs.done 12 3\r\n") +
    payload +
    @utf8.encode("\r\n")
  for split = 0; split <= input.length(); split = split + 1 {
    let decoder = @wire.Decoder::new()
    let first = decoder.feed(input[0:split]).unwrap()
    let second = decoder.feed(input[split:]).unwrap()
    @debug.assert_eq(first + second, [
      Msg(subject="jobs.done", sid=12, reply_to=None, payload~),
    ])
    @debug.assert_eq(decoder.finish(), Ok(()))
  }
}

///|
test "decode MSG one byte at a time and continue with PING" {
  let input = @utf8.encode("MSG ticks.eu 3 1\r\n") +
    ([0x80] : Bytes) +
    @utf8.encode("\r\nPING\r\n")
  let decoder = @wire.Decoder::new()
  let mut frames : Array[@wire.ServerFrame] = []
  for index in 0..<input.length() {
    frames = frames + decoder.feed(input[index:index + 1]).unwrap()
  }
  @debug.assert_eq(frames, [
    Msg(subject="ticks.eu", sid=3, reply_to=None, payload=[0x80]),
    Ping,
  ])
}

///|
test "decode an empty MSG payload" {
  let decoder = @wire.Decoder::new()
  @debug.assert_eq(
    decoder.feed(@utf8.encode("MSG events.empty 1 0\r\n\r\n").view()),
    Ok([Msg(subject="events.empty", sid=1, reply_to=None, payload=b"")]),
  )
}

///|
test "reject malformed MSG metadata" {
  let malformed = [
    "MSG only.two\r\n", "MSG subject sid 1\r\n", "MSG subject 0 1\r\n", "MSG subject 1 -1\r\n",
    "MSG  subject 1 1\r\n",
  ]
  for line in malformed {
    let decoder = @wire.Decoder::new()
    guard decoder.feed(@utf8.encode(line).view())
      is Err(InvalidMessageHeader(_)) else {
      fail("expected invalid MSG header for \{line.escape()}")
    }
  }
}

///|
test "reject bytes beyond the declared payload length" {
  let decoder = @wire.Decoder::new()
  @debug.assert_eq(
    decoder.feed(@utf8.encode("MSG subject 1 2\r\nabc\r\n").view()),
    Err(InvalidPayloadTerminator),
  )
}

///|
test "report a payload truncated by connection close" {
  let decoder = @wire.Decoder::new()
  @debug.assert_eq(
    decoder.feed(@utf8.encode("MSG subject 2 5\r\nabc").view()),
    Ok([]),
  )
  @debug.assert_eq(
    decoder.finish(),
    Err(TruncatedPayload(expected=5, actual=3)),
  )
}

///|
test "encode publish and queue subscription commands" {
  let payload : Bytes = [0x00, 0xFF, 0x0D, 0x0A]
  @debug.assert_eq(
    @wire.encode_subscribe("jobs.*", 42, queue_group="workers"),
    @utf8.encode("SUB jobs.* workers 42\r\n"),
  )
  @debug.assert_eq(
    @wire.encode_subscribe("events.>", 43),
    @utf8.encode("SUB events.> 43\r\n"),
  )
  @debug.assert_eq(
    @wire.encode_publish("events.binary", payload.view()),
    @utf8.encode("PUB events.binary 4\r\n") + payload + @utf8.encode("\r\n"),
  )
  @debug.assert_eq(
    @wire.encode_publish("jobs.start", payload.view(), reply_to="reply.7"),
    @utf8.encode("PUB jobs.start reply.7 4\r\n") +
    payload +
    @utf8.encode("\r\n"),
  )
}

///|
test "split HMSG headers from a binary payload at every transport boundary" {
  let headers = @utf8.encode("NATS/1.0\r\nTrace-Id: a7\r\n\r\n")
  let payload : Bytes = [0x00, 0x0D, 0x0A, 0xFF]
  let total_len = headers.length() + payload.length()
  let input = @utf8.encode(
      "HMSG rpc.echo 8 replies.4 \{headers.length()} \{total_len}\r\n",
    ) +
    headers +
    payload +
    @utf8.encode("\r\n")
  for split = 0; split <= input.length(); split = split + 1 {
    let decoder = @wire.Decoder::new()
    let first = decoder.feed(input[0:split]).unwrap()
    let second = decoder.feed(input[split:]).unwrap()
    @debug.assert_eq(first + second, [
      HMsg(
        subject="rpc.echo",
        sid=8,
        reply_to=Some("replies.4"),
        headers~,
        payload~,
      ),
    ])
  }
}

///|
test "preserve a 503 status-only HMSG" {
  let headers = @utf8.encode("NATS/1.0 503\r\n\r\n")
  let input = @utf8.encode(
      "HMSG _INBOX.MOONBIT.1 3 \{headers.length()} \{headers.length()}\r\n",
    ) +
    headers +
    @utf8.encode("\r\n")
  let decoder = @wire.Decoder::new()
  @debug.assert_eq(
    decoder.feed(input.view()),
    Ok([
      HMsg(
        subject="_INBOX.MOONBIT.1",
        sid=3,
        reply_to=None,
        headers~,
        payload=b"",
      ),
    ]),
  )
}

///|
test "reject HMSG lengths that place headers beyond the body" {
  for
    line in [
      "HMSG subject 1 20 10\r\n", "HMSG subject sid 10 10\r\n", "HMSG subject 1 -1 10\r\n",
      "HMSG subject 1 reply 10\r\n",
    ] {
    let decoder = @wire.Decoder::new()
    guard decoder.feed(@utf8.encode(line).view())
      is Err(InvalidHeaderMessageHeader(_)) else {
      fail("expected invalid HMSG header for \{line.escape()}")
    }
  }
}

///|
test "encode HPUB and both UNSUB forms" {
  let headers = @utf8.encode("NATS/1.0\r\nTrace-Id: a7\r\n\r\n")
  let payload : Bytes = [0x00, 0xFF]
  @debug.assert_eq(
    @wire.encode_header_publish(
      "rpc.echo",
      headers.view(),
      payload.view(),
      reply_to="reply.1",
    ),
    @utf8.encode(
      "HPUB rpc.echo reply.1 \{headers.length()} \{headers.length() + 2}\r\n",
    ) +
    headers +
    payload +
    @utf8.encode("\r\n"),
  )
  @debug.assert_eq(@wire.encode_unsubscribe(7), @utf8.encode("UNSUB 7\r\n"))
  @debug.assert_eq(
    @wire.encode_unsubscribe(8, max_messages=1),
    @utf8.encode("UNSUB 8 1\r\n"),
  )
}
