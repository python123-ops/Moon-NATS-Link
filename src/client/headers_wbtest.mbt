///|
test "headers preserve case duplicate values and their wire order" {
  let headers = Headers::new()
  headers.append("Trace-Id", "a7")
  headers.append("Trace-Id", "b8")
  headers.append("trace-id", "lower")
  @debug.assert_eq(headers.get("Trace-Id"), Some("a7"))
  @debug.assert_eq(headers.values("Trace-Id"), ["a7", "b8"])
  @debug.assert_eq(
    encode_headers(headers),
    @utf8.encode(
      "NATS/1.0\r\nTrace-Id: a7\r\nTrace-Id: b8\r\ntrace-id: lower\r\n\r\n",
    ),
  )
}

///|
test "decode status and repeated fields from an HMSG header block" {
  let headers = decode_headers(
    @utf8.encode(
      "NATS/1.0 503 No Responders\r\nRetry-After: 2\r\nRetry-After: 4\r\n\r\n",
    ).view(),
  )
  @debug.assert_eq(headers.status(), Some(503))
  @debug.assert_eq(headers.status_description(), Some("No Responders"))
  @debug.assert_eq(headers.values("Retry-After"), ["2", "4"])
}

///|
test "reject line injection and malformed inbound blocks" {
  let headers = Headers::new()
  for
    (name, value) in [
      ("Bad Name", "x"),
      ("Bad:Name", "x"),
      ("Trace-Id", "x\r\nInjected: yes"),
    ] {
    try headers.append(name, value) catch {
      InvalidHeader(_) => ()
      _ => fail("expected InvalidHeader")
    } noraise {
      _ => fail("expected invalid outbound header")
    }
  }
  for
    raw in [
      "HTTP/1.1\r\n\r\n", "NATS/1.0\r\nMissing-Colon\r\n\r\n", "NATS/1.0 50x\r\n\r\n",
      "NATS/1.0\r\nTrace-Id: x\r\n",
    ] {
    try decode_headers(@utf8.encode(raw).view()) catch {
      ProtocolViolation(_) => ()
      _ => fail("expected ProtocolViolation")
    } noraise {
      _ => fail("expected invalid inbound header block")
    }
  }
}
