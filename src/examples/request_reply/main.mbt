///|
async fn main {
  let options = @client.Options::new(
    name="moon-nats-link-request-reply",
    request_timeout_ms=120,
  )
  @client.with_client(options, connection => {
    let responder = connection.subscribe("rpc.echo")
    connection.flush()
    let request_headers = @client.Headers::new()
    request_headers.append("Trace-Id", "demo-7")
    @async.with_task_group(group => {
      group.spawn_bg(() => {
        let incoming = responder.next()
        guard incoming.headers is Some(headers) else {
          fail("request did not carry headers")
        }
        @debug.assert_eq(headers.get("Trace-Id"), Some("demo-7"))
        guard incoming.reply_to is Some(reply) else {
          fail("request did not carry a reply subject")
        }
        let response_headers = @client.Headers::new()
        response_headers.append("Handled-By", "echo-1")
        connection.publish(
          reply,
          incoming.payload.view(),
          headers=response_headers,
        )
      })
      let response = connection.request(
        "rpc.echo",
        b"hello".view(),
        headers=request_headers,
      )
      @debug.assert_eq(response.payload, b"hello")
      guard response.headers is Some(headers) else {
        fail("response did not carry headers")
      }
      @debug.assert_eq(headers.get("Handled-By"), Some("echo-1"))
    })
    responder.unsubscribe()
    try connection.request("rpc.missing", b"x".view()) catch {
      @client.NoResponders => ()
      _ => fail("expected NoResponders")
    } noraise {
      _ => fail("expected missing responder failure")
    }
    let silent = connection.subscribe("rpc.silent")
    connection.flush()
    try connection.request("rpc.silent", b"wait".view()) catch {
      @client.Timeout => ()
      _ => fail("expected Timeout")
    } noraise {
      _ => fail("expected silent responder timeout")
    }
    let timed_out_request = silent.next()
    @debug.assert_eq(timed_out_request.payload, b"wait")
    silent.unsubscribe()
    connection.flush()
    println("request/reply ok: headers + 503 no responders + timeout")
  })
}
