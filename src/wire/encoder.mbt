///|
/// Encode the CONNECT command used by the initial handshake.
///
/// Authentication fields are deliberately absent from this first handshake
/// shape; callers cannot accidentally interpolate credentials into a URL.
pub fn encode_connect(
  client_name : String,
  headers? : Bool = false,
  no_responders? : Bool = false,
) -> Bytes {
  let body : Json = {
    "verbose": false,
    "pedantic": false,
    "tls_required": false,
    "name": client_name,
    "lang": "moonbit",
    "version": "0.1.0",
    "protocol": 1,
    "echo": true,
    "headers": headers,
    "no_responders": no_responders,
  }
  @utf8.encode("CONNECT \{body.stringify()}\r\n")
}

///|
/// Encode a client PING command.
pub fn encode_ping() -> Bytes {
  @utf8.encode("PING\r\n")
}

///|
/// Encode a client PONG command.
pub fn encode_pong() -> Bytes {
  @utf8.encode("PONG\r\n")
}

///|
/// Encode a subscription. A queue group, when present, is placed between the
/// subject and the numeric subscription id as required by Core NATS.
pub fn encode_subscribe(
  subject : String,
  sid : Int,
  queue_group? : String,
) -> Bytes {
  match queue_group {
    None => @utf8.encode("SUB \{subject} \{sid}\r\n")
    Some(group) => @utf8.encode("SUB \{subject} \{group} \{sid}\r\n")
  }
}

///|
/// Encode a publish command while preserving the payload as raw bytes.
pub fn encode_publish(
  subject : String,
  payload : BytesView,
  reply_to? : String,
) -> Bytes {
  let header = match reply_to {
    None => "PUB \{subject} \{payload.length()}\r\n"
    Some(reply) => "PUB \{subject} \{reply} \{payload.length()}\r\n"
  }
  @utf8.encode(header) + payload.to_owned() + @utf8.encode("\r\n")
}

///|
/// Encode an HPUB command from an already serialized NATS header block.
pub fn encode_header_publish(
  subject : String,
  headers : BytesView,
  payload : BytesView,
  reply_to? : String,
) -> Bytes {
  let total_len = headers.length() + payload.length()
  let control = match reply_to {
    None => "HPUB \{subject} \{headers.length()} \{total_len}\r\n"
    Some(reply) =>
      "HPUB \{subject} \{reply} \{headers.length()} \{total_len}\r\n"
  }
  @utf8.encode(control) +
  headers.to_owned() +
  payload.to_owned() +
  @utf8.encode("\r\n")
}

///|
/// Encode immediate or automatic unsubscription for a server SID.
pub fn encode_unsubscribe(sid : Int, max_messages? : Int) -> Bytes {
  match max_messages {
    None => @utf8.encode("UNSUB \{sid}\r\n")
    Some(limit) => @utf8.encode("UNSUB \{sid} \{limit}\r\n")
  }
}
