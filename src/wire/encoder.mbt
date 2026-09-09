///|
/// Encode the CONNECT command used by the initial handshake.
///
/// Authentication fields are deliberately absent from this first handshake
/// shape; callers cannot accidentally interpolate credentials into a URL.
pub fn encode_connect(client_name : String) -> Bytes {
  let body : Json = {
    "verbose": false,
    "pedantic": false,
    "tls_required": false,
    "name": client_name,
    "lang": "moonbit",
    "version": "0.1.0",
    "protocol": 1,
    "echo": true,
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
