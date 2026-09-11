///|
/// Certificate verification and handshake order for a TLS connection.
///
/// `server_name` is checked against the server certificate. When `ca_file` is
/// absent, the operating system trust store is used. `handshake_first=false`
/// follows the traditional NATS sequence: receive INFO, then start TLS.
pub struct TlsOptions {
  priv server_name : String
  priv ca_file : String?
  priv handshake_first : Bool
}

///|
/// Create TLS settings without an insecure certificate-bypass mode.
pub fn TlsOptions::new(
  server_name : String,
  ca_file? : String,
  handshake_first? : Bool = false,
) -> TlsOptions raise ClientError {
  if server_name == "" ||
    server_name.contains("\r") ||
    server_name.contains("\n") {
    raise InvalidTlsConfiguration("server name must be a non-empty line")
  }
  if ca_file is Some("") {
    raise InvalidTlsConfiguration("CA file path must not be empty")
  }
  { server_name, ca_file, handshake_first, }
}

///|
fn TlsOptions::trusted_root(self : TlsOptions) -> @tls.TrustedRoot {
  match self.ca_file {
    None => SystemRoot
    Some(path) => CustomPemFile(path)
  }
}

///|
async fn[Inner : @io.Reader + @io.Writer] open_tls(
  inner : Inner,
  options : TlsOptions,
) -> @tls.Tls {
  @tls.Tls::client(
    inner,
    host=options.server_name,
    trust=options.trusted_root(),
  ) catch {
    @tls.TlsError(message) => raise TlsHandshakeFailed(message)
    error => raise error
  }
}
