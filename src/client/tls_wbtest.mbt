///|
test "TLS options retain verification inputs and handshake order" {
  let system = TlsOptions::new("nats.example")
  @debug.assert_eq(system.server_name, "nats.example")
  guard system.ca_file is None && !system.handshake_first else {
    fail("expected system roots and INFO-first")
  }
  guard system.trusted_root() is SystemRoot else {
    fail("expected the operating system trust store")
  }
  let private_ca = TlsOptions::new(
    "localhost",
    ca_file="certs/root.pem",
    handshake_first=true,
  )
  @debug.assert_eq(private_ca.server_name, "localhost")
  @debug.assert_eq(private_ca.ca_file, Some("certs/root.pem"))
  guard private_ca.handshake_first else { fail("expected TLS-first") }
  match private_ca.trusted_root() {
    CustomPemFile(path) => @debug.assert_eq(path, "certs/root.pem")
    _ => fail("expected the custom PEM trust store")
  }
}

///|
test "TLS options reject names and CA paths that cannot be verified" {
  for
    options in [
      ("", None),
      ("bad\rname", None),
      ("bad\nname", None),
      ("localhost", Some("")),
    ] {
    let (server_name, ca_file) = options
    try TlsOptions::new(server_name, ca_file?) catch {
      InvalidTlsConfiguration(_) => ()
      _ => fail("expected InvalidTlsConfiguration")
    } noraise {
      _ => fail("expected invalid TLS settings")
    }
  }
}

///|
test "connection options carry TLS separately from the server address" {
  let tls = TlsOptions::new("localhost", ca_file="certs/root.pem")
  let options = Options::new(server="127.0.0.1:4443", tls~)
  @debug.assert_eq(options.server, "127.0.0.1:4443")
  guard options.tls is Some(config) else { fail("expected TLS settings") }
  @debug.assert_eq(config.server_name, "localhost")
}
