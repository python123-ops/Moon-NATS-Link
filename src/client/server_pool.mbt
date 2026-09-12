///|
fn server_address_is_valid(server : String) -> Bool {
  server != "" && !server.contains("\r") && !server.contains("\n")
}

///|
fn normalize_servers(
  server : String?,
  servers : Array[String]?,
) -> Array[String] raise ClientError {
  let selected = match (server, servers) {
    (None, None) => ["127.0.0.1:4222"]
    (Some(value), None) => [value]
    (None, Some(values)) => [ for value in values => value ]
    (Some(_), Some(_)) =>
      raise InvalidServerList("server and servers cannot be combined")
  }
  if selected.is_empty() {
    raise InvalidServerList("server pool must not be empty")
  }
  for value in selected {
    if !server_address_is_valid(value) {
      raise InvalidServerList("server addresses must be non-empty lines")
    }
  }
  selected
}
