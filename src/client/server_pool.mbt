///|
fn server_address_is_valid(server : String) -> Bool {
  server != "" && !server.contains("\r") && !server.contains("\n")
}

///|
priv struct ServerPool {
  configured : Array[String]
  mut discovered : Array[String]
}

///|
fn ServerPool::new(
  server : String?,
  servers : Array[String]?,
) -> ServerPool raise ClientError {
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
  { configured: selected, discovered: [], }
}

///|
fn ServerPool::addresses(self : ServerPool) -> Array[String] {
  self.configured + self.discovered
}

///|
fn ServerPool::replace_discovered(
  self : ServerPool,
  incoming : Array[String],
) -> Unit raise ClientError {
  let known : Map[String, Unit] = Map([])
  for address in self.configured {
    known[address] = ()
  }
  let discovered = []
  for address in incoming {
    if !server_address_is_valid(address) {
      raise InvalidServerList(
        "discovered server addresses must be non-empty lines",
      )
    }
    if !known.contains(address) {
      known[address] = ()
      discovered.push(address)
    }
  }
  self.discovered = discovered
}

///|
fn ServerPool::next_unattempted(
  self : ServerPool,
  attempted : Map[String, Unit],
) -> String? {
  for address in self.addresses() {
    if !attempted.contains(address) {
      return Some(address)
    }
  }
  None
}

///|
fn ServerPool::update_from_info(
  self : ServerPool,
  payload : String,
) -> Unit raise ClientError {
  let document = @json.parse(payload) catch {
    _ => raise ProtocolViolation("INFO payload is not valid JSON")
  }
  guard document is Object(fields) else {
    raise ProtocolViolation("INFO payload must be a JSON object")
  }
  match fields.get("connect_urls") {
    None => ()
    Some(Array(values)) => {
      let addresses = []
      for value in values {
        match value {
          String(address) => addresses.push(address)
          _ =>
            raise ProtocolViolation(
              "INFO connect_urls must contain only strings",
            )
        }
      }
      self.replace_discovered(addresses) catch {
        InvalidServerList(_) =>
          raise ProtocolViolation(
            "INFO connect_urls contains an invalid address",
          )
        error => raise error
      }
    }
    Some(_) =>
      raise ProtocolViolation("INFO connect_urls must be an array of strings")
  }
}
