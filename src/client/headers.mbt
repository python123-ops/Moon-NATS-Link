///|
/// One case-preserving NATS header field. Repeated names remain separate
/// entries and retain their arrival order.
pub(all) struct Header {
  name : String
  value : String
} derive(Eq, Debug)

///|
/// NATS message headers together with an optional protocol status line.
pub struct Headers {
  priv status_code : Int?
  priv status_description : String?
  priv entries : Array[Header]
} derive(Eq, Debug)

///|
/// Create an empty application header block.
pub fn Headers::new() -> Headers {
  { status_code: None, status_description: None, entries: [], }
}

///|
fn header_field_is_valid(name : StringView, value : StringView) -> Bool {
  if name.is_empty() {
    return false
  }
  for char in name {
    let code = char.to_int()
    if code < 33 || code > 126 || char == ':' {
      return false
    }
  }
  for char in value {
    let code = char.to_int()
    if code != 9 && (code < 32 || code > 126) {
      return false
    }
  }
  true
}

///|
/// Append one field without folding repeated names.
pub fn Headers::append(
  self : Headers,
  name : String,
  value : String,
) -> Unit raise ClientError {
  if !header_field_is_valid(name, value) {
    raise InvalidHeader(name)
  }
  self.entries.push({ name, value, })
}

///|
/// Return the first value whose name matches with the same case.
pub fn Headers::get(self : Headers, name : StringView) -> String? {
  for entry in self.entries {
    if entry.name[:] == name {
      return Some(entry.value)
    }
  }
  None
}

///|
/// Return every value whose name matches with the same case.
pub fn Headers::values(self : Headers, name : StringView) -> Array[String] {
  [
    for entry in self.entries if entry.name[:] == name => entry.value
  ]
}

///|
/// Copy the ordered fields for iteration or logging.
pub fn Headers::entries(self : Headers) -> Array[Header] {
  self.entries.copy()
}

///|
/// Return a protocol status such as 503 when the server supplied one.
pub fn Headers::status(self : Headers) -> Int? {
  self.status_code
}

///|
/// Return the text following a protocol status, when present.
pub fn Headers::status_description(self : Headers) -> String? {
  self.status_description
}

///|
fn encode_headers(headers : Headers) -> Bytes {
  let mut text = "NATS/1.0\r\n"
  for entry in headers.entries {
    text = text + "\{entry.name}: \{entry.value}\r\n"
  }
  @utf8.encode(text + "\r\n")
}

///|
fn[X] invalid_header_block(reason : String) -> X raise ClientError {
  raise ProtocolViolation("invalid NATS header block: \{reason}")
}

///|
fn parse_status_line(line : StringView) -> (Int?, String?) raise ClientError {
  if line == "NATS/1.0" {
    return (None, None)
  }
  if !line.has_prefix("NATS/1.0 ") {
    return invalid_header_block("unsupported version line")
  }
  let rest = line.view(start_offset=9)
  let separator = rest.find(" ")
  let code_text = match separator {
    None => rest
    Some(index) => rest[0:index]
  }
  if code_text.length() != 3 {
    return invalid_header_block("status must contain three digits")
  }
  let code = @string.parse_int(code_text) catch {
    _ => return invalid_header_block("status is not numeric")
  }
  let description = match separator {
    None => None
    Some(index) => {
      let value = rest[index + 1:].trim().to_owned()
      if value.is_empty() {
        None
      } else {
        Some(value)
      }
    }
  }
  (Some(code), description)
}

///|
fn decode_headers(raw : BytesView) -> Headers raise ClientError {
  let text = @utf8.decode(raw) catch {
    _ => return invalid_header_block("bytes are not UTF-8")
  }
  if !text.has_suffix("\r\n\r\n") {
    return invalid_header_block("missing final CRLF pair")
  }
  let body = text[0:text.length() - 4].to_owned()
  let lines = body.split("\r\n").to_array()
  if lines.is_empty() {
    return invalid_header_block("missing version line")
  }
  let (status_code, status_description) = parse_status_line(lines[0])
  let headers : Headers = { status_code, status_description, entries: [], }
  for index in 1..<lines.length() {
    let line = lines[index]
    let colon = match line.find(":") {
      None => return invalid_header_block("field has no colon")
      Some(index) => index
    }
    let name = line[0:colon].to_owned()
    let value = line[colon + 1:].trim().to_owned()
    if !header_field_is_valid(name, value) {
      return invalid_header_block("field is outside the ASCII grammar")
    }
    headers.entries.push({ name, value, })
  }
  headers
}
