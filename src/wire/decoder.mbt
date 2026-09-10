///|
/// A complete server frame decoded from the NATS byte stream.
pub(all) enum ServerFrame {
  Info(String)
  Ok
  Error(String)
  Ping
  Pong
  Msg(subject~ : String, sid~ : Int, reply_to~ : String?, payload~ : Bytes)
  HMsg(
    subject~ : String,
    sid~ : Int,
    reply_to~ : String?,
    headers~ : Bytes,
    payload~ : Bytes
  )
} derive(Eq, Debug)

///|
/// Why a server frame could not be decoded.
pub(all) enum DecodeError {
  BareLineFeed
  InvalidCarriageReturn
  InvalidUtf8
  UnknownControlLine(String)
  InvalidMessageHeader(String)
  InvalidHeaderMessageHeader(String)
  PayloadTooLarge(Int)
  InvalidPayloadTerminator
  TruncatedControlLine(Int)
  TruncatedPayload(expected~ : Int, actual~ : Int)
} derive(Eq, Debug)

///|
priv struct PendingMessage {
  subject : String
  sid : Int
  reply_to : String?
  header_len : Int?
  total_len : Int
}

///|
priv enum DecodeState {
  AwaitLine
  AwaitPayload(PendingMessage)
}

///|
priv enum ParsedLine {
  Complete(ServerFrame)
  PayloadHeader(PendingMessage)
}

///|
/// Incremental decoder state. Incomplete input remains buffered for the next
/// call to `feed`.
pub struct Decoder {
  priv mut buffer : Array[Byte]
  priv mut state : DecodeState
}

///|
/// Create an empty server-frame decoder.
pub fn Decoder::new() -> Decoder {
  { buffer: [], state: AwaitLine, }
}

///|
/// Return the number of bytes currently retained by the decoder.
pub fn Decoder::buffered_len(self : Decoder) -> Int {
  self.buffer.length()
}

///|
fn find_crlf(buffer : Array[Byte]) -> Result[Int?, DecodeError] {
  for cursor in 0..<buffer.length() {
    match buffer[cursor].to_int() {
      0x0A => return Err(BareLineFeed)
      0x0D =>
        if cursor + 1 == buffer.length() {
          return Ok(None)
        } else if buffer[cursor + 1].to_int() == 0x0A {
          return Ok(Some(cursor))
        } else {
          return Err(InvalidCarriageReturn)
        }
      _ => ()
    }
  }
  Ok(None)
}

///|
fn invalid_message_header(line : String) -> Result[ParsedLine, DecodeError] {
  Err(InvalidMessageHeader(line))
}

///|
fn invalid_header_message_header(
  line : String,
) -> Result[ParsedLine, DecodeError] {
  Err(InvalidHeaderMessageHeader(line))
}

///|
fn parse_msg_header(line : String) -> Result[ParsedLine, DecodeError] {
  let fields = line.split(" ").to_array()
  if fields.length() != 4 && fields.length() != 5 {
    return invalid_message_header(line)
  }
  for field in fields {
    if field.is_empty() {
      return invalid_message_header(line)
    }
  }
  let sid = @string.parse_int(fields[2]) catch {
    _ => return invalid_message_header(line)
  }
  let length_index = fields.length() - 1
  let payload_len = @string.parse_int(fields[length_index]) catch {
    _ => return invalid_message_header(line)
  }
  if sid <= 0 || payload_len < 0 {
    return invalid_message_header(line)
  }
  if payload_len > 64 * 1024 * 1024 {
    return Err(PayloadTooLarge(payload_len))
  }
  let reply_to = if fields.length() == 5 {
    Some(fields[3].to_owned())
  } else {
    None
  }
  Ok(
    PayloadHeader({
      subject: fields[1].to_owned(),
      sid,
      reply_to,
      header_len: None,
      total_len: payload_len,
    }),
  )
}

///|
fn parse_hmsg_header(line : String) -> Result[ParsedLine, DecodeError] {
  let fields = line.split(" ").to_array()
  if fields.length() != 5 && fields.length() != 6 {
    return invalid_header_message_header(line)
  }
  for field in fields {
    if field.is_empty() {
      return invalid_header_message_header(line)
    }
  }
  let sid = @string.parse_int(fields[2]) catch {
    _ => return invalid_header_message_header(line)
  }
  let header_index = fields.length() - 2
  let header_len = @string.parse_int(fields[header_index]) catch {
    _ => return invalid_header_message_header(line)
  }
  let total_len = @string.parse_int(fields[header_index + 1]) catch {
    _ => return invalid_header_message_header(line)
  }
  if sid <= 0 || header_len < 0 || total_len < header_len {
    return invalid_header_message_header(line)
  }
  if total_len > 64 * 1024 * 1024 {
    return Err(PayloadTooLarge(total_len))
  }
  let reply_to = if fields.length() == 6 {
    Some(fields[3].to_owned())
  } else {
    None
  }
  Ok(
    PayloadHeader({
      subject: fields[1].to_owned(),
      sid,
      reply_to,
      header_len: Some(header_len),
      total_len,
    }),
  )
}

///|
fn decode_line(bytes : ArrayView[Byte]) -> Result[ParsedLine, DecodeError] {
  let line = @utf8.decode(Bytes::from_array(bytes).view()) catch {
    _ => return Err(InvalidUtf8)
  }
  match line {
    "PING" => Ok(Complete(Ping))
    "PONG" => Ok(Complete(Pong))
    "+OK" => Ok(Complete(Ok))
    _ =>
      if line.has_prefix("INFO ") {
        Ok(Complete(Info(line.view(start_offset=5).to_owned())))
      } else if line.has_prefix("-ERR ") {
        Ok(Complete(Error(line.view(start_offset=5).to_owned())))
      } else if line.has_prefix("MSG ") {
        parse_msg_header(line)
      } else if line.has_prefix("HMSG ") {
        parse_hmsg_header(line)
      } else {
        Err(UnknownControlLine(line))
      }
  }
}

///|
fn Decoder::consume(self : Decoder, count : Int) -> Unit {
  self.buffer = self.buffer[count:].to_owned()
}

///|
/// Append one transport chunk and return every complete server frame. Payload
/// bytes are read by declared length and may contain any byte sequence.
pub fn Decoder::feed(
  self : Decoder,
  chunk : BytesView,
) -> Result[Array[ServerFrame], DecodeError] {
  for byte in chunk {
    self.buffer.push(byte)
  }
  let frames : Array[ServerFrame] = []
  for ;; {
    match self.state {
      AwaitLine => {
        let line_end = match find_crlf(self.buffer) {
          Err(error) => return Err(error)
          Ok(None) => break
          Ok(Some(index)) => index
        }
        let parsed = match decode_line(self.buffer[0:line_end]) {
          Err(error) => return Err(error)
          Ok(parsed) => parsed
        }
        self.consume(line_end + 2)
        match parsed {
          Complete(frame) => frames.push(frame)
          PayloadHeader(pending) => self.state = AwaitPayload(pending)
        }
      }
      AwaitPayload(pending) => {
        let required = pending.total_len + 2
        if self.buffer.length() < required {
          break
        }
        if self.buffer[pending.total_len].to_int() != 0x0D ||
          self.buffer[pending.total_len + 1].to_int() != 0x0A {
          return Err(InvalidPayloadTerminator)
        }
        match pending.header_len {
          None => {
            let payload = Bytes::from_array(self.buffer[0:pending.total_len])
            frames.push(
              Msg(
                subject=pending.subject,
                sid=pending.sid,
                reply_to=pending.reply_to,
                payload~,
              ),
            )
          }
          Some(header_len) => {
            let headers = Bytes::from_array(self.buffer[0:header_len])
            let payload = Bytes::from_array(
              self.buffer[header_len:pending.total_len],
            )
            frames.push(
              HMsg(
                subject=pending.subject,
                sid=pending.sid,
                reply_to=pending.reply_to,
                headers~,
                payload~,
              ),
            )
          }
        }
        self.consume(required)
        self.state = AwaitLine
      }
    }
  }
  Ok(frames)
}

///|
/// Report whether the peer closed the stream between protocol boundaries.
pub fn Decoder::finish(self : Decoder) -> Result[Unit, DecodeError] {
  match self.state {
    AwaitLine =>
      if self.buffer.is_empty() {
        Ok(())
      } else {
        Err(TruncatedControlLine(self.buffer.length()))
      }
    AwaitPayload(pending) => {
      let actual = if self.buffer.length() < pending.total_len {
        self.buffer.length()
      } else {
        pending.total_len
      }
      Err(TruncatedPayload(expected=pending.total_len, actual~))
    }
  }
}
