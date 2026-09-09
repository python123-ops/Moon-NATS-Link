///|
/// A complete server control frame decoded from the NATS byte stream.
pub(all) enum ServerFrame {
  Info(String)
  Ok
  Error(String)
  Ping
  Pong
} derive(Eq, Debug)

///|
/// Why a server control line could not be decoded.
pub(all) enum DecodeError {
  BareLineFeed
  InvalidCarriageReturn
  InvalidUtf8
  UnknownControlLine(String)
} derive(Eq, Debug)

///|
/// Incremental decoder state. Incomplete input remains buffered for the next
/// call to `feed`.
pub struct Decoder {
  mut buffer : Array[Byte]
}

///|
/// Create an empty server-frame decoder.
pub fn Decoder::new() -> Decoder {
  { buffer: [], }
}

///|
/// Return the number of bytes currently waiting for a complete CRLF line.
pub fn Decoder::buffered_len(self : Decoder) -> Int {
  self.buffer.length()
}

///|
fn find_crlf(buffer : Array[Byte], start : Int) -> Result[Int?, DecodeError] {
  let mut cursor = start
  while cursor < buffer.length() {
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
      _ => cursor = cursor + 1
    }
  }
  Ok(None)
}

///|
fn decode_line(bytes : ArrayView[Byte]) -> Result[ServerFrame, DecodeError] {
  let line = @utf8.decode(Bytes::from_array(bytes).view()) catch {
    _ => return Err(InvalidUtf8)
  }
  match line {
    "PING" => Ok(Ping)
    "PONG" => Ok(Pong)
    "+OK" => Ok(Ok)
    _ =>
      if line.has_prefix("INFO ") {
        Ok(Info(line.view(start_offset=5).to_owned()))
      } else if line.has_prefix("-ERR ") {
        Ok(Error(line.view(start_offset=5).to_owned()))
      } else {
        Err(UnknownControlLine(line))
      }
  }
}

///|
/// Append one transport chunk and return every complete server control frame.
/// A chunk may end at any byte boundary, including between CR and LF.
pub fn Decoder::feed(
  self : Decoder,
  chunk : BytesView,
) -> Result[Array[ServerFrame], DecodeError] {
  for byte in chunk {
    self.buffer.push(byte)
  }
  let frames : Array[ServerFrame] = []
  let mut consumed = 0
  while true {
    let line_end = match find_crlf(self.buffer, consumed) {
      Err(error) => return Err(error)
      Ok(None) => break
      Ok(Some(index)) => index
    }
    match decode_line(self.buffer[consumed:line_end]) {
      Err(error) => return Err(error)
      Ok(frame) => frames.push(frame)
    }
    consumed = line_end + 2
  }
  if consumed > 0 {
    self.buffer = self.buffer[consumed:].to_owned()
  }
  Ok(frames)
}
