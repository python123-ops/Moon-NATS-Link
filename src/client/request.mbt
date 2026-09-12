///|
fn Client::next_inbox(self : Client) -> String {
  let sequence = self.next_request
  self.next_request = sequence + 1
  "\{self.inbox_prefix}.\{sequence}"
}

///|
/// Publish a request with a unique reply inbox and return its first response.
/// The timeout comes from `Options::new`.
pub async fn Client::request(
  self : Client,
  subject : String,
  payload : BytesView,
  headers? : Headers,
) -> Message {
  guard self.status is Open else { raise ConnectionClosed }
  validate_publish_subject(subject)
  let reply = self.next_inbox()
  let subscription = self.install_subscription(reply, max_messages=1)
  self.publish(subject, payload, reply_to=reply, headers?)
  let response = @async.with_timeout(
    self.request_timeout_ms,
    () => subscription.next(),
    error=Timeout,
  ) catch {
    Timeout => {
      subscription.unsubscribe()
      self.flush()
      raise Timeout
    }
    error => {
      subscription.close_local()
      raise error
    }
  }
  subscription.close_local()
  if response.headers is Some(response_headers) &&
    response_headers.status() == Some(503) {
    raise NoResponders
  }
  response
}
