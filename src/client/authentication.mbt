///|
fn validate_credentials(
  token : String?,
  username : String?,
  password : String?,
) -> @wire.ConnectAuth? raise ClientError {
  match token {
    Some(value) => {
      if username is Some(_) || password is Some(_) {
        raise InvalidCredentials(
          "token cannot be combined with username and password",
        )
      }
      if value == "" {
        raise InvalidCredentials("token must not be empty")
      }
      return Some(Token(value))
    }
    None => ()
  }
  match (username, password) {
    (None, None) => None
    (Some(user), Some(pass)) => {
      if user == "" || pass == "" {
        raise InvalidCredentials("username and password must not be empty")
      }
      Some(UserPassword(user~, password=pass))
    }
    (_, _) =>
      raise InvalidCredentials(
        "username and password must be provided together",
      )
  }
}

///|
fn handshake_error(message : String) -> ClientError {
  match message {
    "Authorization Violation"
    | "'Authorization Violation'"
    | "Authorization Timeout"
    | "'Authorization Timeout'" => AuthenticationFailed
    _ => ServerRejected(message)
  }
}
