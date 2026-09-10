///|
test "connection credentials select one CONNECT authentication shape" {
  let anonymous = Options::new()
  guard anonymous.auth is None else { fail("expected an anonymous CONNECT") }
  let token = Options::new(token="token-value")
  match token.auth {
    Some(Token(value)) => @debug.assert_eq(value, "token-value")
    _ => fail("expected token authentication")
  }
  let user = Options::new(username="service", password="secret")
  match user.auth {
    Some(UserPassword(user~, password~)) => {
      @debug.assert_eq(user, "service")
      @debug.assert_eq(password, "secret")
    }
    _ => fail("expected username and password authentication")
  }
}

///|
test "reject ambiguous or incomplete credentials without exposing values" {
  for
    options in [
      (Some("token-value"), Some("service"), Some("secret")),
      (Some("token-value"), Some("service"), None),
      (None, Some("service"), None),
      (None, None, Some("secret")),
      (Some(""), None, None),
      (None, Some(""), Some("secret")),
      (None, Some("service"), Some("")),
    ] {
    let (token, username, password) = options
    try Options::new(token?, username?, password?) catch {
      InvalidCredentials(message) => {
        guard !message.contains("token-value") &&
          !message.contains("service") &&
          !message.contains("secret") else {
          fail("credential validation exposed an input value")
        }
      }
      _ => fail("expected InvalidCredentials")
    } noraise {
      _ => fail("expected invalid credential combination")
    }
  }
}

///|
test "map handshake authorization errors without retaining server text" {
  guard handshake_error("'Authorization Violation'") is AuthenticationFailed else {
    fail("expected AuthenticationFailed")
  }
  match handshake_error("'Maximum Connections Exceeded'") {
    ServerRejected(message) =>
      @debug.assert_eq(message, "'Maximum Connections Exceeded'")
    _ => fail("expected other server errors to retain their message")
  }
}
