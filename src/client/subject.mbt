///|
fn invalid_subject(subject : StringView) -> Unit raise ClientError {
  raise InvalidSubject(subject.to_owned())
}

///|
fn validate_subject_tokens(
  subject : StringView,
) -> Array[StringView] raise ClientError {
  if subject.is_empty() {
    invalid_subject(subject)
  }
  for char in subject {
    if char.is_whitespace() {
      invalid_subject(subject)
    }
  }
  let tokens = subject.split(".").to_array()
  for token in tokens {
    if token.is_empty() {
      invalid_subject(subject)
    }
  }
  tokens
}

///|
fn validate_publish_subject(subject : StringView) -> Unit raise ClientError {
  let tokens = validate_subject_tokens(subject)
  for token in tokens {
    if token.contains("*") || token.contains(">") {
      invalid_subject(subject)
    }
  }
}

///|
fn validate_subscription_subject(
  subject : StringView,
) -> Unit raise ClientError {
  let tokens = validate_subject_tokens(subject)
  for index, token in tokens {
    if token.contains("*") && token != "*" {
      invalid_subject(subject)
    }
    if token.contains(">") && (token != ">" || index != tokens.length() - 1) {
      invalid_subject(subject)
    }
  }
}

///|
fn validate_queue_group(group : StringView) -> Unit raise ClientError {
  if group.is_empty() || group.contains("*") || group.contains(">") {
    raise InvalidQueueGroup(group.to_owned())
  }
  for char in group {
    if char.is_whitespace() {
      raise InvalidQueueGroup(group.to_owned())
    }
  }
}
