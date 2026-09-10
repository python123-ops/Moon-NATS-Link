///|
fn expect_invalid_publish_subject(subject : String) -> Unit raise {
  try validate_publish_subject(subject) catch {
    InvalidSubject(value) => @debug.assert_eq(value, subject)
    _ => fail("expected InvalidSubject")
  } noraise {
    _ => fail("expected publish subject rejection")
  }
}

///|
fn expect_invalid_subscription_subject(subject : String) -> Unit raise {
  try validate_subscription_subject(subject) catch {
    InvalidSubject(value) => @debug.assert_eq(value, subject)
    _ => fail("expected InvalidSubject")
  } noraise {
    _ => fail("expected subscription subject rejection")
  }
}

///|
test "accept concrete publish subjects" {
  validate_publish_subject("events.binary")
  validate_publish_subject("tenant-7.metrics.cpu")
}

///|
test "reject publish wildcards whitespace and empty tokens" {
  for
    subject in [
      "", ".events", "events.", "events..cpu", "events *", "jobs.*", "jobs.>",
    ] {
    expect_invalid_publish_subject(subject)
  }
}

///|
test "accept token and tail subscription wildcards" {
  validate_subscription_subject("events.*")
  validate_subscription_subject("tenant.*.metrics")
  validate_subscription_subject("jobs.>")
  validate_subscription_subject(">")
}

///|
test "reject partial and non-tail subscription wildcards" {
  for subject in ["events*", "jobs.>.done", "jobs.worker>", "jobs..done"] {
    expect_invalid_subscription_subject(subject)
  }
}

///|
test "queue groups cannot change SUB token boundaries" {
  validate_queue_group("workers.eu")
  for group in ["", "two groups", "workers*", "workers>"] {
    try validate_queue_group(group) catch {
      InvalidQueueGroup(value) => @debug.assert_eq(value, group)
      _ => fail("expected InvalidQueueGroup")
    } noraise {
      _ => fail("expected queue group rejection")
    }
  }
}

///|
test "request timeout must be positive" {
  try Options::new(request_timeout_ms=0) catch {
    InvalidTimeout(0) => ()
    _ => fail("expected InvalidTimeout(0)")
  } noraise {
    _ => fail("expected invalid request timeout")
  }
  let options = Options::new(request_timeout_ms=75)
  @debug.assert_eq(options.request_timeout_ms, 75)
}
