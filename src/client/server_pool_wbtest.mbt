///|
test "server options preserve order and copy the caller's array" {
  let input = ["127.0.0.1:4333", "127.0.0.1:4222"]
  let options = Options::new(servers=input)
  input[0] = "127.0.0.1:4999"
  @debug.assert_eq(options.servers, ["127.0.0.1:4333", "127.0.0.1:4222"])
}

///|
test "server options retain the default and single-address call shape" {
  @debug.assert_eq(Options::new().servers, ["127.0.0.1:4222"])
  @debug.assert_eq(Options::new(server="127.0.0.1:4555").servers, [
    "127.0.0.1:4555",
  ])
}

///|
test "server options reject ambiguous or unusable pools" {
  let attempts : Array[(String?, Array[String]?)] = [
    (Some("127.0.0.1:4222"), Some(["127.0.0.1:4333"])),
    (None, Some([])),
    (None, Some([""])),
    (None, Some(["127.0.0.1:4222\n127.0.0.1:4333"])),
  ]
  for (server, servers) in attempts {
    try Options::new(server?, servers?) catch {
      InvalidServerList(_) => ()
      _ => fail("expected InvalidServerList")
    } noraise {
      _ => fail("expected an invalid server pool")
    }
  }
}
