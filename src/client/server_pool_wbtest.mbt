///|
test "server options preserve order and copy the caller's array" {
  let input = ["127.0.0.1:4333", "127.0.0.1:4222"]
  let options = Options::new(servers=input)
  input[0] = "127.0.0.1:4999"
  @debug.assert_eq(options.server_pool.addresses(), [
    "127.0.0.1:4333", "127.0.0.1:4222",
  ])
}

///|
test "server options retain the default and single-address call shape" {
  @debug.assert_eq(Options::new().server_pool.addresses(), ["127.0.0.1:4222"])
  @debug.assert_eq(
    Options::new(server="127.0.0.1:4555").server_pool.addresses(),
    ["127.0.0.1:4555"],
  )
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

///|
test "INFO discovery keeps configured seeds and removes duplicates" {
  let pool = ServerPool::new(None, Some(["seed-a:4222", "seed-b:4222"]))
  pool.update_from_info(
    "{\"connect_urls\":[\"seed-b:4222\",\"node-c:4222\",\"node-c:4222\"]}",
  )
  @debug.assert_eq(pool.addresses(), [
    "seed-a:4222", "seed-b:4222", "node-c:4222",
  ])
}

///|
test "a later INFO replaces only the discovered portion of the pool" {
  let pool = ServerPool::new(Some("seed:4222"), None)
  pool.update_from_info("{\"connect_urls\":[\"old-a:4222\",\"old-b:4222\"]}")
  pool.update_from_info("{\"connect_urls\":[\"new:4222\",\"seed:4222\"]}")
  @debug.assert_eq(pool.addresses(), ["seed:4222", "new:4222"])
  pool.update_from_info("{\"server_id\":\"unchanged\"}")
  @debug.assert_eq(pool.addresses(), ["seed:4222", "new:4222"])
  let attempted : Map[String, Unit] = Map([])
  attempted["seed:4222"] = ()
  @debug.assert_eq(pool.next_unattempted(attempted), Some("new:4222"))
  pool.update_from_info("{\"connect_urls\":[]}")
  @debug.assert_eq(pool.addresses(), ["seed:4222"])
}

///|
test "reject malformed INFO discovery without retaining partial addresses" {
  let malformed = [
    "not-json", "[]", "{\"connect_urls\":\"node:4222\"}", "{\"connect_urls\":[\"kept:4222\",7]}",
    "{\"connect_urls\":[\"kept:4222\",\"bad\\naddress\"]}",
  ]
  for payload in malformed {
    let pool = ServerPool::new(Some("seed:4222"), None)
    try pool.update_from_info(payload) catch {
      ProtocolViolation(_) => @debug.assert_eq(pool.addresses(), ["seed:4222"])
      _ => fail("expected ProtocolViolation")
    } noraise {
      _ => fail("expected malformed INFO discovery")
    }
  }
}
