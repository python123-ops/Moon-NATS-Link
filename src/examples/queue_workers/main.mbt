///|
async fn main {
  @client.with_client(
    @client.Options::new(name="moon-nats-link-queue-workers"),
    connection => {
      let worker_a = connection.subscribe("jobs.build", queue_group="workers")
      let worker_b = connection.subscribe("jobs.build", queue_group="workers")
      connection.publish("jobs.build", b"task-a".view())
      let workers : Array[async () -> @client.Message] = [
        () => worker_a.next(),
        () => worker_b.next(),
      ]
      let task = @async.any(workers)
      @debug.assert_eq(task.subject, "jobs.build")
      @debug.assert_eq(task.payload, b"task-a")
      println("queue group ok: one worker claimed task-a")
    },
  )
}
