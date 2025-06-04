plan:
- coordinator-like process that waits for the stream to become ACTIVE and lists shards and computes dependencies (I think plain map `%{shard_id => [...dependency]}` can be used in place of digraph) and spawns shard processors
- each shard processor first connects to dynamo and attempts to acquire a lease:
  - if shard turns out to be "completed" -> `exit(:completed)` -> coordinator checks if any shards were dependent on that shard and starts processors for those
  - if shard already locked -> `exit(:locked)` -> coordinator schedules a message to itself to spawn another processor for that shard in ~lease duration +- some jitter (in case the app that locked that shard goes down)
  - if shard lease acquired -> processor connects to kinesis, `GetShardIterator` (`TRIM_HORIZON` if shard is new, else `AFTER_SEQUENCE_NUMBER`) and starts to `GetRecords` until split or merge or lease is lost, checkpointing progress after each batch
    - in case of split we get two child shard that can immediately be started -> `exit({:split, [new_shards]})` or attempt to acquire the lease and start processing the first one (to reuse kinesis connection) and send the other for scheduling to the coordinator
    - in case of merge need to wait for the other shard to finish processing -> poll dynamodb or dynamodb streams?
      - once another shard is done processing -> attempt to acquire the lease for the new merged shard and start processing -> etc.

thoughts:
- where should `GetRecords` be called and where should the record processing be done? right in the shard processor or in a spawned process? I'm leaning towards the latter since the new one-off process can get access to the Mint connection for the `GetRecords` request and it might help keep garbage collection in the shard processor itself minimal
- opening and closing connections to dynamodb (just to learn that a shard is completed or locked) seems wasteful, and since it would be abstracted as adapter, maybe dynamodb requests can be done via an external connection pool
- how to deal with "poison pills" in batches, delegate it to the caller? recursively bisect the batch or process in `try` blocks? dead letter queue?
- if stream doesn't exist, if dynamodb table cannot be created (maybe due to an iam role issue, where the creds only have kinesis access), ...?

possible API:
- module callback style like `Postgrex.ReplicationConnection` or Oban worker
- function callback style like `:telemetry`

  ```elixir
  defmodule Events do
    def process(shard_id, records, _config = nil) do
      # ...
    end
  end

  _supervisor_child = {Kinesis, stream: "some/stream/arn", consumer: {&Events.process/3, _config = nil}}
  Kinesis.start_consumer("some/stream/arn", &Events.process/3, _config = nil)
  ```

TODOs / to check out:
- [ ] https://docs.localstack.cloud/user-guide/aws/kinesis/
- [ ] https://docs.localstack.cloud/user-guide/aws/dynamodb/
- [ ] https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
- [ ] https://gist.github.com/etspaceman/137f7f540af32bf106873813c830a699
- [ ] https://docs.aws.amazon.com/streams/latest/dev/introduction.html
- [ ] https://github.com/uberbrodt/kcl_ex
- [ ] https://github.com/awslabs/amazon-kinesis-client (tests)
- [ ] https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations_Amazon_DynamoDB.html for leases and checkpoints
- [ ] https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Operations.html
