defmodule KinesisTest do
  use ExUnit.Case

  test "some requests" do
    {:ok, conn} = Mint.HTTP1.connect(:http, "localhost", 4566, mode: :passive)

    assert {:ok, conn, resp} = Kinesis.list_streams(conn, %{})

    assert %{
             "HasMoreStreams" => false,
             "StreamNames" => ["my-local-stream"]
           } = resp

    assert {:ok, conn, response} =
             Kinesis.describe_stream(conn, %{"StreamName" => "my-local-stream"})

    assert %{
             "StreamDescription" => %{
               "EncryptionType" => "NONE",
               "EnhancedMonitoring" => [%{"ShardLevelMetrics" => []}],
               "HasMoreShards" => false,
               "RetentionPeriodHours" => 24,
               "Shards" => [
                 %{
                   "HashKeyRange" => %{
                     "EndingHashKey" => _ending_hash_key,
                     "StartingHashKey" => "0"
                   },
                   "SequenceNumberRange" => %{
                     "StartingSequenceNumber" => starting_sequence_number
                   },
                   "ShardId" => "shardId-000000000000"
                 }
               ],
               "StreamARN" => "arn:aws:kinesis:us-east-1:000000000000:stream/my-local-stream",
               "StreamCreationTimestamp" => _,
               "StreamModeDetails" => %{"StreamMode" => "PROVISIONED"},
               "StreamName" => "my-local-stream",
               "StreamStatus" => "ACTIVE"
             }
           } = response

    assert {:ok, conn, resp} =
             Kinesis.get_shard_iterator(
               conn,
               %{
                 "StreamName" => "my-local-stream",
                 "ShardId" => "shardId-000000000000",
                 "ShardIteratorType" => "AT_SEQUENCE_NUMBER",
                 "StartingSequenceNumber" => starting_sequence_number
               }
             )

    assert %{
             "ShardIterator" => shard_iterator
           } = resp

    assert {:ok, conn, resp} = Kinesis.get_records(conn, %{"ShardIterator" => shard_iterator})

    assert %{
             "MillisBehindLatest" => 0,
             "NextShardIterator" => _next,
             "Records" => []
           } = resp

    # just to avoid warnings for now
    _conn = conn
  end
end
