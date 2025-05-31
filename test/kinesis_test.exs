defmodule KinesisTest do
  use ExUnit.Case

  test "some requests" do
    {:ok, conn} = Mint.HTTP1.connect(:http, "localhost", 4566, mode: :passive)

    assert {:ok, conn, [200, _headers, body]} = Kinesis.list_streams(conn)

    assert JSON.decode!(body) == %{
             "HasMoreStreams" => false,
             "StreamNames" => ["my-local-stream"]
           }

    assert {:ok, conn, [200, _headers, body]} = Kinesis.describe_stream(conn, "my-local-stream")

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
           } = JSON.decode!(body)

    assert {:ok, conn, [200, _headers, body]} =
             Kinesis.get_shard_iterator(
               conn,
               "my-local-stream",
               "shardId-000000000000",
               %{
                 "ShardIteratorType" => "AT_SEQUENCE_NUMBER",
                 "StartingSequenceNumber" => starting_sequence_number
               }
             )

    assert %{
             "ShardIterator" => _shard_iterator
           } = JSON.decode!(body)

    # just to avoid warnings for now
    _conn = conn
  end
end
