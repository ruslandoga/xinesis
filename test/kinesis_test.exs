defmodule KinesisTest do
  use ExUnit.Case

  test "it works" do
    {:ok, _pid} = Kinesis.start_link([])
  end

  test "list_shards/1" do
    {:ok, conn} = Mint.HTTP1.connect(:http, "localhost", 4566, mode: :passive)
    assert {:ok, _conn, [200, _headers, body]} = Kinesis.list_shards(conn)

    assert JSON.decode!(body) == %{
             "HasMoreStreams" => false,
             "StreamNames" => ["my-local-stream"]
           }
  end

  test "describe_stream/2" do
    {:ok, conn} = Mint.HTTP1.connect(:http, "localhost", 4566, mode: :passive)
    assert {:ok, _conn, [200, _headers, body]} = Kinesis.describe_stream(conn, "my-local-stream")

    assert %{
             "StreamDescription" => %{
               "EncryptionType" => "NONE",
               "EnhancedMonitoring" => [%{"ShardLevelMetrics" => []}],
               "HasMoreShards" => false,
               "RetentionPeriodHours" => 24,
               "Shards" => [
                 %{
                   "HashKeyRange" => %{
                     "EndingHashKey" => _,
                     "StartingHashKey" => "0"
                   },
                   "SequenceNumberRange" => %{
                     "StartingSequenceNumber" => _
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
  end
end
