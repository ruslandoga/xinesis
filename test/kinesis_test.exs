defmodule KinesisTest do
  use ExUnit.Case

  test "some requests" do
    {:ok, conn} = Mint.HTTP1.connect(:http, "localhost", 4566, mode: :passive)

    assert {:ok, conn, resp} =
             Kinesis.dynamodb_create_table(conn, %{
               "AttributeDefinitions" => [%{"AttributeName" => "id", "AttributeType" => "S"}],
               "KeySchema" => [%{"AttributeName" => "id", "KeyType" => "HASH"}],
               "ProvisionedThroughput" => %{
                 "ReadCapacityUnits" => 5,
                 "WriteCapacityUnits" => 5
               },
               "TableName" => "my-test-table"
             })

    on_exit(fn ->
      {:ok, conn} = Mint.HTTP1.connect(:http, "localhost", 4566, mode: :passive)
      {:ok, _conn, _resp} = Kinesis.dynamodb_delete_table(conn, %{"TableName" => "my-test-table"})
    end)

    assert %{
             "TableDescription" => %{
               "AttributeDefinitions" => [%{"AttributeName" => "id", "AttributeType" => "S"}],
               "CreationDateTime" => _,
               "DeletionProtectionEnabled" => false,
               "ItemCount" => 0,
               "KeySchema" => [%{"AttributeName" => "id", "KeyType" => "HASH"}],
               "ProvisionedThroughput" => %{
                 "NumberOfDecreasesToday" => 0,
                 "ReadCapacityUnits" => 5,
                 "WriteCapacityUnits" => 5
               },
               "TableArn" => "arn:aws:dynamodb:us-east-1:000000000000:table/my-test-table",
               "TableId" => _,
               "TableName" => "my-test-table",
               "TableSizeBytes" => 0,
               "TableStatus" => "ACTIVE"
             }
           } = resp

    assert {:ok, conn, resp} =
             Kinesis.kinesis_create_stream(conn, %{
               "StreamName" => "my-test-stream",
               "ShardCount" => 1
             })

    on_exit(fn ->
      {:ok, conn} = Mint.HTTP1.connect(:http, "localhost", 4566, mode: :passive)

      {:ok, _conn, _resp} =
        Kinesis.kinesis_delete_stream(conn, %{"StreamName" => "my-test-stream"})
    end)

    assert resp == %{}

    assert {:ok, conn, resp} = Kinesis.kinesis_list_streams(conn, %{})

    assert %{
             "HasMoreStreams" => false,
             "StreamNames" => streams
           } = resp

    assert "my-test-stream" in streams

    assert {:ok, conn, response} =
             Kinesis.kinesis_describe_stream(conn, %{"StreamName" => "my-test-stream"})

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
               "StreamARN" => "arn:aws:kinesis:us-east-1:000000000000:stream/my-test-stream",
               "StreamCreationTimestamp" => _,
               "StreamModeDetails" => %{"StreamMode" => "PROVISIONED"},
               "StreamName" => "my-test-stream",
               # TODO await ACTIVE?
               "StreamStatus" => "CREATING"
             }
           } = response

    # TODO?
    :timer.sleep(500)

    assert {:ok, conn, resp} =
             Kinesis.kinesis_get_shard_iterator(
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

    assert {:ok, conn, resp} =
             Kinesis.kinesis_get_records(conn, %{"ShardIterator" => shard_iterator})

    assert %{
             "MillisBehindLatest" => 0,
             "NextShardIterator" => _next,
             "Records" => []
           } = resp

    assert {:ok, conn, resp} = Kinesis.dynamodb_list_tables(conn, %{})

    assert %{"TableNames" => tables} = resp
    assert "my-test-table" in tables

    # just to avoid warnings for now
    _conn = conn
  end
end
