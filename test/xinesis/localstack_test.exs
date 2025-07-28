defmodule Xinesis.LocalStackTest do
  use ExUnit.Case
  alias Xinesis.AWS

  @localstack_kinesis [
    scheme: :http,
    host: "localhost",
    port: 4566,
    region: "us-east-1",
    access_key_id: "test",
    secret_access_key: "test"
  ]

  setup_all do
    stream = "aws-api-test-stream"

    Xinesis.Test.with_conn(
      @localstack_kinesis,
      fn conn -> AWS.create_stream(conn, %{"ShardCount" => 1, "StreamName" => stream}) end
    )

    on_exit(fn ->
      Xinesis.Test.with_conn(
        @localstack_kinesis,
        fn conn -> AWS.delete_stream(conn, %{"StreamName" => stream}) end
      )
    end)

    assert %{
             "StreamDescriptionSummary" => %{
               "StreamARN" => stream_arn,
               "StreamStatus" => "ACTIVE"
             }
           } =
             Xinesis.Test.with_conn(
               @localstack_kinesis,
               fn conn -> AWS.describe_stream_summary(conn, %{"StreamName" => stream}) end
             )

    {:ok, stream_arn: stream_arn}
  end

  setup do
    {:ok, conn} = AWS.connect(@localstack_kinesis)
    {:ok, conn: conn}
  end

  test "connect and make some requests", %{conn: conn, stream_arn: stream_arn} do
    assert {:ok, conn, %{"ShardIterator" => shard_iterator}} =
             AWS.get_shard_iterator(conn, %{
               "StreamARN" => stream_arn,
               "ShardId" => "shardId-000000000000",
               "ShardIteratorType" => "LATEST"
             })

    assert {:ok, conn, %{"SequenceNumber" => sequence_number}} =
             AWS.put_record(conn, %{
               "StreamARN" => stream_arn,
               "Data" => "AA==",
               "PartitionKey" => "test-key"
             })

    assert {:ok, conn,
            %{
              "MillisBehindLatest" => 0,
              "NextShardIterator" => next_shard_iterator,
              "Records" => [
                %{
                  "ApproximateArrivalTimestamp" => _,
                  "Data" => "AA==",
                  "PartitionKey" => "test-key",
                  "SequenceNumber" => ^sequence_number
                }
              ]
            }} =
             AWS.get_records(conn, %{"ShardIterator" => shard_iterator})

    assert {:ok, _conn,
            %{
              "MillisBehindLatest" => 0,
              "NextShardIterator" => _next_shard_iterator,
              # TODO why not empty?
              "Records" => [
                %{
                  "ApproximateArrivalTimestamp" => _,
                  "Data" => "AA==",
                  "PartitionKey" => "test-key",
                  "SequenceNumber" => ^sequence_number
                }
              ]
            }} =
             AWS.get_records(conn, %{"ShardIterator" => next_shard_iterator})
  end

  test "AFTER_SEQUENCE_NUMBER", %{conn: conn, stream_arn: stream_arn} do
    assert {:ok, conn, %{"SequenceNumber" => sequence_number_1}} =
             AWS.put_record(conn, %{
               "StreamARN" => stream_arn,
               "Data" => "AA==",
               "PartitionKey" => "test-key"
             })

    assert {:ok, conn, %{"SequenceNumber" => sequence_number_2}} =
             AWS.put_record(conn, %{
               "StreamARN" => stream_arn,
               "Data" => "AQ==",
               "PartitionKey" => "test-key"
             })

    assert {:ok, conn, %{"ShardIterator" => shard_iterator}} =
             AWS.get_shard_iterator(conn, %{
               "StreamARN" => stream_arn,
               "ShardId" => "shardId-000000000000",
               "ShardIteratorType" => "AFTER_SEQUENCE_NUMBER",
               "StartingSequenceNumber" => sequence_number_1
             })

    assert {:ok, _conn,
            %{
              "MillisBehindLatest" => 0,
              "NextShardIterator" => _next_shard_iterator,
              # TODO why empty?
              "Records" => []
            }} =
             AWS.get_records(conn, %{"ShardIterator" => shard_iterator})
  end
end
