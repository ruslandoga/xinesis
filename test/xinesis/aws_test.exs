defmodule Xinesis.AWSTest do
  use ExUnit.Case
  alias Xinesis.AWS

  @moduletag :aws

  def kinesis_config do
    [
      scheme: :https,
      host: "kinesis.eu-north-1.api.aws",
      port: 443,
      region: "eu-north-1",
      access_key_id: System.fetch_env!("ACCESS_KEY_ID"),
      secret_access_key: System.fetch_env!("SECRET_ACCESS_KEY")
    ]
  end

  setup_all do
    stream = "knock-elixir-ci-test-stream-api-test"

    Xinesis.Test.with_conn(kinesis_config(), fn conn ->
      AWS.create_stream(conn, %{"ShardCount" => 1, "StreamName" => stream})
    end)

    on_exit(fn -> Xinesis.Test.delete_stream(kinesis_config(), stream) end)

    Xinesis.Test.await_stream_status(kinesis_config(), stream, "ACTIVE")

    %{"StreamDescriptionSummary" => %{"StreamARN" => stream_arn}} =
      Xinesis.Test.with_conn(kinesis_config(), fn conn ->
        AWS.describe_stream_summary(conn, %{"StreamName" => stream})
      end)

    {:ok, stream_arn: stream_arn}
  end

  setup do
    {:ok, conn} = AWS.connect(kinesis_config())
    {:ok, conn: conn}
  end

  test "PutRecord and GetRecords", %{conn: conn, stream_arn: stream_arn} do
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
              "Records" => []
            }} =
             AWS.get_records(conn, %{"ShardIterator" => next_shard_iterator})
  end

  test "AFTER_SEQUENCE_NUMBER", %{conn: conn, stream_arn: stream_arn} do
    assert {:ok, conn, %{"SequenceNumber" => sequence_number_1}} =
             AWS.put_record(conn, %{
               "StreamARN" => stream_arn,
               "Data" => "AA==",
               "PartitionKey" => "test-key-0"
             })

    assert {:ok, conn, %{"SequenceNumber" => _sequence_number_2}} =
             AWS.put_record(conn, %{
               "StreamARN" => stream_arn,
               "Data" => "AQ==",
               "PartitionKey" => "test-key-1"
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
              "Records" => [
                %{"Data" => "AQ==", "PartitionKey" => "test-key-1"}
              ]
            }} =
             AWS.get_records(conn, %{"ShardIterator" => shard_iterator})
  end
end
