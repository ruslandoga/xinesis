defmodule Xinesis.AWSTest do
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

  test "connect and make some requests" do
    assert {:ok, conn} = AWS.connect(@localstack_kinesis)

    assert {:ok, conn, create_stream_response} =
             AWS.create_stream(conn, %{"ShardCount" => 1, "StreamName" => "aws-api-test-stream"})

    on_exit(fn ->
      Xinesis.Test.with_conn(
        @localstack_kinesis,
        fn conn -> AWS.delete_stream(conn, %{"StreamName" => "aws-api-test-stream"}) end
      )
    end)

    assert create_stream_response == %{}

    assert {:ok, conn, describe_stream_summary_response} =
             AWS.describe_stream_summary(conn, %{"StreamName" => "aws-api-test-stream"})

    assert %{
             "StreamDescriptionSummary" => %{
               "OpenShardCount" => 1,
               "StreamName" => "aws-api-test-stream",
               "ConsumerCount" => 0,
               "EncryptionType" => "NONE",
               "EnhancedMonitoring" => [%{"ShardLevelMetrics" => []}],
               "RetentionPeriodHours" => 24,
               "StreamARN" => stream_arn,
               "StreamCreationTimestamp" => _,
               "StreamModeDetails" => %{"StreamMode" => "PROVISIONED"},
               # TODO await for the stream to be active
               "StreamStatus" => "ACTIVE"
             }
           } = describe_stream_summary_response

    assert {:ok, conn, get_shard_iterator_response} =
             AWS.get_shard_iterator(conn, %{
               "StreamARN" => stream_arn,
               "ShardId" => "shardId-000000000000",
               "ShardIteratorType" => "TRIM_HORIZON"
             })

    assert %{"ShardIterator" => shard_iterator} = get_shard_iterator_response

    assert {:ok, conn, get_records_response} =
             AWS.get_records(conn, %{"ShardIterator" => shard_iterator})

    assert %{
             "MillisBehindLatest" => 0,
             "NextShardIterator" => shard_iterator,
             "Records" => []
           } = get_records_response

    assert {:ok, conn, put_record_response} =
             AWS.put_record(conn, %{
               "StreamARN" => stream_arn,
               "Data" => "testdata",
               "PartitionKey" => "test-key"
             })

    assert %{
             "EncryptionType" => "NONE",
             "SequenceNumber" => sequence_number,
             "ShardId" => "shardId-000000000000"
           } = put_record_response

    assert {:ok, conn, get_records_response} =
             AWS.get_records(conn, %{"ShardIterator" => shard_iterator})

    assert %{
             "MillisBehindLatest" => 0,
             "NextShardIterator" => next_shard_iterator,
             "Records" => [
               %{
                 "ApproximateArrivalTimestamp" => _,
                 "Data" => "testdata",
                 "EncryptionType" => "NONE",
                 "PartitionKey" => "test-key",
                 "SequenceNumber" => ^sequence_number
               }
             ]
           } = get_records_response

    assert {:ok, _conn, get_records_response} =
             AWS.get_records(conn, %{"ShardIterator" => next_shard_iterator})

    assert %{
             "MillisBehindLatest" => 0,
             "NextShardIterator" => _next_shard_iterator,
             "Records" => [
               %{
                 "ApproximateArrivalTimestamp" => _,
                 "Data" => "testdata",
                 "EncryptionType" => "NONE",
                 "PartitionKey" => "test-key",
                 "SequenceNumber" => ^sequence_number
               }
             ]
           } = get_records_response
  end
end
