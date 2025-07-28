defmodule Xinesis.SomeOtherTest do
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
    try do
      Xinesis.Test.with_conn(@localstack_kinesis, fn conn ->
        AWS.delete_stream(conn, %{"StreamName" => "some-other-test-stream"})
      end)
    rescue
      _ -> :ok
    end

    Xinesis.Test.with_conn(@localstack_kinesis, fn conn ->
      AWS.create_stream(conn, %{
        "ShardCount" => 1,
        "StreamName" => "some-other-test-stream"
      })
    end)

    on_exit(fn ->
      Xinesis.Test.with_conn(@localstack_kinesis, fn conn ->
        AWS.delete_stream(conn, %{"StreamName" => "some-other-test-stream"})
      end)
    end)

    %{
      "StreamDescriptionSummary" => %{
        "ConsumerCount" => 0,
        "EncryptionType" => "NONE",
        "EnhancedMonitoring" => [%{"ShardLevelMetrics" => []}],
        "OpenShardCount" => 1,
        "RetentionPeriodHours" => 24,
        "StreamARN" => stream_arn,
        "StreamCreationTimestamp" => _,
        "StreamModeDetails" => %{"StreamMode" => "PROVISIONED"},
        "StreamName" => "some-other-test-stream",
        "StreamStatus" => "ACTIVE"
      }
    } =
      Xinesis.Test.with_conn(@localstack_kinesis, fn conn ->
        AWS.describe_stream_summary(conn, %{"StreamName" => "some-other-test-stream"})
      end)

    {:ok, stream_arn: stream_arn}
  end

  test "it works", %{stream_arn: stream_arn} do
    test = self()

    start_supervised!(
      {Xinesis,
       name: __MODULE__,
       stream_arn: stream_arn,
       scheme: :http,
       host: "localhost",
       port: 4566,
       region: "us-east-1",
       access_key_id: "test",
       secret_access_key: "test",
       processor: fn shard_id, records ->
         send(test, {:records, shard_id, records})
       end}
    )

    Xinesis.Test.with_conn(@localstack_kinesis, fn conn ->
      AWS.put_record(conn, %{
        "StreamARN" => stream_arn,
        "Data" => Base.encode64("some data and stuff"),
        "PartitionKey" => "test-key"
      })
    end)

    assert_receive {:records, "shardId-000000000000", records}

    assert [
             %{
               "ApproximateArrivalTimestamp" => _,
               "Data" => "c29tZSBkYXRhIGFuZCBzdHVmZg==",
               "EncryptionType" => "NONE",
               "PartitionKey" => "test-key",
               "SequenceNumber" => _
             }
           ] = records

    refute_receive _anything, 2000
  end
end
