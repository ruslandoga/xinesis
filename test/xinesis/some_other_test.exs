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
        "ShardCount" => 4,
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
        "OpenShardCount" => 4,
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
    start_supervised!(
      {Xinesis,
       name: __MODULE__,
       stream_arn: stream_arn,
       scheme: :http,
       host: "localhost",
       port: 4566,
       region: "us-east-1",
       access_key_id: "test",
       secret_access_key: "test"}
    )

    Xinesis.Test.with_conn(@localstack_kinesis, fn conn ->
      AWS.put_record(conn, %{
        "StreamARN" => stream_arn,
        "Data" => "testdata",
        "PartitionKey" => "test-key"
      })
    end)

    :timer.sleep(:timer.seconds(10))
  end
end
