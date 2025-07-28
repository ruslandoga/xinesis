defmodule Xinesis.SomeOtherTest do
  use ExUnit.Case
  alias Xinesis.AWS

  @moduletag :localstack

  @localstack_kinesis [
    scheme: :http,
    host: "localhost",
    port: 4566,
    region: "us-east-1",
    access_key_id: "test",
    secret_access_key: "test"
  ]

  setup do
    stream = "some-other-test-stream-#{System.unique_integer([:positive])}"

    # Xinesis.Test.with_conn(@localstack_kinesis, fn conn ->
    #   AWS.delete_stream(conn, %{"StreamName" => stream})
    # end)

    Xinesis.Test.with_conn(@localstack_kinesis, fn conn ->
      AWS.create_stream(conn, %{"ShardCount" => 4, "StreamName" => stream})
    end)

    on_exit(fn ->
      Xinesis.Test.with_conn(@localstack_kinesis, fn conn ->
        AWS.delete_stream(conn, %{"StreamName" => stream})
      end)
    end)

    %{"StreamDescriptionSummary" => %{"StreamARN" => stream_arn, "StreamStatus" => "ACTIVE"}} =
      Xinesis.Test.with_conn(@localstack_kinesis, fn conn ->
        AWS.describe_stream_summary(conn, %{"StreamName" => stream})
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
