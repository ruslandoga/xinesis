defmodule Xinesis.SomeOtherTest do
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

  setup do
    stream = "knock-elixir-ci-test-stream-some-other-test"

    Xinesis.Test.with_conn(kinesis_config(), fn conn ->
      AWS.create_stream(conn, %{"ShardCount" => 1, "StreamName" => stream})
    end)

    on_exit(fn ->
      Xinesis.Test.with_conn(kinesis_config(), fn conn ->
        AWS.delete_stream(conn, %{"StreamName" => stream})
      end)
    end)

    %{"StreamDescriptionSummary" => %{"StreamARN" => stream_arn}} =
      Xinesis.Test.with_conn(kinesis_config(), fn conn ->
        AWS.describe_stream_summary(conn, %{"StreamName" => stream})
      end)

    Xinesis.Test.await_stream_active(kinesis_config(), stream_arn)

    {:ok, stream_arn: stream_arn}
  end

  test "it works", %{stream_arn: stream_arn} do
    test = self()

    config =
      Keyword.merge(kinesis_config(),
        name: __MODULE__,
        stream_arn: stream_arn,
        processor: fn shard_id, records ->
          send(test, {:records, shard_id, records})
        end
      )

    start_supervised!({Xinesis, config})

    Xinesis.Test.with_conn(kinesis_config(), fn conn ->
      AWS.put_record(conn, %{
        "StreamARN" => stream_arn,
        "Data" => "AA==",
        "PartitionKey" => "test-key-0"
      })
    end)

    assert_receive {:records, "shardId-000000000000", records}, :timer.seconds(5)

    assert [
             %{
               "ApproximateArrivalTimestamp" => _,
               "Data" => "AA==",
               "PartitionKey" => "test-key-0",
               "SequenceNumber" => _
             }
           ] = records

    assert_receive {:records, "shardId-000000000000", []}, :timer.seconds(5)
  end
end
