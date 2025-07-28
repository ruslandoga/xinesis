defmodule Xinesis.Test do
  require Logger

  def with_conn(config, f) when is_function(f, 1) do
    {:ok, conn} = Xinesis.AWS.connect(config)

    try do
      f.(conn)
    else
      {:ok, _conn, response} -> response
      {:error, _conn, reason} -> raise reason
      {:disconnect, reason} -> raise reason
    after
      Mint.HTTP1.close(conn)
    end
  end

  def await_stream_status(config, stream, status) do
    %{"StreamDescriptionSummary" => %{"StreamStatus" => stream_status}} =
      with_conn(config, fn conn ->
        Xinesis.AWS.describe_stream_summary(conn, %{"StreamName" => stream})
      end)

    if stream_status == status do
      :ok
    else
      Logger.debug(
        "Waiting for stream `#{stream}` to change status #{stream_status} -> #{status} ..."
      )

      :timer.sleep(500)
      await_stream_status(config, stream, status)
    end
  end

  def delete_stream(config, stream) do
    with_conn(config, fn conn ->
      Xinesis.AWS.delete_stream(conn, %{"StreamName" => stream})
    end)

    await_stream_deleted(config, stream)
  end

  defp await_stream_deleted(config, stream) do
    :timer.sleep(500)

    try do
      with_conn(config, fn conn ->
        Xinesis.AWS.describe_stream_summary(conn, %{"StreamName" => stream})
      end)
    rescue
      e in Xinesis.AWS.Error ->
        if e.type == "ResourceNotFoundException" do
          :done
        else
          reraise(e, __STACKTRACE__)
        end
    else
      _ ->
        Logger.debug("Stream `#{stream}` still exists, waiting for deletion ...")
        await_stream_deleted(config, stream)
    end
  end

  # This is the IAM policy that I am using for the CI tests.

  # {
  #     "Version": "2012-10-17",
  #     "Statement": [
  #         {
  #             "Sid": "KinesisStreamActions",
  #             "Effect": "Allow",
  #             "Action": "kinesis:*",
  #             "Resource": [
  #                 "arn:aws:kinesis:us-east-1:433301737351:stream/knock-elixir-ci-test-stream-*",
  #                 "arn:aws:kinesis:eu-north-1:433301737351:stream/knock-elixir-ci-test-stream-*"
  #             ]
  #         },
  #         {
  #             "Sid": "DynamoDBTableActions",
  #             "Effect": "Allow",
  #             "Action": [
  #                 "dynamodb:CreateTable",
  #                 "dynamodb:DeleteTable",
  #                 "dynamodb:DescribeTable",
  #                 "dynamodb:GetItem",
  #                 "dynamodb:PutItem",
  #                 "dynamodb:UpdateItem"
  #             ],
  #             "Resource": [
  #                 "arn:aws:dynamodb:us-east-1:433301737351:table/knock-elixir-ci-checkpoint-table-*",
  #                 "arn:aws:dynamodb:eu-north-1:433301737351:table/knock-elixir-ci-checkpoint-table-*"
  #             ]
  #         },
  #         {
  #             "Sid": "DynamoDBListTables",
  #             "Effect": "Allow",
  #             "Action": "dynamodb:ListTables",
  #             "Resource": "*"
  #         }
  #     ]
  # }
end
