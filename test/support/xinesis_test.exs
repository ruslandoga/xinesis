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

  def await_stream_active(config, stream_arn) do
    %{"StreamDescriptionSummary" => %{"StreamStatus" => stream_status}} =
      with_conn(config, fn conn ->
        Xinesis.AWS.describe_stream_summary(conn, %{"StreamARN" => stream_arn})
      end)

    if stream_status == "ACTIVE" do
      :ok
    else
      Logger.debug("Waiting for stream #{stream_arn} to become active: #{stream_status} ...")
      :timer.sleep(1000)
      await_stream_active(config, stream_arn)
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
