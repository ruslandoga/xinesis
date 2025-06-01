defmodule Kinesis do
  @moduledoc """
  Basic AWS Kinesis client.
  """

  require Logger
  alias Mint.HTTP1, as: HTTP

  # use GenServer

  # def start_link(opts \\ []) do
  #   {gen_opts, opts} = Keyword.split(opts, [:name, :debug, :spawn_opt])
  #   GenServer.start_link(__MODULE__, opts, gen_opts)
  # end

  # @impl true
  # def init(opts) do
  #   Process.flag(:trap_exit, true)
  #   processor = Keyword.fetch!(opts, :processor)
  #   stream = Keyword.fetch!(opts, :stream)
  #   table = Keyword.fetch!(opts, :table)

  #   state = %{
  #     conn: nil,
  #     shards: %{},
  #     processor: processor,
  #     stream: stream,
  #     table: table
  #   }

  #   {:ok, state, {:continue, :connect}}
  # end

  # @impl true
  # def handle_continue(:connect, state) do
  #   {:ok, conn} = HTTP.connect(:http, "localhost", 8123, mode: :passive)
  #   {:noreply, %{state | conn: conn}, {:continue, :stream}}
  # end

  # def handle_continue(:stream, state) do
  #   case api_describe_stream_summary(state.conn, %{"StreamName" => state.stream}) do
  #     {:ok, conn, %{}} ->
  #       {:noreply, %{state | conn: conn}, {:continue, :shard}}

  #     # TODO
  #     {:error, error, conn} ->
  #       {:stop, error, %{state | conn: conn}}
  #   end
  # end

  # def handle_continue(:processor, state) do
  #   for shard_id <- Map.keys(state.shards) do
  #     spawn_processor(state, shard_id)
  #   end

  #   {:noreply, state}
  # end

  # @impl true
  # def handle_info({:EXIT, _pid, reason}, state) do
  #   Logger.error("Processor exited with reason: #{inspect(reason)}")
  #   {:noreply, state}
  # end

  # defp spawn_processor(state, shard_id) do
  #   owner = "owner-#{shard_id}"

  #   :proc_lib.spawn_link(__MODULE__, :_, [state.processor])
  # end

  # @doc false
  # def launch_processor() do
  #   #
  # end

  defmodule Error do
    @moduledoc "TODO"
    defexception [:type, :message]

    def message(%{type: nil, message: message}) do
      message
    end

    def message(%{type: type, message: message}) do
      "#{type}: #{message}"
    end
  end

  # https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html
  kinesis_actions = [
    "create_stream",
    "delete_stream",
    "list_streams",
    "describe_stream_summary",
    "list_shards",
    "split_shard",
    "merge_shards",
    "get_shard_iterator",
    "get_records",
    "put_record",
    "put_records"
  ]

  for action <- kinesis_actions do
    @doc false
    def unquote(:"api_#{action}")(conn, payload, opts \\ []) do
      headers = [
        {"x-amz-target", unquote("Kinesis_20131202.#{Macro.camelize(action)}")},
        {"content-type", "application/x-amz-json-1.1"}
      ]

      json = JSON.encode_to_iodata!(payload)
      request("kinesis", conn, headers, json, opts)
    end
  end

  # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations_Amazon_DynamoDB.html
  dynamodb_actions = [
    "create_table",
    "delete_table",
    "put_item",
    "get_item",
    "update_item"
  ]

  for action <- dynamodb_actions do
    @doc false
    def unquote(:"dynamodb_#{action}")(conn, payload, opts \\ []) do
      headers = [
        {"x-amz-target", unquote("DynamoDB_20120810.#{Macro.camelize(action)}")},
        {"content-type", "application/x-amz-json-1.0"}
      ]

      json = JSON.encode_to_iodata!(payload)
      request("dynamodb", conn, headers, json, opts)
    end
  end

  def create_lease(conn, table, shard, owner, opts \\ []) do
    payload = %{
      "TableName" => table,
      "Item" => %{
        "shard_id" => %{"S" => shard},
        "lease_owner" => %{"S" => owner},
        "lease_count" => %{"N" => "1"},
        "completed" => %{"BOOL" => false}
      },
      "ConditionExpression" => "attribute_not_exists(shard_id)"
    }

    dynamodb_put_item(conn, payload, opts)
  end

  def get_lease(conn, table, shard, opts \\ []) do
    payload = %{
      "TableName" => table,
      "Key" => %{"shard_id" => %{"S" => shard}}
    }

    dynamodb_get_item(conn, payload, opts)
  end

  def renew_lease(conn, table, shard, owner, count, opts \\ []) do
    payload = %{
      "TableName" => table,
      "Key" => %{"shard_id" => %{"S" => shard}},
      "UpdateExpression" => "SET lease_count = lease_count + :val",
      "ConditionExpression" => "lease_owner = :owner AND lease_count = :count",
      "ExpressionAttributeValues" => %{
        ":val" => %{"N" => "1"},
        ":owner" => %{"S" => owner},
        ":count" => %{"N" => Integer.to_string(count)}
      },
      "ReturnValues" => "UPDATED_NEW"
    }

    dynamodb_update_item(conn, payload, opts)
  end

  def take_lease(conn, table, shard, new_owner, count, opts \\ []) do
    payload = %{
      "TableName" => table,
      "Key" => %{"shard_id" => %{"S" => shard}},
      "UpdateExpression" => "SET lease_owner = :new_owner, lease_count = lease_count + :val",
      "ConditionExpression" => "lease_count = :count AND lease_owner <> :new_owner",
      "ExpressionAttributeValues" => %{
        ":new_owner" => %{"S" => new_owner},
        ":val" => %{"N" => "1"},
        ":count" => %{"N" => Integer.to_string(count)}
      },
      "ReturnValues" => "UPDATED_NEW"
    }

    dynamodb_update_item(conn, payload, opts)
  end

  def update_checkpoint(conn, table, shard, owner, checkpoint, opts \\ []) do
    payload = %{
      "TableName" => table,
      "Key" => %{"shard_id" => %{"S" => shard}},
      "UpdateExpression" => "SET checkpoint = :checkpoint",
      "ConditionExpression" => "lease_owner = :owner",
      "ExpressionAttributeValues" => %{
        ":checkpoint" => %{"S" => checkpoint},
        ":owner" => %{"S" => owner}
      },
      "ReturnValues" => "UPDATED_NEW"
    }

    dynamodb_update_item(conn, payload, opts)
  end

  def mark_shard_completed(conn, table, shard, owner, opts \\ []) do
    payload = %{
      "TableName" => table,
      "Key" => %{"shard_id" => %{"S" => shard}},
      "UpdateExpression" => "SET completed = :completed",
      "ConditionExpression" => "lease_owner = :owner",
      "ExpressionAttributeValues" => %{
        ":completed" => %{"BOOL" => true},
        ":owner" => %{"S" => owner}
      },
      "ReturnValues" => "UPDATED_NEW"
    }

    dynamodb_update_item(conn, payload, opts)
  end

  # TODO retries, exponential backoff, etc. or should it be handled by the caller (gen_statem)?
  defp request(service, conn, headers, body, opts) do
    with {:ok, conn, _ref} <- send_request(service, conn, headers, body) do
      receive_response(conn, timeout(conn, opts))
    end
  end

  @dialyzer {:no_improper_lists, send_request: 4}
  defp send_request(service, conn, headers, body) do
    # TODO
    access_key_id = "test"
    # TODO
    secret_access_key = "test"
    # TODO
    host = "localhost"
    # TODO
    region = "us-east-1"

    utc_now = DateTime.utc_now(:second)
    amz_date = Calendar.strftime(utc_now, "%Y%m%dT%H%M%SZ")
    amz_short_date = String.slice(amz_date, 0, 8)
    scope = IO.iodata_to_binary([amz_short_date, ?/, region, ?/, service, ?/, "aws4_request"])

    headers =
      Enum.map(headers, fn {k, v} -> {String.downcase(k), v} end)
      |> put_header("host", host)
      |> put_header("x-amz-date", amz_date)

    signed_headers =
      headers
      |> Enum.map(fn {k, _} -> k end)
      |> Enum.intersperse(?;)
      |> IO.iodata_to_binary()

    canonical_request = [
      "POST\n/\n\n",
      Enum.map(headers, fn {k, v} -> [k, ?:, v, ?\n] end),
      ?\n,
      signed_headers,
      "\nUNSIGNED-PAYLOAD"
    ]

    string_to_sign = [
      "AWS4-HMAC-SHA256\n",
      amz_date,
      ?\n,
      scope,
      ?\n,
      hex_sha256(canonical_request)
    ]

    signing_key =
      ["AWS4" | secret_access_key]
      |> hmac_sha256(amz_short_date)
      |> hmac_sha256(region)
      |> hmac_sha256(service)
      |> hmac_sha256("aws4_request")

    signature = hex_hmac_sha256(signing_key, string_to_sign)

    authorization = """
    AWS4-HMAC-SHA256 Credential=#{access_key_id}/#{scope},\
    SignedHeaders=#{signed_headers},\
    Signature=#{signature}\
    """

    headers = [{"authorization", authorization} | headers]

    case HTTP.request(conn, "POST", "/", headers, body) do
      {:ok, _conn, _ref} = ok -> ok
      {:error, conn, reason} -> {:disconnect, reason, conn}
    end
  end

  defp receive_response(conn, timeout) do
    with {:ok, conn, responses} <- recv_all(conn, [], timeout) do
      case responses do
        [status, headers | rest] when status >= 200 and status < 300 ->
          content_type =
            :proplists.get_value("content-type", headers, nil) ||
              raise "missing content-type header"

          String.contains?(content_type, "json") ||
            raise "unexpected content-type: #{content_type}"

          response = JSON.decode!(IO.iodata_to_binary(rest))
          {:ok, conn, response}

        [status, headers | data] when status >= 400 and status < 600 ->
          content_type = :proplists.get_value("content-type", headers, nil)
          error_type = :proplists.get_value("x-amzn-errortype", headers, nil)
          data = IO.iodata_to_binary(data)

          # TODO
          error =
            if is_binary(content_type) and String.contains?(content_type, "json") do
              json = JSON.decode!(data)

              Error.exception(
                type: json["__type"] || error_type,
                message: json["message"] || data
              )
            else
              Error.exception(
                type: error_type || Integer.to_string(status),
                message: data
              )
            end

          {:error, error, conn}
      end
    end
  end

  defp recv_all(conn, acc, timeout) do
    case HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case handle_responses(responses, acc) do
          {:ok, responses} -> {:ok, conn, responses}
          {:more, acc} -> recv_all(conn, acc, timeout)
        end

      {:error, conn, reason, _responses} ->
        {:disconnect, reason, conn}
    end
  end

  # TODO trailers?
  for tag <- [:data, :status, :headers] do
    defp handle_responses([{unquote(tag), _ref, data} | rest], acc) do
      handle_responses(rest, [data | acc])
    end
  end

  defp handle_responses([{:done, _ref}], acc), do: {:ok, :lists.reverse(acc)}
  defp handle_responses([], acc), do: {:more, acc}

  # TODO also consider connect timeout
  # TODO
  defp timeout(_conn, opts), do: Keyword.get(opts, :timeout, :timer.seconds(5))

  defp put_header(headers, key, value), do: [{key, value} | List.keydelete(headers, key, 1)]
  defp hex(value), do: Base.encode16(value, case: :lower)
  defp sha256(value), do: :crypto.hash(:sha256, value)
  defp hmac_sha256(secret, value), do: :crypto.mac(:hmac, :sha256, secret, value)
  defp hex_sha256(value), do: hex(sha256(value))
  defp hex_hmac_sha256(secret, value), do: hex(hmac_sha256(secret, value))
end
