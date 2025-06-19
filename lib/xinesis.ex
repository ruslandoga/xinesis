defmodule Xinesis do
  @moduledoc """
  Basic AWS Kinesis client.
  """

  require Logger
  alias Mint.HTTP1, as: HTTP

  use GenServer

  def start_link(opts) do
    gen_opts = Keyword.take(opts, [:name, :spawn_opt, :debug])
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    scheme = Keyword.fetch!(opts, :scheme)
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    access_key_id = Keyword.fetch!(opts, :access_key_id)
    # TODO custom Inspect to hide it in logs?
    secret_access_key = Keyword.fetch!(opts, :secret_access_key)
    region = Keyword.fetch!(opts, :region)

    stream =
      cond do
        arn = Keyword.get(opts, :stream_arn) -> {:arn, arn}
        name = Keyword.get(opts, :stream_name) -> {:name, name}
        true -> raise ArgumentError, "either :stream_arn or :stream_name must be provided"
      end

    processor = Keyword.fetch!(opts, :processor)

    unless is_function(processor, 3) do
      raise ArgumentError,
            "processor must be a function that accepts three arguments: shard_id, records, and config"
    end

    processor_config = Keyword.get(opts, :processor_config)

    state = %{
      scheme: scheme,
      host: host,
      port: port,
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region,
      stream: stream,
      backoff: 0,
      conn: nil,
      conn_monitor: nil,
      processor: processor,
      processor_config: processor_config,
      shards: %{}
    }

    {:ok, state, {:continue, :connect}}
  end

  defp inc_backoff(%{backoff: backoff} = state) do
    # TODO jitter
    backoff =
      case backoff do
        0 -> :rand.uniform(300)
        _ -> min(backoff * 2, :timer.seconds(1))
      end

    Logger.debug("Increasing backoff to #{backoff} ms")
    %{state | backoff: backoff}
  end

  defp reset_backoff(state) do
    Logger.debug("Resetting backoff")
    %{state | backoff: 0}
  end

  defp sleep_backoff(state) do
    backoff = state.backoff

    if backoff > 0 do
      Logger.debug("Sleeping for #{backoff} ms before next attempt")
      :timer.sleep(backoff)
    end

    :ok
  end

  @impl true
  def handle_continue(:connect, state) do
    sleep_backoff(state)

    case connect(state) do
      {:ok, conn} ->
        # TODO ssl
        conn_monitor = :inet.monitor(conn.socket)
        state = reset_backoff(state)

        Logger.info("Connected to Kinesis service at #{state.host}:#{state.port}")

        {:noreply, %{state | conn: conn, conn_monitor: conn_monitor},
         {:continue, :await_stream_active}}

      {:error, reason} ->
        Logger.error(
          "Failed to connect to Kinesis service at #{state.host}:#{state.port} - #{inspect(reason)}"
        )

        {:noreply, inc_backoff(state), {:continue, :connect}}
    end
  end

  def handle_continue(:await_stream_active, state) do
    sleep_backoff(state)

    %{conn: conn, stream: stream} = state

    payload =
      case stream do
        {:arn, arn} -> %{"StreamARN" => arn}
        {:name, name} -> %{"StreamName" => name}
      end

    # TODO timeout?
    case Xinesis.api_describe_stream_summary(conn, payload) do
      {:ok, conn, %{"StreamDescriptionSummary" => %{"StreamStatus" => status}}} ->
        state = %{state | conn: conn}

        if status == "ACTIVE" do
          Logger.debug("Stream is now active, status: #{status}")
          {:noreply, reset_backoff(state), {:continue, :list_shards}}
        else
          Logger.info("Stream is not active yet, current status: #{status}")
          {:noreply, inc_backoff(state), {:continue, :await_stream_active}}
        end

      {:error, conn, reason} ->
        Logger.error("Failed to describe stream: #{inspect(reason)}")
        state = %{state | conn: conn}
        {:noreply, inc_backoff(state), {:continue, :await_stream_active}}

      {:disconnect, conn, reason} ->
        Logger.error(
          "Connection lost while waiting for stream to become active: #{inspect(reason)}"
        )

        :inet.cancel_monitor(state.conn_monitor)
        {:ok, conn} = HTTP.close(conn)
        state = %{state | conn: conn, conn_monitor: nil}
        {:noreply, reset_backoff(state), {:continue, :connect}}
    end
  end

  def handle_continue(:list_shards, state) do
    sleep_backoff(state)

    %{conn: conn, stream: stream} = state

    payload =
      case stream do
        {:arn, arn} -> %{"StreamARN" => arn}
        {:name, name} -> %{"StreamName" => name}
      end

    # TODO NextToken
    case Xinesis.api_list_shards(conn, payload) do
      {:ok, conn, %{"Shards" => shards} = response} ->
        Logger.debug("Listed shards: #{inspect(response)}")

        shards =
          Map.new(shards, fn shard ->
            %{"ShardId" => shard_id} = shard
            parent_shard_id = Map.get(shard, "ParentShardId")
            adjacent_parent_shard_id = Map.get(shard, "AdjacentParentShardId")
            parents = Enum.reject([parent_shard_id, adjacent_parent_shard_id], &is_nil/1)
            {shard_id, parents}
          end)

        {:noreply, reset_backoff(%{state | conn: conn, shards: shards}),
         {:continue, :spawn_processors}}

      {:error, conn, reason} ->
        Logger.error("Failed to list shards: #{inspect(reason)}")
        {:noreply, inc_backoff(%{state | conn: conn}), {:continue, :list_shards}}

      {:disconnect, conn, reason} ->
        Logger.error("Connection lost while listing shards: #{inspect(reason)}")
        :inet.cancel_monitor(state.conn_monitor)
        {:ok, conn} = HTTP.close(conn)
        state = %{state | conn: conn, conn_monitor: nil}
        {:noreply, reset_backoff(state), {:continue, :connect}}
    end
  end

  def handle_continue(:spawn_processors, state) do
    # TODO what to do with conn?
    {:noreply, spawn_processors(state)}
  end

  defp spawn_processors(state) do
    shards =
      Map.new(state.shards, fn {shard_id, status} ->
        case status do
          _no_parents_no_pid = [] -> {shard_id, spawn_processor(shard_id, state)}
          _ -> {shard_id, status}
        end
      end)

    %{state | shards: shards}
  end

  defp spawn_processor(shard_id, parent_state) do
    %{
      scheme: scheme,
      host: host,
      port: port,
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region,
      stream: stream,
      processor: processor,
      processor_config: processor_config
    } = parent_state

    child_state =
      %{
        shard_id: shard_id,
        scheme: scheme,
        host: host,
        port: port,
        access_key_id: access_key_id,
        secret_access_key: secret_access_key,
        region: region,
        stream: stream,
        processor: processor,
        processor_config: processor_config,
        conn: nil,
        backoff: 0,
        iterator: nil
      }

    :proc_lib.spawn_opt(__MODULE__, :processor, [child_state], [{:monitor, [{:tag, shard_id}]}])
  end

  @impl true
  def handle_info({{:DOWN, shard_id}, _ref, :process, _pid, reason}, state) do
    state =
      case reason do
        {:completed, child_shards} ->
          Logger.info(
            "Processor for shard #{shard_id} completed with child shards: #{inspect(child_shards)}"
          )

          # TODO update state.shards
          shards =
            Map.new(state.shards, fn {id, status} ->
              if is_list(status) do
                {id, List.delete(status, shard_id)}
              else
                {id, status}
              end
            end)

          spawn_processors(%{state | shards: shards})

          # TODO other cases like lost lease, etc.
      end

    {:noreply, state}
  end

  @doc false
  def processor(state) do
    processor_connect(state)
  end

  defp processor_connect(state) do
    sleep_backoff(state)

    case connect(state) do
      {:ok, conn} ->
        state = %{state | conn: conn}
        processor_get_shard_iterator(reset_backoff(state))

      {:error, reason} ->
        Logger.error(
          "Failed to connect processor for shard #{state.shard_id}: #{inspect(reason)}"
        )

        processor_connect(inc_backoff(state))
    end
  end

  defp processor_get_shard_iterator(state) do
    sleep_backoff(state)

    %{conn: conn, shard_id: shard_id, stream: stream} = state

    payload = %{
      "ShardId" => shard_id,
      # TODO AFTER_SEQUENCE_NUMBER if checkpoint is provided
      "ShardIteratorType" => "TRIM_HORIZON",
      "StreamARN" =>
        case stream do
          {:arn, arn} -> arn
          {:name, _name} -> nil
        end,
      "StreamName" =>
        case stream do
          {:arn, _arn} -> nil
          {:name, name} -> name
        end
    }

    case Xinesis.api_get_shard_iterator(conn, payload) do
      {:ok, conn, %{"ShardIterator" => iterator}} ->
        processor_loop_get_records(reset_backoff(%{state | conn: conn, iterator: iterator}))

      {:error, conn, reason} ->
        Logger.error("Failed to get shard iterator for #{shard_id}: #{inspect(reason)}")
        processor_get_shard_iterator(inc_backoff(%{state | conn: conn}))

      {:disconnect, conn, reason} ->
        Logger.error("Connection lost while getting shard iterator: #{inspect(reason)}")
        {:ok, conn} = HTTP.close(conn)
        state = %{state | conn: conn}
        processor_connect(reset_backoff(state))
    end
  end

  defp processor_loop_get_records(state) do
    sleep_backoff(state)

    %{
      conn: conn,
      stream: stream,
      shard_id: shard_id,
      iterator: iterator,
      processor: processor,
      processor_config: processor_config
    } = state

    processor_state = %{
      conn: conn,
      stream: stream,
      shard_id: shard_id,
      iterator: iterator,
      processor: processor,
      processor_config: processor_config
    }

    {pid, ref} =
      :proc_lib.spawn_opt(__MODULE__, :processor_get_records, [processor_state], [:monitor])

    receive do
      {:DOWN, ^ref, :process, ^pid, reason} ->
        case reason do
          {:continue, conn, _result, next_iterator} ->
            processor_loop_get_records(
              reset_backoff(%{state | conn: conn, iterator: next_iterator})
            )

          {:empty, conn, next_iterator} ->
            processor_loop_get_records(
              inc_backoff(%{state | conn: conn, iterator: next_iterator})
            )

          # TODO
          {:error, conn, reason, next_iterator} ->
            Logger.error("Unexpected error in processor loop: #{inspect(reason)}")

            processor_loop_get_records(
              reset_backoff(%{state | conn: conn, iterator: next_iterator})
            )

          {:completed, _child_shards} = completed ->
            exit(completed)

          {:error, conn, reason} ->
            Logger.error("Processor for shard #{shard_id} failed: #{inspect(reason)}")
            processor_loop_get_records(inc_backoff(%{state | conn: conn}))

          {:disconnect, conn, reason} ->
            Logger.error("Connection lost while processing records: #{inspect(reason)}")
            {:ok, conn} = HTTP.close(conn)
            %{state | conn: conn}
            processor_connect(reset_backoff(state))
        end

      # TODO could be shutdown and stuff
      other ->
        # TODO
        raise "Unexpected message in processor loop: #{inspect(other)}"

        # TODO after timeout?
    end
  end

  @doc false
  def processor_get_records(state) do
    %{
      conn: conn,
      stream: stream,
      shard_id: shard_id,
      iterator: iterator,
      processor: processor,
      processor_config: processor_config
    } = state

    # TODO Limit?
    payload = %{
      "ShardIterator" => iterator,
      "StreamARN" =>
        case stream do
          {:arn, arn} -> arn
          {:name, _name} -> nil
        end,
      "StreamName" =>
        case stream do
          {:arn, _arn} -> nil
          {:name, name} -> name
        end
    }

    case Xinesis.api_get_records(conn, payload) do
      # TODO MillisBehindLatest
      {:ok, conn, response} ->
        Logger.debug("Received records for shard #{shard_id}: #{inspect(response)}")

        case response do
          %{"NextShardIterator" => next_iterator, "Records" => [_ | _] = records} ->
            Logger.warning("Processing records for shard #{shard_id}: #{inspect(records)}")

            {kind, result} =
              try do
                processor.(shard_id, records, processor_config)
              catch
                kind, reason -> {:error, {kind, reason, __STACKTRACE__}}
              else
                result -> {:continue, result}
              end

            Logger.warning("Processed records for shard #{shard_id}: #{inspect(result)}")

            exit({kind, conn, result, next_iterator})

          %{"NextShardIterator" => next_iterator, "Records" => []} ->
            exit({:empty, conn, next_iterator})

          %{"ChildShards" => child_shards} ->
            exit({:completed, child_shards})
        end

      {:error, _conn, _reason} = error ->
        exit(error)

      {:disconnect, _conn, _reason} = error ->
        exit(error)
    end
  end

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

  def connect(state) when is_map(state) do
    %{
      scheme: scheme,
      host: host,
      port: port,
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region
    } = state

    client = %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region
    }

    # TODO allow providing custom connection options and timeout
    with {:ok, conn} <- HTTP.connect(scheme, host, port, mode: :passive) do
      {:ok, HTTP.put_private(conn, :client, client)}
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
    "update_shard_count",
    "get_shard_iterator",
    "get_records",
    "put_record",
    "put_records",
    "deregister_stream_consumer",
    "register_stream_consumer"
  ]

  for action <- kinesis_actions do
    @doc false
    def unquote(:"api_#{action}")(conn, payload, opts \\ []) do
      client = HTTP.get_private(conn, :client)
      json = JSON.encode_to_iodata!(payload)

      headers =
        headers(client, "kinesis", json, [
          {"x-amz-target", unquote("Kinesis_20131202.#{Macro.camelize(action)}")},
          {"content-type", "application/x-amz-json-1.1"},
          {"host", conn.host}
        ])

      request(conn, headers, json, opts)
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
      client = HTTP.get_private(conn, :client)
      json = JSON.encode_to_iodata!(payload)

      headers =
        headers(client, "dynamodb", json, [
          {"x-amz-target", unquote("DynamoDB_20120810.#{Macro.camelize(action)}")},
          {"content-type", "application/x-amz-json-1.0"},
          {"host", conn.host}
        ])

      request(conn, headers, json, opts)
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
  defp request(conn, headers, body, opts) do
    with {:ok, conn, _ref} <- send_request(conn, headers, body) do
      receive_response(conn, timeout(conn, opts))
    end
  end

  defp send_request(conn, headers, body) do
    case HTTP.request(conn, "POST", "/", headers, body) do
      {:ok, _conn, _ref} = ok -> ok
      {:error, conn, reason} -> {:disconnect, conn, reason}
    end
  end

  @dialyzer {:no_improper_lists, headers: 4}
  defp headers(client, service, body, headers) do
    %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region
    } = client

    utc_now = DateTime.utc_now(:second)
    amz_date = Calendar.strftime(utc_now, "%Y%m%dT%H%M%SZ")
    amz_short_date = String.slice(amz_date, 0, 8)
    scope = amz_short_date <> "/" <> region <> "/" <> service <> "/aws4_request"

    amz_content_sha256 = hex_sha256(body)

    headers = [{"x-amz-content-sha256", amz_content_sha256}, {"x-amz-date", amz_date} | headers]
    headers = Enum.sort_by(headers, fn {k, _} -> k end)

    signed_headers =
      headers
      |> Enum.map_intersperse(?;, fn {k, _} -> k end)
      |> IO.iodata_to_binary()

    canonical_request =
      [
        "POST\n/\n\n",
        Enum.map(headers, fn {k, v} -> [k, ?:, v, ?\n] end),
        ?\n,
        signed_headers,
        ?\n,
        amz_content_sha256
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

    authorization =
      """
      AWS4-HMAC-SHA256 Credential=#{access_key_id}/#{scope},\
      SignedHeaders=#{signed_headers},\
      Signature=#{signature}\
      """

    [{"authorization", authorization} | headers]
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

          data = IO.iodata_to_binary(rest)

          response =
            case data do
              "" -> nil
              _ -> JSON.decode!(data)
            end

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

          {:error, conn, error}
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
        {:disconnect, conn, reason}
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

  defp hex(value), do: Base.encode16(value, case: :lower)
  defp sha256(value), do: :crypto.hash(:sha256, value)
  defp hmac_sha256(secret, value), do: :crypto.mac(:hmac, :sha256, secret, value)
  defp hex_sha256(value), do: hex(sha256(value))
  defp hex_hmac_sha256(secret, value), do: hex(hmac_sha256(secret, value))
end
