defmodule Xinesis2 do
  @moduledoc """
  Xinesis2: A gen_statem-based Kinesis consumer.

  This implementation uses a `StreamCoordinator` gen_statem to manage shards
  and `ShardProcessor` gen_statems to process records from each shard.
  Record fetching is done asynchronously using a `Task`.
  """

  alias Xinesis2.StreamCoordinator

  def start_link(opts) do
    StreamCoordinator.start_link(opts)
  end
end

defmodule Xinesis2.StreamCoordinator do
  @moduledoc "Manages the Kinesis stream and shard processors."
  use GenStatem, restart: :transient

  require Logger

  # Public API
  def start_link(opts) do
    GenStatem.start_link(__MODULE__, opts, name: opts[:name] || __MODULE__)
  end

  # Callbacks
  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %{
      # Connection config
      client_opts: Keyword.take(opts, [:scheme, :host, :port, :access_key_id, :secret_access_key, :region]),
      # Stream config
      stream:
        cond do
          arn = Keyword.get(opts, :stream_arn) -> {:arn, arn}
          name = Keyword.get(opts, :stream_name) -> {:name, name}
          true -> raise ArgumentError, "either :stream_arn or :stream_name must be provided"
        end,
      # Processor config
      processor: Keyword.fetch!(opts, :processor),
      processor_config: Keyword.get(opts, :processor_config),
      # Internal state
      conn: nil,
      shards: %{}, # %{shard_id => {status, pid | nil}}
      backoff: 0
    }

    {:ok, :connecting, state}
  end

  @impl true
  def callback_mode, do: :state_functions

  # States
  def connecting(event_type, event_content, state) do
    handle_common(event_type, event_content, state, fn state ->
      sleep_backoff(state)

      case Xinesis2.AwsClient.connect(state.client_opts) do
        {:ok, conn} ->
          Logger.info("Coordinator connected.")
          next_state(:listing_shards, %{state | conn: conn, backoff: 0})

        {:error, reason} ->
          Logger.error("Coordinator failed to connect: #{inspect(reason)}")
          next_state(:connecting, inc_backoff(state))
      end
    end)
  end

  def listing_shards(event_type, event_content, state) do
    handle_common(event_type, event_content, state, fn state ->
      sleep_backoff(state)

      case Xinesis2.AwsClient.api_list_shards(state.conn, stream_payload(state.stream)) do
        {:ok, conn, %{"Shards" => shards_data}} ->
          Logger.info("Successfully listed #{length(shards_data)} shards.")

          shards =
            Map.new(shards_data, fn shard ->
              %{"ShardId" => shard_id} = shard
              parent_id = Map.get(shard, "ParentShardId")
              {shard_id, {:unprocessed, parent_id}}
            end)

          next_state(:running, %{state | conn: conn, shards: shards, backoff: 0})

        {:error, conn, reason} ->
          Logger.error("Failed to list shards: #{inspect(reason)}")
          next_state(:listing_shards, %{state | conn: conn}, inc_backoff(state))

        {:disconnect, _conn, reason} ->
          Logger.error("Coordinator disconnected: #{inspect(reason)}")
          next_state(:connecting, %{state | conn: nil})
      end
    end)
  end

  def running(:enter, _old_state, state) do
    spawn_initial_processors(state)
  end

  def running({:info, {:DOWN, _ref, :process, pid, reason}}, _from, state) do
    {shard_id, new_shards} =
      Enum.find_map(state.shards, fn {sid, {_status, p}} ->
        if p == pid, do: {sid, handle_processor_exit(sid, reason, state.shards)}, else: nil
      end)

    Logger.info("Processor for shard #{shard_id} exited. Reason: #{inspect(reason)}")
    spawn_ready_processors(%{state | shards: new_shards})
  end

  def running(event_type, event_content, state) do
    handle_common(event_type, event_content, state, fn state ->
      {:keep_state, state}
    end)
  end

  # State transition helpers
  defp spawn_initial_processors(state) do
    # Spawn processors for shards with no parents (root shards)
    shards_to_spawn =
      Enum.filter(state.shards, fn {_shard_id, {:unprocessed, parent_id}} -> is_nil(parent_id) end)

    new_shards =
      Enum.reduce(shards_to_spawn, state.shards, fn {shard_id, _}, acc ->
        spawn_processor(shard_id, acc, state)
      end)

    {:keep_state, %{state | shards: new_shards}}
  end

  defp spawn_ready_processors(state) do
    # Find shards whose parents are now completed
    parent_ids =
      Map.values(state.shards)
      |> Enum.filter(fn {status, _pid} -> status == :completed end)
      |> Map.new(fn {_status, _pid, shard_id} -> {shard_id, true} end)

    shards_to_spawn =
      Enum.filter(state.shards, fn {_shard_id, {:unprocessed, parent_id}} ->
        Map.has_key?(parent_ids, parent_id)
      end)

    new_shards =
      Enum.reduce(shards_to_spawn, state.shards, fn {shard_id, _}, acc ->
        spawn_processor(shard_id, acc, state)
      end)

    {:keep_state, %{state | shards: new_shards}}
  end

  defp spawn_processor(shard_id, shards, parent_state) do
    processor_opts = [
      shard_id: shard_id,
      stream: parent_state.stream,
      processor: parent_state.processor,
      processor_config: parent_state.processor_config
    ] ++ parent_state.client_opts

    case Xinesis2.ShardProcessor.start_link(processor_opts) do
      {:ok, pid} ->
        Logger.info("Started processor for shard #{shard_id} (pid: #{inspect(pid)})")
        Map.put(shards, shard_id, {:processing, pid})

      {:error, reason} ->
        Logger.error("Failed to start processor for shard #{shard_id}: #{inspect(reason)}")
        shards # Keep as :unprocessed
    end
  end

  defp handle_processor_exit(shard_id, reason, shards) do
    new_status =
      case reason do
        :normal -> {:completed, shard_id}
        _ -> {:failed, shard_id}
      end

    Map.put(shards, shard_id, new_status)
  end

  # Common logic
  defp handle_common(:info, {:EXIT, _from, reason}, state, _func) do
    # Unhandled exit, maybe from a linked process
    {:stop, reason, state}
  end

  defp handle_common(:info, msg, state, _func) do
    Logger.warn("Coordinator received unexpected message: #{inspect(msg)}")
    {:keep_state, state}
  end

  defp handle_common({:call, from}, _event_content, state, _func) do
    GenStatem.reply(from, :ok)
    {:keep_state, state}
  end

  defp handle_common(:state_timeout, _event_content, state, func) do
    func.(state)
  end

  defp handle_common(:internal, _event_content, state, func) do
    func.(state)
  end

  # Helpers
  defp next_state(name, data, timeout \ 0), do: {:next_state, name, data, {:state_timeout, timeout}}
  defp inc_backoff(state), do: %{state | backoff: min((state.backoff * 2) + 100, 10_000)}
  defp sleep_backoff(state), do: if(state.backoff > 0, do: Process.sleep(state.backoff))
  defp stream_payload({:arn, arn}), do: %{"StreamARN" => arn}
  defp stream_payload({:name, name}), do: %{"StreamName" => name}
end

defmodule Xinesis2.ShardProcessor do
  @moduledoc "Processes a single Kinesis shard."
  use GenStatem, restart: :transient

  require Logger

  # Public API
  def start_link(opts) do
    GenStatem.start_link(__MODULE__, opts, [])
  end

  # Callbacks
  @impl true
  def init(opts) do
    state = %{
      # Config
      shard_id: Keyword.fetch!(opts, :shard_id),
      stream: Keyword.fetch!(opts, :stream),
      processor: Keyword.fetch!(opts, :processor),
      processor_config: Keyword.get(opts, :processor_config),
      client_opts: Keyword.take(opts, [:scheme, :host, :port, :access_key_id, :secret_access_key, :region]),
      # Internal state
      conn: nil,
      iterator: nil,
      task_ref: nil,
      backoff: 0
    }

    {:ok, :connecting, state}
  end

  @impl true
  def callback_mode, do: :state_functions

  # States
  def connecting(event_type, event_content, state) do
    handle_common(event_type, event_content, state, fn state ->
      sleep_backoff(state)

      case Xinesis2.AwsClient.connect(state.client_opts) do
        {:ok, conn} ->
          Logger.debug("[#{state.shard_id}] Connected.")
          next_state(:getting_iterator, %{state | conn: conn, backoff: 0})

        {:error, reason} ->
          Logger.error("[#{state.shard_id}] Failed to connect: #{inspect(reason)}")
          next_state(:connecting, inc_backoff(state))
      end
    end)
  end

  def getting_iterator(event_type, event_content, state) do
    handle_common(event_type, event_content, state, fn state ->
      sleep_backoff(state)
      payload = stream_payload(state.stream)
      payload = Map.put(payload, "ShardId", state.shard_id)
      # TODO: Add checkpointing support
      payload = Map.put(payload, "ShardIteratorType", "TRIM_HORIZON")

      case Xinesis2.AwsClient.api_get_shard_iterator(state.conn, payload) do
        {:ok, conn, %{"ShardIterator" => iterator}} ->
          Logger.debug("[#{state.shard_id}] Got initial iterator.")
          next_state(:fetching_records, %{state | conn: conn, iterator: iterator, backoff: 0})

        {:error, conn, reason} ->
          Logger.error("[#{state.shard_id}] Failed to get iterator: #{inspect(reason)}")
          next_state(:getting_iterator, %{state | conn: conn}, inc_backoff(state))

        {:disconnect, _conn, reason} ->
          Logger.error("[#{state.shard_id}] Disconnected: #{inspect(reason)}")
          next_state(:connecting, %{state | conn: nil})
      end
    end)
  end

  def fetching_records(:enter, _old_state, state) do
    # Immediately start fetching
    task = Task.async(fn -> Xinesis2.AwsClient.api_get_records(state.conn, %{"ShardIterator" => state.iterator}) end)
    {:keep_state, %{state | task_ref: task.ref}}
  end

  def fetching_records({:info, {:DOWN, ref, :process, _pid, reason}}, _from, state) do
    if ref != state.task_ref, do: {:keep_state, state}
    else
      handle_fetch_result(reason, state)
    end
  end

  def fetching_records(event_type, event_content, state) do
    handle_common(event_type, event_content, state, fn state -> {:keep_state, state} end)
  end

  # Helpers
  defp handle_fetch_result(result, state) do
    case result do
      {:ok, conn, %{"NextShardIterator" => next_iter, "Records" => records}} when records != [] ->
        Logger.debug("[#{state.shard_id}] Received #{length(records)} records.")
        try do
          state.processor.(state.shard_id, records, state.processor_config)
        catch
          kind, reason -> Logger.error("[#{state.shard_id}] Processor crashed: #{kind} #{inspect(reason)}")
        end
        next_state(:fetching_records, %{state | conn: conn, iterator: next_iter, backoff: 0})

      {:ok, conn, %{"NextShardIterator" => next_iter, "Records" => []}} ->
        Logger.debug("[#{state.shard_id}] Received empty records, waiting.")
        # Kinesis recommends waiting ~1s for TRIM_HORIZON or LATEST
        next_state(:fetching_records, %{state | conn: conn, iterator: next_iter}, 1000)

      {:ok, _conn, %{"NextShardIterator" => nil}} ->
        Logger.info("[#{state.shard_id}] Shard has been closed.")
        {:stop, :normal, state}

      {:error, conn, reason} ->
        Logger.error("[#{state.shard_id}] Error fetching records: #{inspect(reason)}")
        next_state(:fetching_records, %{state | conn: conn}, inc_backoff(state))

      {:disconnect, _conn, reason} ->
        Logger.error("[#{state.shard_id}] Disconnected: #{inspect(reason)}")
        next_state(:connecting, %{state | conn: nil})
    end
  end

  defp handle_common(event_type, event_content, state, func) do
    Xinesis2.StreamCoordinator.handle_common(event_type, event_content, state, func)
  end

  defp next_state(name, data, timeout \ 0), do: {:next_state, name, data, {:state_timeout, timeout}}
  defp inc_backoff(state), do: %{state | backoff: min((state.backoff * 2) + 100, 10_000)}
  defp sleep_backoff(state), do: if(state.backoff > 0, do: Process.sleep(state.backoff))
  defp stream_payload({:arn, arn}), do: %{"StreamARN" => arn}
  defp stream_payload({:name, name}), do: %{"StreamName" => name}
end

defmodule Xinesis2.AwsClient do
  @moduledoc "Handles low-level AWS communication. (Adapted from Xinesis)"
  require Logger
  require Macro
  alias Mint.HTTP1, as: HTTP

  def connect(opts) do
    with {:ok, conn} <- HTTP.connect(opts[:scheme], opts[:host], opts[:port], mode: :passive) do
      client_details = Map.new(Keyword.take(opts, [:access_key_id, :secret_access_key, :region]))
      {:ok, HTTP.put_private(conn, :client, client_details)}
    end
  end

  kinesis_actions = ["list_shards", "get_shard_iterator", "get_records"]
  for action <- kinesis_actions do
    def unquote(:"api_#{action}")(conn, payload, opts \ []) do
      client = HTTP.get_private(conn, :client)
      json = JSON.encode_to_iodata!(payload)
      headers = headers(client, "kinesis", conn.host, json, "Kinesis_20131202.#{Macro.camelize(action)}")
      request(conn, headers, json, opts)
    end
  end

  defp request(conn, headers, body, opts) do
    timeout = Keyword.get(opts, :timeout, 5000)
    with {:ok, conn, _ref} <- HTTP.request(conn, "POST", "/", headers, body) do
      receive_response(conn, timeout)
    end
  end

  defp receive_response(conn, timeout) do
    case HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} -> handle_http_responses(conn, responses)
      {:error, conn, reason} -> {:disconnect, conn, reason}
    end
  end

  defp handle_http_responses(conn, responses) do
    # Simplified response handling
    case Enum.find(responses, fn {type, _, _} -> type == :status end) do
      {:status, _, status_code} when status_code in 200..299 ->
        body =
          responses
          |> Enum.filter_map(fn {type, _, val} -> if type == :data, do: val, else: nil end)
          |> IO.iodata_to_binary()
        {:ok, conn, JSON.decode!(body)}
      _ ->
        {:error, conn, "Request failed: #{inspect(responses)}"}
    end
  end

  defp headers(client, service, host, body, target) do
    amz_date = DateTime.utc_now(:second) |> Calendar.strftime("%Y%m%dT%H%M%SZ")
    amz_short_date = String.slice(amz_date, 0, 8)
    scope = "#{amz_short_date}/#{client.region}/#{service}/aws4_request"
    amz_content_sha256 = :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)

    headers = [
      {"content-type", "application/x-amz-json-1.1"},
      {"host", host},
      {"x-amz-content-sha256", amz_content_sha256},
      {"x-amz-date", amz_date},
      {"x-amz-target", target}
    ]
    |> Enum.sort_by(&elem(&1, 0))

    signed_headers = Enum.map_join(headers, ";", &elem(&1, 0))
    canonical_request = "POST
/

" <> Enum.map_join(headers, "
", fn {k, v} -> "#{k}:#{v}" end) <> "

#{signed_headers}
#{amz_content_sha256}"
    string_to_sign = "AWS4-HMAC-SHA256
#{amz_date}
#{scope}
" <> (:crypto.hash(:sha256, canonical_request) |> Base.encode16(case: :lower))

    k_date = :crypto.mac(:hmac, :sha256, "AWS4" <> client.secret_access_key, amz_short_date)
    k_region = :crypto.mac(:hmac, :sha256, k_date, client.region)
    k_service = :crypto.mac(:hmac, :sha256, k_region, service)
    k_signing = :crypto.mac(:hmac, :sha256, k_service, "aws4_request")
    signature = :crypto.mac(:hmac, :sha256, k_signing, string_to_sign) |> Base.encode16(case: :lower)

    authorization = "AWS4-HMAC-SHA256 Credential=#{client.access_key_id}/#{scope},SignedHeaders=#{signed_headers},Signature=#{signature}"
    [{"authorization", authorization} | headers]
  end
end
