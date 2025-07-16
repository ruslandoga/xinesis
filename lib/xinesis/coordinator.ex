defmodule Xinesis.Coordinator do
  @moduledoc """
  Responsible for coordinating the processing of records across multiple shards.
  """

  @behaviour :gen_statem
  alias Xinesis.AWS
  require Logger

  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    {gen_opts, opts} = Keyword.split(opts, [])

    case name do
      nil ->
        :gen_statem.start_link(__MODULE__, opts, gen_opts)

      _ when is_atom(name) ->
        :gen_statem.start_link({:local, name}, __MODULE__, opts, gen_opts)
        # TODO other cases
    end
  end

  @impl true
  def callback_mode do
    :handle_event_function
  end

  @impl true
  def init(opts) do
    # TODO maybe don't need this?
    Process.flag(:trap_exit, true)

    scheme = Keyword.fetch!(opts, :scheme)
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    access_key_id = Keyword.fetch!(opts, :access_key_id)
    # TODO custom Inspect to hide it in logs?
    secret_access_key = Keyword.fetch!(opts, :secret_access_key)
    region = Keyword.fetch!(opts, :region)

    client = %{
      scheme: scheme,
      host: host,
      port: port,
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region
    }

    stream_arn = Keyword.fetch!(opts, :stream_arn)

    processor = Keyword.fetch!(opts, :processor)

    unless is_function(processor, 3) do
      raise ArgumentError,
            "processor must be a function that accepts three arguments: shard_id, records, and config"
    end

    processor_config = Keyword.get(opts, :processor_config)

    data = %{
      client: client,
      stream_arn: stream_arn,
      processor: [processor | processor_config],
      shards: %{},
      backoff_base: Keyword.get(opts, :backoff_base) || 100,
      backoff_max: Keyword.get(opts, :backoff_max) || 5000
    }

    {:ok, :disconnected, data, {:next_event, :internal, {:connect, 0}}}
  end

  @impl true
  def handle_event(:internal, {:connect, failure_count}, :disconnected, data) do
    %{client: client} = data

    case AWS.connect(client) do
      # TODO monitor connection
      {:ok, conn} ->
        {:next_state, {:connected, conn}, data, {:next_event, :internal, :wait_stream}}

      {:error, reason} ->
        Logger.warning("Failed to connect to AWS: #{Exception.message(reason)}")
        %{backoff_base: backoff_base, backoff_max: backoff_max} = data
        delay = backoff(backoff_base, backoff_max, failure_count)
        {:keep_state_and_data, {{:timeout, :reconnect}, delay, failure_count + 1}}
    end
  end

  def handle_event({:timeout, :reconnect}, failure_count, :disconnected, _data) do
    {:keep_state_and_data, {:next_event, :internal, {:connect, failure_count}}}
  end

  @wait_stream_delay :timer.seconds(1)

  def handle_event(:internal, :wait_stream, {:connected, conn}, data) do
    %{client: client, stream_arn: stream_arn} = data

    case AWS.kinesis_describe_stream_summary(conn, client, %{"StreamARN" => stream_arn}) do
      {:ok, conn, response} ->
        %{"StreamDescriptionSummary" => %{"StreamStatus" => stream_status}} = response

        if stream_status == "ACTIVE" do
          {:next_state, {:connected, conn}, data, {:next_event, :internal, :list_shards}}
        else
          Logger.warning("Stream is not active yet: #{stream_status}")
          {:next_state, {:connected, conn}, data, {{:timeout, :wait_stream}, @wait_stream_delay}}
        end

      {:error, conn, reason} ->
        Logger.warning("Failed to describe stream: #{Exception.message(reason)}")
        {:next_state, {:connected, conn}, data, {{:timeout, :wait_stream}, @wait_stream_delay}}

      {:disconnect, reason} ->
        Logger.error("Disconnected from AWS: #{Exception.message(reason)}")
        {:next_state, :disconnected, data, {:next_event, :internal, {:connect, 0}}}
    end
  end

  def handle_event({:timeout, :wait_stream}, _, {:connected, _conn}, _data) do
    {:keep_state_and_data, {:next_event, :internal, :wait_stream}}
  end

  def handle_event(:internal, {:list_shards, failure_count}, {:connected, conn}, data) do
    %{client: client, stream_arn: stream_arn} = data

    case list_all_shards(conn, client, %{"StreamARN" => stream_arn}) do
      {:ok, conn, shards} ->
        shards =
          Map.new(shards, fn shard ->
            %{"ShardId" => shard_id} = shard
            parent_shard_id = Map.get(shard, "ParentShardId")
            adjacent_parent_shard_id = Map.get(shard, "AdjacentParentShardId")
            parents = Enum.reject([parent_shard_id, adjacent_parent_shard_id], &is_nil/1)
            {shard_id, parents}
          end)

        data = %{data | shards: shards}
        {:next_state, {:connected, conn}, data, {:next_event, :internal, :start_processors}}

      {:error, conn, reason} ->
        Logger.warning("Failed to list shards: #{Exception.message(reason)}")
        %{backoff_base: backoff_base, backoff_max: backoff_max} = data
        delay = backoff(backoff_base, backoff_max, failure_count)

        {:next_state, {:connected, conn}, data,
         {{:timeout, :list_shards}, delay, failure_count + 1}}

      {:disconnect, reason} ->
        Logger.error("Disconnected from AWS: #{Exception.message(reason)}")
        {:next_state, :disconnected, data, {:next_event, :internal, {:connect, 0}}}
    end
  end

  def handle_event({:timeout, :list_shards}, failure_count, {:connected, _conn}, _data) do
    {:keep_state_and_data, {:next_event, :internal, {:list_shards, failure_count}}}
  end

  # TODO filter out completed shards using DynamoDB or other adapter
  # TODO check if our node can start the shard (see http://github.com/knocklabs/kcl_ex)
  def handle_event(:internal, :start_processors, {:connected, _conn}, data) do
    %{
      stream_arn: stream_arn,
      shards: shards,
      processor: processor,
      client: client,
      backoff_base: backoff_base,
      backoff_max: backoff_max
    } = data

    shards =
      Map.new(shards, fn {shard_id, status} ->
        case status do
          _no_parents = [] ->
            # TODO spawn_opt with tag etc.?
            # TODO start under a dynamic supervisor?
            {:ok, pid} =
              Xinesis.Processor.start_link(
                client: client,
                stream_arn: stream_arn,
                shard_id: shard_id,
                processor: processor,
                backoff_base: backoff_base,
                backoff_max: backoff_max
              )

            {shard_id, _new_status = pid}

          _ ->
            {shard_id, status}
        end
      end)

    {:keep_state, %{data | shards: shards}}
  end

  # TODO attempt to restart processor unless finished etc.
  def handle_event(:info, {:EXIT, pid, reason}, _state, _data) do
    Logger.error("Processor #{inspect(pid)} exited with reason: #{inspect(reason)}")
    :keep_state_and_data
  end

  defp backoff(base_backoff, max_backoff, failure_count) do
    factor = :math.pow(2, failure_count)
    max_sleep = trunc(min(max_backoff, base_backoff * factor))
    :rand.uniform(max_sleep)
  end

  defp list_all_shards(conn, client, stream_arn) do
    list_all_shards(conn, client, stream_arn, _acc = [], _next_token = nil)
  end

  # TODO can resume from the last next_token on error
  defp list_all_shards(conn, client, stream_arn, acc, next_token) do
    payload = %{"StreamARN" => stream_arn, "NextToken" => next_token}

    with {:ok, conn, response} <- AWS.kinesis_list_shards(conn, client, payload) do
      %{"Shards" => shards} = response
      acc = acc ++ shards

      if response["NextToken"] do
        list_all_shards(conn, client, stream_arn, acc, next_token)
      else
        {:ok, conn, acc}
      end
    end
  end
end
