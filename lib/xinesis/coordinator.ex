defmodule Xinesis.Coordinator do
  @moduledoc false
  alias Xinesis.AWS
  require Logger

  @behaviour :gen_statem

  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    {gen_opts, opts} = Keyword.split(opts, [:debug, :trace, :hibernate_after])

    case name do
      nil -> :gen_statem.start_link(__MODULE__, opts, gen_opts)
      _ when is_atom(name) -> :gen_statem.start_link({:local, name}, __MODULE__, opts, gen_opts)
    end
  end

  @impl true
  def callback_mode do
    :handle_event_function
  end

  @impl true
  def init(opts) do
    shard_registry = Keyword.fetch!(opts, :shard_registry)
    shard_supervisor = Keyword.fetch!(opts, :shard_supervisor)
    config = Keyword.fetch!(opts, :config)

    scheme = Keyword.fetch!(config, :scheme)
    host = Keyword.fetch!(config, :host)
    port = Keyword.get(config, :port)
    access_key_id = Keyword.fetch!(config, :access_key_id)
    secret_access_key = Keyword.fetch!(config, :secret_access_key)
    region = Keyword.fetch!(config, :region)

    stream_arn = Keyword.fetch!(config, :stream_arn)

    backoff_base = Keyword.get(config, :backoff_base, 100)
    backoff_max = Keyword.get(config, :backoff_max, 5000)

    data = %{
      client: [
        scheme: scheme,
        host: host,
        port: port,
        access_key_id: access_key_id,
        secret_access_key: secret_access_key,
        region: region
      ],
      stream_arn: stream_arn,
      backoff_base: backoff_base,
      backoff_max: backoff_max,
      shard_registry: shard_registry,
      shard_supervisor: shard_supervisor
    }

    {:ok, :disconnected, data, {:next_event, :internal, {:connect, 0}}}
  end

  @impl true
  def handle_event(:internal, {:connect, failure_count}, :disconnected, data) do
    %{client: client} = data

    case AWS.connect(client) do
      {:ok, conn} ->
        {:next_state, {:connected, conn}, data, {:next_event, :internal, :wait_stream}}

      {:error, reason} ->
        Logger.error("Failed to connect to AWS: #{Exception.message(reason)}")
        %{backoff_base: backoff_base, backoff_max: backoff_max} = data
        delay = backoff(backoff_base, backoff_max, failure_count)
        {:keep_state_and_data, {{:timeout, :reconnect}, delay, failure_count + 1}}
    end
  end

  def handle_event({:timeout, :reconnect}, failure_count, :disconnected, _data) do
    {:keep_state_and_data, {:next_event, :internal, {:connect, failure_count}}}
  end

  def handle_event(:internal, :wait_stream, {:connected, conn}, data) do
    %{stream_arn: stream_arn} = data

    case AWS.describe_stream_summary(conn, %{"StreamARN" => stream_arn}) do
      {:ok, conn, response} ->
        %{"StreamDescriptionSummary" => %{"StreamStatus" => stream_status}} = response
        # CREATING | DELETING | ACTIVE | UPDATING
        # https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescriptionSummary.html#Streams-Type-StreamDescriptionSummary-StreamStatus
        if stream_status == "ACTIVE" do
          {:next_state, {:active, conn}, data, {:next_event, :internal, :list_shards}}
        else
          Logger.info("Stream is not active yet: #{stream_status}")
          {:next_state, {:connected, conn}, data, {{:timeout, :wait_stream}, :timer.seconds(1)}}
        end

      {:error, conn, reason} ->
        Logger.error("Failed to describe stream: #{Exception.message(reason)}")
        {:next_state, {:connected, conn}, data, {{:timeout, :wait_stream}, :timer.seconds(1)}}

      {:disconnect, reason} ->
        Logger.error("Disconnected from AWS: #{Exception.message(reason)}")
        {:next_state, :disconnected, data, {:next_event, :internal, {:connect, 0}}}
    end
  end

  def handle_event({:timeout, :wait_stream}, _, {:connected, _conn}, _data) do
    {:keep_state_and_data, {:next_event, :internal, :wait_stream}}
  end

  def handle_event(:internal, {:list_shards, failure_count}, {:active, conn}, data) do
    %{stream_arn: stream_arn} = data

    case list_all_shards(conn, stream_arn) do
      {:ok, conn, shards} ->
        shards =
          Map.new(shards, fn shard ->
            %{"ShardId" => shard_id} = shard
            parent_shard_id = Map.get(shard, "ParentShardId")
            adjacent_parent_shard_id = Map.get(shard, "AdjacentParentShardId")
            parents = Enum.reject([parent_shard_id, adjacent_parent_shard_id], &is_nil/1)
            {shard_id, parents}
          end)

        {:next_state, {:active, conn}, data,
         {:next_event, :internal, {:start_processors, shards}}}

      {:error, conn, reason} ->
        Logger.error("Failed to list shards: #{Exception.message(reason)}")
        %{backoff_base: backoff_base, backoff_max: backoff_max} = data
        delay = backoff(backoff_base, backoff_max, failure_count)

        {:next_state, {:connected, conn}, data,
         {{:timeout, :list_shards}, delay, failure_count + 1}}

      {:disconnect, reason} ->
        Logger.error("Disconnected from AWS while listing shards: #{Exception.message(reason)}")
        {:next_state, :disconnected, data, {:next_event, :internal, {:connect, 0}}}
    end
  end

  def handle_event({:timeout, :list_shards}, failure_count, {:active, _conn}, _data) do
    {:keep_state_and_data, {:next_event, :internal, {:list_shards, failure_count}}}
  end

  def handle_event(:internal, {:start_processors, shards}, {:active, conn}, data) do
    %{
      shard_registry: shard_registry,
      shard_supervisor: shard_supervisor,
      client: client,
      stream_arn: stream_arn,
      backoff_base: backoff_base,
      backoff_max: backoff_max
    } = data

    for {shard_id, []} <- shards do
      DynamicSupervisor.start_child(
        shard_supervisor,
        {Xinesis.Processor,
         name: {:via, Registry, {shard_registry, shard_id}},
         client: client,
         stream_arn: stream_arn,
         shard_id: shard_id,
         backoff_base: backoff_base,
         backoff_max: backoff_max}
      )
    end

    {:next_state, {:processing, conn, shards}, data}
  end

  defp backoff(base, max, failure_count) do
    factor = :math.pow(2, failure_count)
    max_sleep = trunc(min(max, base * factor))
    :rand.uniform(max_sleep)
  end

  defp list_all_shards(conn, stream_arn) do
    list_all_shards(conn, stream_arn, _acc = [], _next_token = nil)
  end

  defp list_all_shards(conn, stream_arn, acc, next_token) do
    payload = %{"StreamARN" => stream_arn, "NextToken" => next_token}

    with {:ok, conn, response} <- AWS.list_shards(conn, payload) do
      %{"Shards" => shards} = response
      acc = acc ++ shards
      next_token = response["NextToken"]

      if next_token do
        list_all_shards(conn, stream_arn, acc, next_token)
      else
        {:ok, conn, acc}
      end
    end
  end
end
