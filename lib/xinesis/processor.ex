defmodule Xinesis.Processor do
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
    client = Keyword.fetch!(opts, :client)
    stream_arn = Keyword.fetch!(opts, :stream_arn)
    shard_id = Keyword.fetch!(opts, :shard_id)
    backoff_base = Keyword.fetch!(opts, :backoff_base)
    backoff_max = Keyword.fetch!(opts, :backoff_max)

    data = %{
      client: client,
      stream_arn: stream_arn,
      shard_id: shard_id,
      backoff_base: backoff_base,
      backoff_max: backoff_max
    }

    {:ok, :disconnected, data, {:next_event, :internal, {:connect, 0}}}
  end

  @impl true
  def handle_event(:internal, {:connect, failure_count}, :disconnected, data) do
    %{client: client} = data

    case AWS.connect(client) do
      {:ok, conn} ->
        # TODO actually get a lease first
        {:next_state, {:connected, conn}, data, {:next_event, :internal, :get_shard_iterator}}

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

  def handle_event(:internal, :get_shard_iterator, {:connected, conn}, data) do
    %{stream_arn: stream_arn, shard_id: shard_id} = data

    payload = %{
      "StreamARN" => stream_arn,
      "ShardId" => shard_id,
      # TODO make this configurable?
      "ShardIteratorType" => "TRIM_HORIZON"
    }

    case AWS.get_shard_iterator(conn, payload) do
      {:ok, conn, %{"ShardIterator" => shard_iterator}} ->
        {:next_state, {:iterating, conn, shard_iterator}, data,
         {:next_event, :internal, :get_records}}

      {:error, conn, reason} ->
        Logger.error("Failed to get shard iterator: #{Exception.message(reason)}")

        {:next_state, {:connected, conn}, data,
         {{:timeout, :get_shard_iterator}, :timer.seconds(1)}}

      {:disconnect, reason} ->
        Logger.error("Disconnected while getting shard iterator: #{Exception.message(reason)}")
        {:next_state, :disconnected, data, {:next_event, :internal, {:connect, 0}}}
    end
  end

  def handle_event({:timeout, :get_shard_iterator}, _, {:connected, _conn}, _data) do
    {:keep_state_and_data, {:next_event, :internal, :get_shard_iterator}}
  end

  def handle_event(:internal, :get_records, {:iterating, conn, shard_iterator}, data) do
    payload = %{
      "ShardIterator" => shard_iterator,
      # TODO make this configurable?
      "Limit" => 100
    }

    case AWS.get_records(conn, payload) do
      {:ok, conn, response} ->
        IO.inspect(response, label: "Received records")

        next_shard_iterator = response["NextShardIterator"]

        next =
          if next_shard_iterator do
            {{:timeout, :get_records}, :timer.seconds(1)}
          else
            {:next_event, :internal, :finish_shard}
          end

        {:next_state, {:iterating, conn, next_shard_iterator}, data, next}

      {:error, conn, reason} ->
        Logger.error("Failed to get records: #{Exception.message(reason)}")

        {:next_state, {:iterating, conn, shard_iterator}, data,
         {{:timeout, :get_records}, :timer.seconds(1)}}

      {:disconnect, reason} ->
        Logger.error("Disconnected while getting records: #{Exception.message(reason)}")
        {:next_state, :disconnected, data, {:next_event, :internal, {:connect, 0}}}
    end
  end

  def handle_event({:timeout, :get_records}, _, {:iterating, _conn, shard_iterator}, _data) do
    {:keep_state_and_data, {:next_event, :internal, {:get_records, shard_iterator}}}
  end

  defp backoff(base, max, failure_count) do
    factor = :math.pow(2, failure_count)
    max_sleep = trunc(min(max, base * factor))
    :rand.uniform(max_sleep)
  end
end
