defmodule Xinesis4 do
  @moduledoc """
  Xinesis4: A Kinesis consumer using :atomics for decentralized, non-blocking back-pressure.

  This implementation uses a shared atomic counter to manage concurrency. Shard
  processors attempt to "claim" a processing slot by decrementing the counter.
  This avoids a GenServer bottleneck for permit management, but requires the
  processors to manage their own retry/backoff logic if they fail to acquire a slot.
  """

  alias Xinesis4.StreamCoordinator

  def start_link(opts) do
    StreamCoordinator.start_link(opts)
  end
end

defmodule Xinesis4.StreamCoordinator do
  @moduledoc """
  Manages shard lifecycles and the shared atomic counter.
  """
  @behaviour :gen_statem
  require Logger

  # Public API
  def start_link(opts) do
    :gen_statem.start_link(__MODULE__, opts, name: opts[:name] || __MODULE__)
  end

  # Callbacks
  @impl true
  def callback_mode, do: :handle_event_function

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    max_fetches = Keyword.get(opts, :max_concurrent_fetches, 10)
    # The atomics array will serve as our semaphore/permit counter.
    # It's shared across all processes.
    permits_ref = :atomics.new(1, signed: false)
    :atomics.put(permits_ref, 1, max_fetches)

    state = %{
      stream_identifier: Keyword.fetch!(opts, :stream_identifier),
      processor_module: Keyword.fetch!(opts, :processor_module),
      processor_config: Keyword.get(opts, :processor_config),
      permits_ref: permits_ref,
      shards: %{}
    }

    {:ok, :discovering_shards, state, {:next_event, :internal, :discover_shards}}
  end

  @impl true
  def handle_event(:internal, :discover_shards, :discovering_shards, state) do
    Logger.info("Discovering shards...")

    case Xinesis4.AwsClient.list_shards(state.stream_identifier) do
      {:ok, shards_data} ->
        Logger.info("Discovered #{length(shards_data)} shards.")
        new_state = spawn_processors_for_shards(shards_data, state)
        {:next_state, :running, new_state}

      {:error, reason} ->
        Logger.error("Failed to list shards: #{inspect(reason)}. Retrying...")
        {:keep_state, state, {{:timeout, :retry}, 5000, :discover_shards}}
    end
  end

  @impl true
  def handle_event({:timeout, :retry}, :discover_shards, :discovering_shards, state) do
    {:keep_state, state, {:next_event, :internal, :discover_shards}}
  end

  @impl true
  def handle_event(:info, {:EXIT, pid, reason}, _any_state, state) do
    case Enum.find(state.shards, fn {_, p} -> p == pid end) do
      {shard_id, _} ->
        Logger.warn("Shard processor for #{shard_id} exited: #{inspect(reason)}")
        # In a real app, you'd handle respawning or resharding.
        new_shards = Map.delete(state.shards, shard_id)
        {:keep_state, %{state | shards: new_shards}}

      nil ->
        {:keep_state, state}
    end
  end

  # Catch-all for other events
  @impl true
  def handle_event(_event_type, _event_content, _state, data) do
    {:keep_state, data}
  end

  # Internal
  defp spawn_processors_for_shards(shards_data, state) do
    shards_data
    |> Enum.reduce(state, fn %{"ShardId" => shard_id}, acc_state ->
      opts = [
        coordinator: self(),
        shard_id: shard_id,
        stream_identifier: acc_state.stream_identifier,
        processor_module: acc_state.processor_module,
        processor_config: acc_state.processor_config,
        permits_ref: acc_state.permits_ref
      ]

      if Map.has_key?(acc_state.shards, shard_id) do
        acc_state
      else
        case Xinesis4.ShardProcessor.start_link(opts) do
          {:ok, pid} ->
            Logger.info("Started processor for shard #{shard_id}")
            %{acc_state | shards: Map.put(acc_state.shards, shard_id, pid)}

          {:error, reason} ->
            Logger.error("Failed to start processor for #{shard_id}: #{inspect(reason)}")
            acc_state
        end
      end
    end)
  end
end

defmodule Xinesis4.ShardProcessor do
  @moduledoc "A shard processor that uses an atomic counter to grab a fetch permit."
  @behaviour :gen_statem
  require Logger

  def start_link(opts), do: :gen_statem.start_link(__MODULE__, opts, [])

  def init(opts) do
    state = %{
      # ... config ...
      permits_ref: Keyword.fetch!(opts, :permits_ref),
      shard_id: Keyword.fetch!(opts, :shard_id),
      stream_identifier: Keyword.fetch!(opts, :stream_identifier),
      processor_module: Keyword.fetch!(opts, :processor_module),
      processor_config: Keyword.get(opts, :processor_config),
      # ... internal state ...
      iterator: nil
    }

    {:ok, :getting_iterator, state}
  end

  def callback_mode, do: :state_functions

  # States
  def getting_iterator(_event_type, _event_content, state) do
    case Xinesis4.AwsClient.get_shard_iterator(state.stream_identifier, state.shard_id) do
      {:ok, iterator} ->
        Logger.debug("[#{state.shard_id}] Got iterator, moving to request permit.")
        {:next_state, :requesting_permit, %{state | iterator: iterator}}

      {:error, reason} ->
        Logger.error("[#{state.shard_id}] Failed to get iterator: #{inspect(reason)}")
        {:next_state, :getting_iterator, state, 5000}
    end
  end

  def requesting_permit(:enter, _old_state, state) do
    # Attempt to decrement the atomic counter.
    # If it was > 0, we get the new value (>= 0) and have acquired a permit.
    # If it was 0, we get -1 and failed. We must increment it back to 0.
    if :atomics.sub_get(state.permits_ref, 1, 1) >= 0 do
      # Success! We have a permit.
      Logger.debug("[#{state.shard_id}] Acquired permit via atomics.")
      {:next_state, :fetching_records, state}
    else
      # Failure, no permits available. Increment back and retry.
      :atomics.add(state.permits_ref, 1, 1)
      Logger.debug("[#{state.shard_id}] No permits available, will retry.")
      # backoff
      {:next_state, :requesting_permit, state, Enum.random(500..2000)}
    end
  end

  def requesting_permit(_event_type, _event_content, state), do: {:keep_state, state}

  def fetching_records(:enter, _old_state, state) do
    # Using Task to fetch asynchronously
    Task.async(fn -> Xinesis4.AwsClient.get_records(state.iterator) end)
    {:keep_state, state}
  end

  def fetching_records({:info, {:DOWN, _, :process, _, result}}, _from, state) do
    # IMPORTANT: Return the permit by incrementing the counter.
    :atomics.add(state.permits_ref, 1, 1)
    Logger.debug("[#{state.shard_id}] Returned permit via atomics.")
    handle_fetch_result(result, state)
  end

  def fetching_records(_event_type, _event_content, state), do: {:keep_state, state}

  # Helpers
  defp handle_fetch_result(result, state) do
    case result do
      {:ok, %{"NextShardIterator" => next_iter, "Records" => records}} ->
        if not is_nil(next_iter) do
          state.processor_module.process(state.shard_id, records, state.processor_config)
          {:next_state, :requesting_permit, %{state | iterator: next_iter}}
        else
          Logger.info("[#{state.shard_id}] Shard closed.")
          {:stop, :normal, state}
        end

      {:error, reason} ->
        Logger.error("[#{state.shard_id}] Fetch failed: #{inspect(reason)}")
        {:next_state, :requesting_permit, state, 5000}
    end
  end
end

# --- Mock AWS Client and Processor ---
defmodule Xinesis4.AwsClient do
  def list_shards(_), do: {:ok, [%{"ShardId" => "shard-001"}, %{"ShardId" => "shard-002"}]}
  def get_shard_iterator(_, shard_id), do: {:ok, "iterator-for-#{shard_id}"}

  def get_records(iterator) do
    Process.sleep(Enum.random(100..500))
    records = Enum.map(1..Enum.random(0..5), &%{"Data" => "data-#{&1}"})
    {:ok, %{"NextShardIterator" => "next-#{iterator}", "Records" => records}}
  end
end

defmodule MyProcessor4 do
  require Logger

  def process(shard_id, records, _config) do
    Logger.info("[#{shard_id}] MyProcessor4 processing #{length(records)} records.")
  end
end
