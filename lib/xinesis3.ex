defmodule Xinesis3 do
  @moduledoc """
  Xinesis3: A Kinesis consumer with a centralized permit system for fairness.

  This implementation uses a `StreamCoordinator` GenServer to distribute "fetch permits"
  to a pool of `ShardProcessor` gen_statems. This ensures that only a configured
  number of shards can fetch records concurrently, providing fairness and
  preventing "hot shards" from starving others.
  """

  alias Xinesis3.StreamCoordinator

  def start_link(opts) do
    StreamCoordinator.start_link(opts)
  end
end

defmodule Xinesis3.StreamCoordinator do
  @moduledoc """
  Manages shards and distributes fetch permits to ensure fairness.
  """
  @behaviour :gen_statem

  require Logger

  # Public API
  def start_link(opts) do
    :gen_statem.start_link(__MODULE__, opts, name: opts[:name] || __MODULE__)
  end

  def request_permit(pid, shard_id) do
    # This call will block until a permit is granted by the coordinator.
    :gen_statem.call(pid, {:request_permit, shard_id}, :infinity)
  end

  def return_permit(pid, shard_id) do
    :gen_statem.cast(pid, {:return_permit, shard_id})
  end

  # Callbacks
  @impl true
  def callback_mode, do: :handle_event_function

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %{
      # Config
      stream_identifier: Keyword.fetch!(opts, :stream_identifier), # e.g., {:name, "my-stream"}
      processor_module: Keyword.fetch!(opts, :processor_module),
      processor_config: Keyword.get(opts, :processor_config),
      max_concurrent_fetches: Keyword.get(opts, :max_concurrent_fetches, 10),

      # Internal state
      shards: %{}, # %{shard_id => pid}
      permit_queue: :queue.new(), # queue of {shard_id, from_ref} waiting for a permit
      permits_in_flight: MapSet.new(), # Set of shard_ids that have a permit
      shard_info: %{} # %{shard_id => %{parent: "...", status: :...}}
    }

    # Start by discovering shards
    {:ok, :discovering_shards, state, {:next_event, :internal, :discover_shards}}
  end

  @impl true
  def handle_event(:internal, :discover_shards, :discovering_shards, state) do
    Logger.info("Discovering shards for #{inspect(state.stream_identifier)}...")

    case Xinesis3.AwsClient.list_shards(state.stream_identifier) do
      {:ok, shards_data} ->
        Logger.info("Discovered #{length(shards_data)} shards.")
        shard_info =
          Map.new(shards_data, fn %{"ShardId" => sid} = shard ->
            {sid, %{parent: Map.get(shard, "ParentShardId"), status: :pending}}
          end)

        new_state = spawn_initial_processors(%{state | shard_info: shard_info})
        {:next_state, :running, new_state}

      {:error, reason} ->
        Logger.error("Failed to list shards: #{inspect(reason)}. Retrying...")
        {:keep_state, state, {{:timeout, :discover_timeout}, 5000, :discover_shards}}
    end
  end

  @impl true
  def handle_event({:timeout, :discover_timeout}, :discover_shards, :discovering_shards, state) do
     {:keep_state, state, {:next_event, :internal, :discover_shards}}
  end

  @impl true
  def handle_event({:call, from}, {:request_permit, shard_id}, :running, state) do
    if MapSet.size(state.permits_in_flight) < state.max_concurrent_fetches do
      # Grant permit immediately
      new_permits = MapSet.put(state.permits_in_flight, shard_id)
      {:next_state, :running, %{state | permits_in_flight: new_permits}, {:reply, from, :ok}}
    else
      # Enqueue and reply later
      Logger.debug("[#{shard_id}] waiting for permit.")
      new_queue = :queue.in({shard_id, from}, state.permit_queue)
      {:keep_state, %{state | permit_queue: new_queue}}
    end
  end

  @impl true
  def handle_event(:cast, {:return_permit, shard_id}, :running, state) do
    new_permits = MapSet.delete(state.permits_in_flight, shard_id)

    case :queue.out(state.permit_queue) do
      {{:value, {waiting_shard_id, from}}, new_queue} ->
        Logger.debug("Granting deferred permit to #{waiting_shard_id}")
        :gen_statem.reply(from, :ok)
        # The waiting shard now has a permit
        new_permits = MapSet.put(new_permits, waiting_shard_id)
        {:keep_state, %{state | permits_in_flight: new_permits, permit_queue: new_queue}}
      {:empty, new_queue} ->
        # No one is waiting
        {:keep_state, %{state | permits_in_flight: new_permits, permit_queue: new_queue}}
    end
  end

  @impl true
  def handle_event(:info, {:EXIT, pid, reason}, _state_name, state) do
    case Enum.find(state.shards, fn {_, p} -> p == pid end) do
      {shard_id, _} ->
        Logger.warn("Shard processor for #{shard_id} exited with reason: #{inspect(reason)}")
        # A real implementation would handle resharding and processor restarts.
        new_shards = Map.delete(state.shards, shard_id)
        {:keep_state, %{state | shards: new_shards}}
      nil ->
        # This could be a linked process that we don't manage, so we stop.
        {:stop, {:unhandled_exit, reason}, state}
    end
  end

  # Catch-all
  @impl true
  def handle_event(event_type, event_content, state_name, data) do
    Logger.warn("Unhandled event: #{inspect {event_type, event_content}} in state #{state_name}")
    {:keep_state, data}
  end

  # Internal helpers
  defp spawn_initial_processors(state) do
    # Simplified: spawn for all shards without a parent.
    state.shard_info
    |> Enum.filter(fn {_, %{parent: nil}} -> true end)
    |> Enum.reduce(state, fn {shard_id, _}, acc_state ->
      spawn_processor(shard_id, acc_state)
    end)
  end

  defp spawn_processor(shard_id, state) do
    opts = [
      coordinator: self(),
      shard_id: shard_id,
      stream_identifier: state.stream_identifier,
      processor_module: state.processor_module,
      processor_config: state.processor_config
    ]

    case Xinesis3.ShardProcessor.start_link(opts) do
      {:ok, pid} ->
        Logger.info("Started processor for shard #{shard_id} (pid: #{inspect(pid)})")
        %{state | shards: Map.put(state.shards, shard_id, pid)}
      {:error, reason} ->
        Logger.error("Failed to start processor for shard #{shard_id}: #{inspect(reason)}")
        state
    end
  end
end


defmodule Xinesis3.ShardProcessor do
  @moduledoc "Processes a single shard, respecting the coordinator's permit system."
  @behaviour :gen_statem

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
      coordinator: Keyword.fetch!(opts, :coordinator),
      shard_id: Keyword.fetch!(opts, :shard_id),
      stream_identifier: Keyword.fetch!(opts, :stream_identifier),
      processor_module: Keyword.fetch!(opts, :processor_module),
      processor_config: Keyword.get(opts, :processor_config),
      # Internal state
      iterator: nil,
      task_ref: nil
    }
    # Start by getting an iterator
    {:ok, :getting_iterator, state}
  end

  @impl true
  def callback_mode, do: :state_functions

  # States
  def getting_iterator(event_type, event_content, state) do
    handle_common(event_type, event_content, state, fn state ->
      case Xinesis3.AwsClient.get_shard_iterator(state.stream_identifier, state.shard_id) do
        {:ok, iterator} ->
          Logger.debug("[#{state.shard_id}] Got initial iterator.")
          next_state(:requesting_permit, %{state | iterator: iterator})
        {:error, reason} ->
          Logger.error("[#{state.shard_id}] Failed to get iterator: #{inspect(reason)}. Retrying...")
          next_state(:getting_iterator, state, 5000)
      end
    end)
  end

  def requesting_permit(:enter, _old_state, state) do
    Logger.debug("[#{state.shard_id}] Requesting fetch permit.")
    # This is a blocking call. The coordinator will reply when a permit is available.
    case Xinesis3.StreamCoordinator.request_permit(state.coordinator, state.shard_id) do
      :ok ->
        # Permit granted.
        next_state(:fetching_records, state)
      {:error, reason} ->
        # Something went wrong with the request itself
        Logger.error("[#{state.shard_id}] Failed to request permit: #{inspect(reason)}")
        next_state(:requesting_permit, state, 5000)
    end
  end

  def requesting_permit(event_type, event_content, state) do
    handle_common(event_type, event_content, state, fn state -> {:keep_state, state} end)
  end

  def fetching_records(:enter, _old_state, state) do
    Logger.debug("[#{state.shard_id}] Permit granted, fetching records.")
    task = Task.async(fn -> Xinesis3.AwsClient.get_records(state.iterator) end)
    {:keep_state, %{state | task_ref: task.ref}}
  end

  def fetching_records({:info, {:DOWN, ref, :process, _pid, result}}, _from, state) do
    if ref != state.task_ref, do: {:keep_state, state}
    else
      # IMPORTANT: Return the permit so another shard can work.
      Xinesis3.StreamCoordinator.return_permit(state.coordinator, state.shard_id)
      handle_fetch_result(result, state)
    end
  end

  def fetching_records(event_type, event_content, state) do
    handle_common(event_type, event_content, state, fn state -> {:keep_state, state} end)
  end

  # Helpers
  defp handle_fetch_result(result, state) do
    case result do
      {:ok, %{"NextShardIterator" => next_iter, "Records" => records}} when not is_nil(next_iter) ->
        Logger.debug("[#{state.shard_id}] Received #{length(records)} records.")
        try do
          state.processor_module.process(state.shard_id, records, state.processor_config)
        catch
          kind, reason -> Logger.error("[#{state.shard_id}] Processor crashed: #{kind} #{inspect(reason)}")
        end
        # Loop back to request another permit for the next fetch.
        next_state(:requesting_permit, %{state | iterator: next_iter})

      {:ok, %{"NextShardIterator" => nil}} ->
        Logger.info("[#{state.shard_id}] Shard has been closed.")
        {:stop, :normal, state}

      {:error, reason} ->
        Logger.error("[#{state.shard_id}] Error fetching records: #{inspect(reason)}. Retrying...")
        # Loop back to try again after a delay.
        next_state(:requesting_permit, state, 5000)
    end
  end

  defp handle_common(:state_timeout, _event_content, state, func), do: func.(state)
  defp handle_common(:internal, _event_content, state, func), do: func.(state)
  defp handle_common(_, _, state, _), do: {:keep_state, state}

  defp next_state(name, data, timeout \\ 0), do: {:next_state, name, data, {:state_timeout, timeout}}
end


defmodule Xinesis3.AwsClient do
  @moduledoc "A mock AWS client. The user will fill this in."

  def list_shards(_stream_identifier) do
    # Invented response. A real implementation would handle pagination.
    shards = [
      %{"ShardId" => "shard-001", "ParentShardId" => nil},
      %{"ShardId" => "shard-002", "ParentShardId" => nil},
      %{"ShardId" => "shard-003", "ParentShardId" => "shard-001"},
    ]
    {:ok, shards}
  end

  def get_shard_iterator(_stream_identifier, shard_id, _type \ "TRIM_HORIZON") do
    # Invented response
    {:ok, "iterator-for-#{shard_id}"}
  end

  def get_records(iterator) do
    # Invented response
    Process.sleep(Enum.random(50..500)) # Simulate network latency
    if is_nil(iterator) or "closed" in iterator do
      {:ok, %{"NextShardIterator" => nil, "Records" => []}}
    else
      # Simulate a shard closing occasionally
      next_iterator = if :rand.uniform(100) > 95, do: nil, else: "next-#{iterator}"

      {:ok, %{
        "NextShardIterator" => next_iterator,
        "Records" => Enum.map(1..Enum.random(0..10), &(%{"Data" => "record-#{&1}"}))
      }}
    end
  end
end

defmodule MyProcessor do
  @moduledoc "Example processor module."
  require Logger

  def process(shard_id, records, _config) do
    Logger.info("[#{shard_id}] MyProcessor processing #{length(records)} records.")
    # ... actual processing logic ...
  end
end
