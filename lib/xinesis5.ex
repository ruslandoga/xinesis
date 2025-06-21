defmodule Xinesis5 do
  @moduledoc """
  Xinesis5: A Kinesis consumer with an adaptive, TCP-inspired (AIMD) back-pressure system.

  The StreamCoordinator dynamically adjusts the total number of concurrent fetches
  allowed based on observed performance (e.g., record fetch times). It increases the
  concurrency window additively when things are good and decreases it multiplicatively
  when it detects problems (like high latency), similar to TCP's congestion control.
  """

  alias Xinesis5.StreamCoordinator

  def start_link(opts) do
    StreamCoordinator.start_link(opts)
  end
end

defmodule Xinesis5.StreamCoordinator do
  @moduledoc "Manages shards and dynamically adjusts the concurrency window."
  use GenServer
  require Logger

  # AIMD Configuration
  @increase_step 1 # Additive increase value
  @decrease_factor 0.5 # Multiplicative decrease factor
  @latency_threshold 1000 # ms - if avg latency is above this, decrease window
  @adjustment_interval 10_000 # ms - how often to adjust the window

  # Public API
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name] || __MODULE__)
  end

  def request_permit(pid, shard_id), do: GenServer.call(pid, {:request_permit, shard_id}, :infinity)
  def return_permit(pid, shard_id, metrics), do: GenServer.cast(pid, {:return_permit, shard_id, metrics})

  # Callbacks
  @impl true
  def init(opts) do
    state = %{
      # Config
      stream_identifier: Keyword.fetch!(opts, :stream_identifier),
      processor_module: Keyword.fetch!(opts, :processor_module),
      min_concurrency: Keyword.get(opts, :min_concurrency, 1),
      initial_concurrency: Keyword.get(opts, :initial_concurrency, 5),

      # Dynamic State
      max_concurrent_fetches: Keyword.get(opts, :initial_concurrency, 5),
      recent_fetch_times: [], # List of last N fetch times in ms

      # Internal state (same as Xinesis3)
      shards: %{},
      permit_queue: :queue.new(),
      permits_in_flight: MapSet.new()
    }

    Process.send_after(self(), :adjust_window, @adjustment_interval)
    {:ok, state, {:continue, :discover_shards}}
  end

  @impl true
  def handle_continue(:discover_shards, state) do
    # Shard discovery logic is identical to Xinesis3
    # ... spawn processors ...
    {:noreply, state}
  end

  @impl true
  def handle_call({:request_permit, shard_id}, from, state) do
    # Permit granting logic is identical to Xinesis3
    if MapSet.size(state.permits_in_flight) < state.max_concurrent_fetches do
      {:reply, :ok, %{state | permits_in_flight: MapSet.put(state.permits_in_flight, shard_id)}}
    else
      {:noreply, %{state | permit_queue: :queue.in({shard_id, from}, state.permit_queue)}}
    end
  end

  @impl true
  def handle_cast({:return_permit, shard_id, %{fetch_duration: duration_ms}}, state) do
    # A shard returns its permit AND performance metrics
    new_permits = MapSet.delete(state.permits_in_flight, shard_id)
    new_fetch_times = [duration_ms | state.recent_fetch_times] |> Enum.take(100)

    # Grant next permit if anyone is waiting (identical to Xinesis3)
    case :queue.out(state.permit_queue) do
      {{:value, {waiting_shard_id, from}}, new_queue} ->
        GenServer.reply(from, :ok)
        new_permits = MapSet.put(new_permits, waiting_shard_id)
        {:noreply, %{state | permits_in_flight: new_permits, permit_queue: new_queue, recent_fetch_times: new_fetch_times}}
      {:empty, new_queue} ->
        {:noreply, %{state | permits_in_flight: new_permits, permit_queue: new_queue, recent_fetch_times: new_fetch_times}}
    end
  end

  @impl true
  def handle_info(:adjust_window, state) do
    # Periodically adjust the concurrency window
    avg_latency = if state.recent_fetch_times == [], do: 0, else: Enum.sum(state.recent_fetch_times) / length(state.recent_fetch_times)

    new_max_fetches =
      if avg_latency > @latency_threshold and length(state.recent_fetch_times) > 0 do
        # Multiplicative Decrease
        max(state.min_concurrency, floor(state.max_concurrent_fetches * @decrease_factor))
      else
        # Additive Increase
        state.max_concurrent_fetches + @increase_step
      end

    Logger.info("Adjusting window. Avg Latency: #{avg_latency}ms. New max fetches: #{new_max_fetches}")

    # Schedule next adjustment
    Process.send_after(self(), :adjust_window, @adjustment_interval)
    {:noreply, %{state | max_concurrent_fetches: new_max_fetches, recent_fetch_times: []} # Reset stats
    }
  end
end

defmodule Xinesis5.ShardProcessor do
  @moduledoc "Reports fetch duration back to the coordinator."
  @behaviour :gen_statem
  require Logger

  def start_link(opts), do: :gen_statem.start_link(__MODULE__, opts, [])
  def init(opts) do
    # ... init state as before ...
    state = %{
      coordinator: Keyword.fetch!(opts, :coordinator),
      shard_id: Keyword.fetch!(opts, :shard_id),
      stream_identifier: Keyword.fetch!(opts, :stream_identifier),
      processor_module: Keyword.fetch!(opts, :processor_module),
      iterator: nil
    }
    {:ok, :getting_iterator, state}
  end

  def callback_mode, do: :state_functions

  def getting_iterator(_event_type, _event_content, state) do
    # ... same as before ...
    {:next_state, :requesting_permit, state}
  end

  def requesting_permit(:enter, _old_state, state) do
    case Xinesis5.StreamCoordinator.request_permit(state.coordinator, state.shard_id) do
      :ok -> {:next_state, :fetching_records, state}
      _ -> {:next_state, :requesting_permit, state, 5000}
    end
  end
  def requesting_permit(_, _, state), do: {:keep_state, state}

  def fetching_records(:enter, _old_state, state) do
    # Time the fetch operation
    task = Task.async(fn ->
      start_time = System.monotonic_time(:millisecond)
      result = Xinesis5.AwsClient.get_records(state.iterator)
      end_time = System.monotonic_time(:millisecond)
      {result, end_time - start_time}
    end)
    {:keep_state, %{state | task_ref: task.ref}}
  end

  def fetching_records({:info, {:DOWN, ref, :process, _, {result, duration_ms}}}, _from, state) do
    if ref != state.task_ref, do: {:keep_state, state}
    else
      # Return permit with metrics
      Xinesis5.StreamCoordinator.return_permit(state.coordinator, state.shard_id, %{fetch_duration: duration_ms})

      # ... handle result as before ...
      {:next_state, :requesting_permit, state}
    end
  end
  def fetching_records(_, _, state), do: {:keep_state, state}
end

# --- Mock AWS Client and Processor ---
defmodule Xinesis5.AwsClient do
  def get_records(_iterator) do
    Process.sleep(Enum.random(50..1500)) # Simulate variable latency
    # ...
  end
end
