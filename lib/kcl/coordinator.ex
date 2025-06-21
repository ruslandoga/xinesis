defmodule Kcl.Coordinator do
  @moduledoc """
  A KCL-style Stream Coordinator using gen_statem.

  This coordinator is responsible for discovering shards and supervising a
  `Kcl.Processor` for each shard. It ensures that a processor is always
  running (or attempting to run) for every shard.
  """
  use GenStatem, restart: :transient

  require Logger

  # ============================================================================
  # Public API
  # ============================================================================

  def start_link(opts) do
    # The coordinator is a singleton within this node.
    GenStatem.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # ============================================================================
  # GenStatem Callbacks
  # ============================================================================

  @impl true
  def init(opts) do
    # Initialize the state from the given options.
    # This would include stream name, application name, AWS credentials, etc.
    Process.flag(:trap_exit, true)

    state = %{
      opts: opts,
      shards: %{}, # shard_id => %{pid: pid | nil, status: :running | :restarting}
      discovered_shard_ids: []
    }

    # Start by discovering the shards for the stream.
    {:ok, :discovering_shards, state, {:state_timeout, 0, :discover}}
  end

  @impl true
  def callback_mode, do: :state_functions

  # ============================================================================
  # State Implementations
  # ============================================================================

  @doc """
  Discover all shards for the configured Kinesis stream.
  """
  def discovering_shards(:state_timeout, :discover, state) do
    # 1. Call ListShards to get the full list of shards for the stream.
    #    - In a real implementation, this would use an AWS client.
    # 2. On success, transition to spawning processors.
    # 3. On failure, retry with backoff.
    discovered_shard_ids = ["shard-001", "shard-002"] # Placeholder
    Logger.info("Discovered shards: #{inspect(discovered_shard_ids)}")
    new_state = %{state | discovered_shard_ids: discovered_shard_ids}
    {:next_state, :spawning_processors, new_state, {:state_timeout, 0, :spawn}}
  end

  @doc """
  Spawns an initial processor for each discovered shard.
  """
  def spawning_processors(:state_timeout, :spawn, state) do
    # For each shard, spawn a processor.
    shards =
      Enum.into(state.discovered_shard_ids, %{}, fn shard_id ->
        {shard_id, spawn_processor(shard_id, state.opts)}
      end)

    {:next_state, :running, %{state | shards: shards}}
  end

  @doc """
  The main operational state. Monitors processors and restarts them if they exit.
  """
  def running({:info, {:restart_processor, shard_id}}, _from, state) do
    Logger.info("Attempting to restart processor for shard #{shard_id}...")
    new_shard_map = Map.put(state.shards, shard_id, spawn_processor(shard_id, state.opts))
    {:keep_state, %{state | shards: new_shard_map}}
  end

  def running(event_type, event_content, state) do
    # This state is mostly event-driven, waiting for EXIT or restart messages.
    # A periodic timer could be added here to check for shard splits/merges.
    {:keep_state, state}
  end

  # ============================================================================
  # Common Event Handlers
  # ============================================================================

  @impl true
  def handle_event({:call, from}, event, state_name, state) do
    # Handle any synchronous calls if needed.
    GenStatem.reply(from, :ok)
    {:keep_state, state}
  end

  @impl true
  def handle_event(:info, {:EXIT, pid, reason}, state_name, state) do
    # A linked process (a shard processor) has exited.
    case find_shard_by_pid(pid, state.shards) do
      {:ok, shard_id} ->
        Logger.warn(
          "Processor for shard #{shard_id} exited with reason: #{inspect(reason)}. Restarting after delay."
        )
        # Mark the processor as dead and schedule a restart.
        new_shards = Map.put(state.shards, shard_id, %{pid: nil, status: :restarting})
        # Restart after a delay to avoid busy-looping if it can't get the lease.
        Process.send_after(self(), {:restart_processor, shard_id}, 15_000)
        {:keep_state, %{state | shards: new_shards}}

      :not_found ->
        # Not a process we manage, ignore.
        {:keep_state, state}
    end
  end

  @impl true
  def handle_event(:info, msg, state_name, state) do
    # Handle other unexpected messages.
    Logger.warn("Coordinator in state #{state_name} received unexpected message: #{inspect(msg)}")
    {:keep_state, state}
  end

  @impl true
  def terminate(reason, state_name, state) do
    # Cleanup logic that runs when the gen_statem is about to terminate.
    # This should try to gracefully shut down all child processors.
    :ok
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp spawn_processor(shard_id, opts) do
    processor_opts = Keyword.put(opts, :shard_id, shard_id)

    case Kcl.Processor.start_link(processor_opts) do
      {:ok, pid} ->
        Logger.info("Started processor for shard #{shard_id} (pid: #{inspect(pid)})")
        %{pid: pid, status: :running}

      {:error, reason} ->
        Logger.error("Failed to start processor for shard #{shard_id}: #{inspect(reason)}")
        %{pid: nil, status: :failed}
    end
  end

  defp find_shard_by_pid(pid, shards) do
    Enum.find_value(shards, :not_found, fn {shard_id, %{pid: p}} ->
      if p == pid, do: {:ok, shard_id}
    end)
  end
end
