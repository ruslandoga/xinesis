defmodule Kcl.Processor do
  @moduledoc """
  A KCL-style Shard Processor using gen_statem.

  This processor is responsible for a single shard. It is created by the
  `Kcl.Coordinator` and immediately tries to acquire a lease for its shard.

  It follows this lifecycle:
  1. `acquiring_lease`: Tries to claim the shard. Exits if it can't.
  2. `initializing`: Calls the user's `initialize/1` callback.
  3. `processing`: In a loop, it:
     a. Starts an async Task to fetch and process records.
     b. Periodically renews its lease.
     c. Exits if lease is lost.
  4. `shutting_down`: Calls the user's `shutdown/1` callback and releases the lease.
  """
  use GenStatem, restart: :transient

  require Logger

  # ============================================================================
  # Public API
  # ============================================================================

  def start_link(opts) do
    # Started by the Coordinator for a specific shard.
    GenStatem.start_link(__MODULE__, opts, [])
  end

  # ============================================================================
  # GenStatem Callbacks
  # ============================================================================

  @impl true
  def init(opts) do
    # The processor is initialized with everything it needs to work on the shard.
    state = %{
      opts: opts,
      shard_id: Keyword.fetch!(opts, :shard_id),
      user_processor: Keyword.fetch!(opts, :user_processor),
      user_state: nil,
      iterator: nil,
      lease: nil, # Will hold lease information
      task_ref: nil # To monitor the record processing task
    }

    # Immediately try to acquire the lease for our shard.
    {:ok, :acquiring_lease, state, {:state_timeout, 0, :acquire}}
  end

  @impl true
  def callback_mode, do: :state_functions

  # ============================================================================
  # State Implementations
  # ============================================================================

  @doc """
  Attempts to acquire the lease for the shard.
  """
  def acquiring_lease(:state_timeout, :acquire, state) do
    # 1. Attempt to acquire the lease from the lease manager (e.g., DynamoDB).
    #    - This is a placeholder for the actual lease logic.
    case {:ok, "lease-token-for-#{state.shard_id}"} do
      {:ok, lease} ->
        Logger.info("[#{state.shard_id}] Lease acquired.")
        # If successful, move to the initializing state.
        {:next_state, :initializing, %{state | lease: lease}}

      {:error, :lease_unavailable} ->
        Logger.info("[#{state.shard_id}] Lease is held by another worker. Backing off.")
        # If the lease is held by another worker, exit gracefully.
        # The coordinator will restart this process after a delay to try again.
        {:stop, :normal, state}
    end
  end

  @doc """
  Calls the user-provided `initialize/1` callback after a lease is secured.
  """
  def initializing(event_type, event_content, state) do
    # 1. Call the user's `initialize` callback.
    # 2. Get the initial shard iterator from Kinesis.
    # 3. Transition to :processing, which starts the main loop.
    Logger.info("[#{state.shard_id}] Initializing user processor.")
    # Placeholder for getting the real iterator
    iterator = "initial-iterator"
    {:next_state, :processing, %{state | iterator: iterator}}
  end

  @doc """
  The main operational state. Manages lease renewal and processing tasks.
  """
  def processing(:enter, _old_state, state) do
    # When we enter this state, we start two independent, repeating timers:
    # 1. A timer to renew our lease periodically.
    # 2. A timer to trigger fetching records.
    {:keep_state, state,
     [
       {:state_timeout, 5000, :renew_lease}, # Renew lease every 5s
       {:state_timeout, 1000, :fetch_records} # Attempt to fetch records every 1s
     ]}
  end

  def processing(:state_timeout, :renew_lease, state) do
    # 1. Attempt to renew the lease.
    # 2. If it fails, the lease was likely stolen or expired, so shut down.
    case :ok do # Placeholder for actual lease renewal logic
      :ok ->
        # Lease renewed, schedule the next renewal.
        {:keep_state, state, {:state_timeout, 5000, :renew_lease}}

      :error ->
        Logger.warn("[#{state.shard_id}] Failed to renew lease. Shutting down.")
        {:next_state, :shutting_down, state}
    end
  end

  def processing(:state_timeout, :fetch_records, state) do
    # This event triggers a new processing task, but only if one isn't already running.
    if state.task_ref do
      # Task is already running, do nothing. The next fetch will be triggered
      # when the current task finishes.
      {:keep_state, state}
    else
      # No task is running, so start one.
      task = create_processing_task(state)
      new_state = %{state | task_ref: task.ref}
      {:keep_state, new_state}
    end
  end

  def processing({:info, {:DOWN, ref, :process, _pid, reason}}, _from, state) do
    # This is the result of our processing task.
    if ref != state.task_ref do
      # Not our task, ignore it.
      {:keep_state, state}
    else
      # Process the result and schedule the next fetch.
      handle_task_result(reason, state)
    end
  end

  def processing({:cast, {:checkpoint, data}}, _from, state) do
    # This allows the user's code (in the task) to send a message back to
    # the processor to perform a checkpoint.
    Logger.info("[#{state.shard_id}] Checkpointing with data: #{inspect(data)}")
    # Add actual checkpointing logic here.
    {:keep_state, state}
  end

  @doc """
  Performs graceful shutdown. Calls the user's `shutdown/1` callback.
  """
  def shutting_down(event_type, event_content, state) do
    # 1. Call the user's `shutdown` callback for final processing/checkpointing.
    # 2. Release the lease.
    Logger.info("[#{state.shard_id}] Shutting down user processor and releasing lease.")
    {:stop, :normal, state}
  end

  # ============================================================================
  # Common Event Handlers
  # ============================================================================

  @impl true
  def handle_event({:call, from}, :shutdown, state_name, state) do
    # The coordinator can request a graceful shutdown.
    GenStatem.reply(from, :ok)
    {:next_state, :shutting_down, state}
  end

  @impl true
  def handle_event(event_type, event_content, state_name, state) do
    # Catch-all for unhandled events in any state.
    Logger.warn(
      "Processor in state #{state_name} received unhandled event: #{inspect(event_content)}"
    )

    {:keep_state, state}
  end

  @impl true
  def terminate(reason, state_name, state) do
    # Final cleanup, e.g., ensuring the lease is released.
    Logger.info("[#{state.shard_id}] Terminating with reason: #{inspect(reason)}")
    # Add final lease release logic here if needed.
    :ok
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp create_processing_task(state) do
    # This task runs the potentially long-running I/O and user code.
    Task.async(fn ->
      # 1. Fetch records from Kinesis.
      # records = AwsClient.get_records(state.iterator)
      records = [%{data: "record1"}, %{data: "record2"}] # Placeholder
      next_iterator = "next-iterator" # Placeholder

      # 2. Call the user's processor function.
      #    The user processor can send a `{:checkpoint, data}` message to us.
      # state.user_processor.(records, %{checkpointer: self()})

      # 3. Return the result.
      {:ok, next_iterator}
    end)
  end

  defp handle_task_result(result, state) do
    new_state =
      case result do
        {:ok, next_iterator} ->
          # Task succeeded, update our iterator.
          %{state | iterator: next_iterator, task_ref: nil}

        {:error, reason} ->
          # Task failed, log the error.
          Logger.error("[#{state.shard_id}] Processing task failed: #{inspect(reason)}")
          %{state | task_ref: nil}
      end

    # Schedule the next fetch immediately.
    {:keep_state, new_state, {:state_timeout, 1000, :fetch_records}}
  end
end
