defmodule Xinesis6 do
  @moduledoc """
  Xinesis6: A Kinesis consumer using GenStage for demand-driven back-pressure.

  This implementation models the Kinesis stream as a `GenStage` producer.
  Downstream consumers subscribe and explicitly ask for records (`demand`).
  The producer maintains a buffer and only fetches more records from Kinesis
  when the demand from consumers exceeds the buffer.
  """

  defmodule Producer do
    @moduledoc "A GenStage producer for a single Kinesis shard."
    use GenStage

    require Logger

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts, name: opts[:name])
    end

    @impl true
    def init(opts) do
      state = %{
        # Config
        stream_identifier: Keyword.fetch!(opts, :stream_identifier),
        shard_id: Keyword.fetch!(opts, :shard_id),
        # Internal State
        iterator: nil,
        buffer: [],
        demand: 0
      }

      # Start by getting an iterator, then wait for demand.
      {:producer, state, {:continue, :get_iterator}}
    end

    @impl true
    def handle_continue(:get_iterator, state) do
      case Xinesis6.AwsClient.get_shard_iterator(state.stream_identifier, state.shard_id) do
        {:ok, iterator} ->
          Logger.debug("[#{state.shard_id}] Got iterator.")
          {:noreply, [], %{state | iterator: iterator}}

        {:error, reason} ->
          Logger.error(
            "[#{state.shard_id}] Failed to get iterator: #{inspect(reason)}. Retrying..."
          )

          Process.send_after(self(), {:continue, :get_iterator}, 5000)
          {:noreply, [], state}
      end
    end

    @impl true
    def handle_demand(demand, state) do
      Logger.debug("[#{state.shard_id}] Received demand for #{demand} events.")
      new_demand = state.demand + demand
      maybe_fetch_and_dispatch(state, new_demand)
    end

    # Internal Helpers

    defp maybe_fetch_and_dispatch(state, demand) do
      if demand > 0 and demand > length(state.buffer) do
        # Not enough in buffer to satisfy demand, fetch more.
        case Xinesis6.AwsClient.get_records(state.iterator) do
          {:ok, %{"NextShardIterator" => next_iter, "Records" => records}}
          when not is_nil(next_iter) ->
            new_buffer = state.buffer ++ records
            dispatch_events(new_buffer, demand, %{state | iterator: next_iter})

          {:ok, %{"NextShardIterator" => nil}} ->
            Logger.info("[#{state.shard_id}] Shard closed. Draining buffer.")
            dispatch_events(state.buffer, demand, %{state | iterator: nil})

          {:error, reason} ->
            Logger.error("[#{state.shard_id}] Fetch error: #{inspect(reason)}")
            # Don't fetch, just dispatch what we have.
            dispatch_events(state.buffer, demand, state)
        end
      else
        # Enough in buffer to satisfy demand.
        dispatch_events(state.buffer, demand, state)
      end
    end

    defp dispatch_events(buffer, demand, state) do
      {records_to_send, new_buffer} = Enum.split(buffer, demand)
      new_demand = demand - length(records_to_send)

      Logger.debug(
        "[#{state.shard_id}] Dispatched #{length(records_to_send)} events. Remaining demand: #{new_demand}"
      )

      new_state = %{state | buffer: new_buffer, demand: new_demand}

      # If the shard is closed and we have dispatched all buffered events, we are done.
      if is_nil(state.iterator) and Enum.empty?(new_buffer) do
        {:stop, :normal, records_to_send, new_state}
      else
        {:noreply, records_to_send, new_state}
      end
    end
  end

  defmodule Consumer do
    use GenStage
    require Logger

    def start_link(_) do
      GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    @impl true
    def init(:ok) do
      # Subscribe to a producer. In a real app, you'd have a supervisor
      # starting and subscribing to all shard producers.
      {:consumer, :ok, subscribe_to: [name: "shard-001-producer"]}
    end

    @impl true
    def handle_events(events, _from, state) do
      # This is where you process the records.
      # The :min_demand in the consumer configuration controls back-pressure.
      Logger.info("Consumer received #{length(events)} events.")
      # Simulate work
      Process.sleep(1000)
      {:noreply, [], state}
    end
  end
end

# --- Mock AWS Client ---
defmodule Xinesis6.AwsClient do
  def list_shards(_), do: {:ok, [%{"ShardId" => "shard-001"}]}
  def get_shard_iterator(_, shard_id), do: {:ok, "iterator-for-#{shard_id}"}

  def get_records(iterator) do
    Process.sleep(250)
    records = Enum.map(1..Enum.random(5..10), &%{"Data" => "data-#{&1}"})
    {:ok, %{"NextShardIterator" => "next-#{iterator}", "Records" => records}}
  end
end
