defmodule Xinesis.Processor do
  @moduledoc """
  Responsible for processing records for a single shard.
  """

  @behaviour :gen_statem
  alias Xinesis.AWS
  require Logger

  def start_link(opts) do
    {gen_opts, opts} = Keyword.split(opts, [:spawn_opt, :debug])
    :gen_statem.start_link(__MODULE__, opts, gen_opts)
  end

  @impl true
  def callback_mode do
    :handle_event_function
  end

  @impl true
  def init(opts) do
    # TODO maybe don't need this?
    Process.flag(:trap_exit, true)

    client = Keyword.fetch!(opts, :client)
    processor = Keyword.fetch!(opts, :processor)
    stream_arn = Keyword.fetch!(opts, :stream_arn)
    shard_id = Keyword.fetch!(opts, :shard_id)
    backoff_base = Keyword.fetch!(opts, :backoff_base)
    backoff_max = Keyword.fetch!(opts, :backoff_max)

    data = %{
      client: client,
      stream_arn: stream_arn,
      shard_id: shard_id,
      processor: processor,
      backoff_base: backoff_base,
      backoff_max: backoff_max
    }

    {:ok, :disconnected, data, {:next_event, :internal, {:connect, 0}}}
  end

  @impl true
  def handle_event(:internal, {:connect, failure_count}, :disconnected, data) do
  end
end

# defp spawn_processors(state) do
#     shards =
#       Map.new(state.shards, fn {shard_id, status} ->
#         case status do
#           _no_parents_no_pid = [] -> {shard_id, spawn_processor(shard_id, state)}
#           _ -> {shard_id, status}
#         end
#       end)

#     %{state | shards: shards}
#   end

#   defp spawn_processor(shard_id, parent_state) do
#     %{
#       scheme: scheme,
#       host: host,
#       port: port,
#       access_key_id: access_key_id,
#       secret_access_key: secret_access_key,
#       region: region,
#       stream: stream,
#       processor: processor,
#       processor_config: processor_config
#     } = parent_state

#     child_state =
#       %{
#         shard_id: shard_id,
#         scheme: scheme,
#         host: host,
#         port: port,
#         access_key_id: access_key_id,
#         secret_access_key: secret_access_key,
#         region: region,
#         stream: stream,
#         processor: processor,
#         processor_config: processor_config,
#         conn: nil,
#         backoff: 0,
#         iterator: nil
#       }

#     # TODO actually need to link
#     :proc_lib.spawn_opt(__MODULE__, :processor, [child_state], [{:monitor, [{:tag, shard_id}]}])
#   end

#   @impl true
#   def handle_info({{:DOWN, shard_id}, _ref, :process, _pid, reason}, state) do
#     state =
#       case reason do
#         {:completed, child_shards} ->
#           Logger.info(
#             "Processor for shard #{shard_id} completed with child shards: #{inspect(child_shards)}"
#           )

#           # TODO update state.shards
#           shards =
#             Map.new(state.shards, fn {id, status} ->
#               if is_list(status) do
#                 {id, List.delete(status, shard_id)}
#               else
#                 {id, status}
#               end
#             end)

#           spawn_processors(%{state | shards: shards})

#           # TODO other cases like lost lease, etc.
#       end

#     {:noreply, state}
#   end

#   @doc false
#   def processor(state) do
#     processor_connect(state)
#   end

#   defp processor_connect(state) do
#     sleep_backoff(state)

#     case connect(state) do
#       {:ok, conn} ->
#         state = %{state | conn: conn}
#         processor_get_shard_iterator(reset_backoff(state))

#       {:error, reason} ->
#         Logger.error(
#           "Failed to connect processor for shard #{state.shard_id}: #{inspect(reason)}"
#         )

#         processor_connect(inc_backoff(state))
#     end
#   end

#   defp processor_get_shard_iterator(state) do
#     sleep_backoff(state)

#     %{conn: conn, shard_id: shard_id, stream: stream} = state

#     payload = %{
#       "ShardId" => shard_id,
#       # TODO AFTER_SEQUENCE_NUMBER if checkpoint is provided
#       "ShardIteratorType" => "TRIM_HORIZON",
#       "StreamARN" =>
#         case stream do
#           {:arn, arn} -> arn
#           {:name, _name} -> nil
#         end,
#       "StreamName" =>
#         case stream do
#           {:arn, _arn} -> nil
#           {:name, name} -> name
#         end
#     }

#     case Xinesis.api_get_shard_iterator(conn, payload) do
#       {:ok, conn, %{"ShardIterator" => iterator}} ->
#         processor_loop_get_records(reset_backoff(%{state | conn: conn, iterator: iterator}))

#       {:error, conn, reason} ->
#         Logger.error("Failed to get shard iterator for #{shard_id}: #{inspect(reason)}")
#         processor_get_shard_iterator(inc_backoff(%{state | conn: conn}))

#       {:disconnect, conn, reason} ->
#         Logger.error("Connection lost while getting shard iterator: #{inspect(reason)}")
#         {:ok, conn} = HTTP.close(conn)
#         state = %{state | conn: conn}
#         processor_connect(reset_backoff(state))
#     end
#   end

#   defp processor_loop_get_records(state) do
#     sleep_backoff(state)

#     %{
#       conn: conn,
#       stream: stream,
#       shard_id: shard_id,
#       iterator: iterator,
#       processor: processor,
#       processor_config: processor_config
#     } = state

#     processor_state = %{
#       conn: conn,
#       stream: stream,
#       shard_id: shard_id,
#       iterator: iterator,
#       processor: processor,
#       processor_config: processor_config
#     }

#     # TODO link as well
#     {pid, ref} =
#       :proc_lib.spawn_opt(__MODULE__, :processor_get_records, [processor_state], [:monitor])

#     receive do
#       {:DOWN, ^ref, :process, ^pid, reason} ->
#         case reason do
#           {:continue, conn, _result, next_iterator} ->
#             processor_loop_get_records(
#               reset_backoff(%{state | conn: conn, iterator: next_iterator})
#             )

#           {:empty, conn, next_iterator} ->
#             processor_loop_get_records(
#               inc_backoff(%{state | conn: conn, iterator: next_iterator})
#             )

#           # TODO
#           {:error, conn, reason, next_iterator} ->
#             Logger.error("Unexpected error in processor loop: #{inspect(reason)}")

#             processor_loop_get_records(
#               reset_backoff(%{state | conn: conn, iterator: next_iterator})
#             )

#           {:completed, _child_shards} = completed ->
#             exit(completed)

#           {:error, conn, reason} ->
#             Logger.error("Processor for shard #{shard_id} failed: #{inspect(reason)}")
#             processor_loop_get_records(inc_backoff(%{state | conn: conn}))

#           {:disconnect, conn, reason} ->
#             Logger.error("Connection lost while processing records: #{inspect(reason)}")
#             {:ok, conn} = HTTP.close(conn)
#             %{state | conn: conn}
#             processor_connect(reset_backoff(state))
#         end

#       # TODO could be shutdown and stuff
#       other ->
#         # TODO
#         raise "Unexpected message in processor loop: #{inspect(other)}"

#         # TODO after timeout?
#     end
#   end

#   @doc false
#   def processor_get_records(state) do
#     %{
#       conn: conn,
#       stream: stream,
#       shard_id: shard_id,
#       iterator: iterator,
#       processor: processor,
#       processor_config: processor_config
#     } = state

#     # TODO Limit?
#     payload = %{
#       "ShardIterator" => iterator,
#       "StreamARN" =>
#         case stream do
#           {:arn, arn} -> arn
#           {:name, _name} -> nil
#         end,
#       "StreamName" =>
#         case stream do
#           {:arn, _arn} -> nil
#           {:name, name} -> name
#         end
#     }

#     case Xinesis.api_get_records(conn, payload) do
#       # TODO MillisBehindLatest
#       {:ok, conn, response} ->
#         Logger.debug("Received records for shard #{shard_id}: #{inspect(response)}")

#         case response do
#           %{"NextShardIterator" => next_iterator, "Records" => [_ | _] = records} ->
#             {kind, result} =
#               try do
#                 processor.(shard_id, records, processor_config)
#               catch
#                 kind, reason -> {:error, {kind, reason, __STACKTRACE__}}
#               else
#                 result -> {:continue, result}
#               end

#             exit({kind, conn, result, next_iterator})

#           %{"NextShardIterator" => next_iterator, "Records" => []} ->
#             exit({:empty, conn, next_iterator})

#           %{"ChildShards" => child_shards} ->
#             exit({:completed, child_shards})
#         end

#       {:error, _conn, _reason} = error ->
#         exit(error)

#       {:disconnect, _conn, _reason} = error ->
#         exit(error)
#     end
#   end
