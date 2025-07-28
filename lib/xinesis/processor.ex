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
    {:ok, :disconnected, opts, {:next_event, :internal, {:connect, 0}}}
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

  defp backoff(base, max, failure_count) do
    factor = :math.pow(2, failure_count)
    max_sleep = trunc(min(max, base * factor))
    :rand.uniform(max_sleep)
  end
end
