defmodule Xinesis do
  @moduledoc "Kinesis client for Elixir"
  use Supervisor

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  # TODO validate config with nimble_options
  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name, __MODULE__)

    registry = Module.concat(name, Registry)
    shard_supervisor = Module.concat(name, DynamicSupervisor)
    coordinator = Module.concat(name, Coordinator)

    children = [
      {Registry, keys: :unique, name: registry},
      {DynamicSupervisor, name: shard_supervisor, strategy: :one_for_one},
      {Xinesis.Coordinator,
       name: coordinator, registry: registry, shard_supervisor: shard_supervisor, config: opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
