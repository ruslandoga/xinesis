defmodule Xinesis do
  @moduledoc """
  Basic AWS Kinesis client.
  """

  # TODO can maybe be a supervisor that starts a Coordinator and a DynamicSupervisor for processors?

  def start_link(opts) do
    Xinesis.Coordinator.start_link(opts)
  end

  def child_spec(opts) do
    %{
      id: Xinesis.Coordinator,
      start: {Xinesis.Coordinator, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      # TODO make sure coordinator handles shutdown gracefully
      shutdown: 5000
    }
  end
end
