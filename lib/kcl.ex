defmodule Kcl do
  @moduledoc """
  Entry point for the KCL-style Kinesis consumer.
  """

  def start_link(opts) do
    # The coordinator is the top-level process for the consumer application.
    Kcl.Coordinator.start_link(opts)
  end
end
