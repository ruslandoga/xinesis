defmodule Xinesis.Producer do
  @behaviour GenStage
  @behaviour Broadway.Producer

  @impl true
  def prepare_for_start(module, options) do
  end
end
