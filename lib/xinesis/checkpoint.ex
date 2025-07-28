defmodule Xinesis.Checkpoint do
  # TODO fencing tokens

  @callback acquire_lease(stream_arn :: String.t(), shard_id :: String.t()) ::
              {:ok, sequence_number :: String.t()} | {:error, Exception.t()}

  @callback release_lease(stream_arn :: String.t(), shard_id :: String.t()) ::
              :ok | {:error, Exception.t()}

  @callback checkpoint(
              stream_arn :: String.t(),
              shard_id :: String.t(),
              sequence_number :: String.t()
            ) :: :ok | {:error, Exception.t()}

  @callback finish_shard(
              stream_arn :: String.t(),
              shard_id :: String.t()
            ) ::
              :ok | {:error, Exception.t()}
end
