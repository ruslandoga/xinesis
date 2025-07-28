defmodule Xinesis.Test do
  def with_conn(config, f) when is_function(f, 1) do
    {:ok, conn} = Xinesis.AWS.connect(config)

    try do
      f.(conn)
    else
      {:ok, _conn, response} -> response
      {:error, _conn, reason} -> raise reason
      {:disconnect, reason} -> raise reason
    after
      Mint.HTTP1.close(conn)
    end
  end
end
