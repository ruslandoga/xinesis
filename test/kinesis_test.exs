defmodule KinesisTest do
  use ExUnit.Case

  test "it works" do
    {:ok, _pid} = Kinesis.start_link([])
  end

  test "list_shards/1" do
    {:ok, conn} = Mint.HTTP1.connect(:http, "localhost", 4566, mode: :passive)
    assert {:ok, _conn, [200, _headers, body]} = Kinesis.list_shards(conn)

    assert JSON.decode!(body) == %{
             "HasMoreStreams" => false,
             "StreamNames" => ["my-local-stream"]
           }
  end
end
