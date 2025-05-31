defmodule KinesisTest do
  use ExUnit.Case
  doctest Kinesis

  test "greets the world" do
    assert Kinesis.hello() == :world
  end
end
