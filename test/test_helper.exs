# TODO: check kinesis ports
kinesis_available? =
  case :httpc.request(:get, {~c"http://localhost:8123/ping", []}, [], []) do
    {:ok, {{_version, _status = 200, _reason}, _headers, ~c"Ok.\n"}} ->
      true

    {:error, {:failed_connect, [{:to_address, _to_address}, {:inet, [:inet], :econnrefused}]}} ->
      false
  end

unless kinesis_available? do
  Mix.shell().error("""
  Kinesis mock is not detected! Please start the local container with the following command:

      docker compose up -d localstack
  """)
end

ExUnit.start()
