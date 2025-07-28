localstack_available? =
  case :httpc.request(:get, {~c"http://localhost:4566/_localstack/health", []}, [], []) do
    {:ok, {{_version, _status = 200, _reason}, _headers, json}} ->
      %{"services" => %{"kinesis" => kinesis, "dynamodb" => dynamodb}} =
        JSON.decode!(List.to_string(json))

      kinesis in ["running", "available"] and dynamodb in ["running", "available"]

    {:error, {:failed_connect, [{:to_address, _to_address}, {:inet, [:inet], :econnrefused}]}} ->
      false
  end

unless localstack_available? do
  Mix.shell().error("""
  Localstack is not detected! Please start the container with the following command:

      docker compose up -d localstack
  """)

  System.halt(1)
end

ExUnit.start(exclude: [:aws])
