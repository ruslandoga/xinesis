defmodule Xinesis.AWSError do
  @moduledoc "TODO"
  defexception [:type, :message]

  def message(%{type: nil, message: message}) do
    message
  end

  def message(%{type: type, message: message}) do
    "#{type}: #{message}"
  end
end

defmodule Xinesis.AWS do
  @moduledoc false
  alias Mint.HTTP1, as: HTTP
  alias Xinesis.AWSError, as: AWSError

  def connect(client) when is_map(client) do
    %{scheme: scheme, host: host, port: port} = client
    # TODO allow providing custom connection options and timeout
    HTTP.connect(scheme, host, port, mode: :passive)
  end

  # https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html
  kinesis_actions = [
    "create_stream",
    "delete_stream",
    "list_streams",
    "describe_stream_summary",
    "list_shards",
    "split_shard",
    "merge_shards",
    "update_shard_count",
    "get_shard_iterator",
    "get_records",
    "put_record",
    "put_records",
    "deregister_stream_consumer",
    "register_stream_consumer"
  ]

  for action <- kinesis_actions do
    def unquote(:"kinesis_#{action}")(conn, client, payload, opts \\ []) do
      json = JSON.encode_to_iodata!(payload)

      headers =
        headers(client, "kinesis", json, [
          {"x-amz-target", unquote("Kinesis_20131202.#{Macro.camelize(action)}")},
          {"content-type", "application/x-amz-json-1.1"},
          {"host", conn.host}
        ])

      request(conn, headers, json, opts)
    end
  end

  # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations_Amazon_DynamoDB.html
  dynamodb_actions = [
    "create_table",
    "delete_table",
    "put_item",
    "get_item",
    "update_item"
  ]

  for action <- dynamodb_actions do
    def unquote(:"dynamodb_#{action}")(conn, client, payload, opts \\ []) do
      json = JSON.encode_to_iodata!(payload)

      headers =
        headers(client, "dynamodb", json, [
          {"x-amz-target", unquote("DynamoDB_20120810.#{Macro.camelize(action)}")},
          {"content-type", "application/x-amz-json-1.0"},
          {"host", conn.host}
        ])

      request(conn, headers, json, opts)
    end
  end

  defp request(conn, headers, body, opts) do
    result =
      with {:ok, conn, _ref} <- send_request(conn, headers, body) do
        receive_response(conn, timeout(conn, opts))
      end

    with {:disconnect, conn, reason} <- result do
      {:ok, _conn} = HTTP.close(conn)
      {:disconnect, reason}
    end
  end

  defp send_request(conn, headers, body) do
    case HTTP.request(conn, "POST", "/", headers, body) do
      {:ok, _conn, _ref} = ok -> ok
      {:error, conn, reason} -> {:disconnect, conn, reason}
    end
  end

  @dialyzer {:no_improper_lists, headers: 4}
  defp headers(client, service, body, headers) do
    %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region
    } = client

    utc_now = DateTime.utc_now(:second)
    amz_date = Calendar.strftime(utc_now, "%Y%m%dT%H%M%SZ")
    amz_short_date = String.slice(amz_date, 0, 8)
    scope = amz_short_date <> "/" <> region <> "/" <> service <> "/aws4_request"

    amz_content_sha256 = hex_sha256(body)

    headers = [{"x-amz-content-sha256", amz_content_sha256}, {"x-amz-date", amz_date} | headers]
    headers = Enum.sort_by(headers, fn {k, _} -> k end)

    signed_headers =
      headers
      |> Enum.map_intersperse(?;, fn {k, _} -> k end)
      |> IO.iodata_to_binary()

    canonical_request =
      [
        "POST\n/\n\n",
        Enum.map(headers, fn {k, v} -> [k, ?:, v, ?\n] end),
        ?\n,
        signed_headers,
        ?\n,
        amz_content_sha256
      ]

    string_to_sign = [
      "AWS4-HMAC-SHA256\n",
      amz_date,
      ?\n,
      scope,
      ?\n,
      hex_sha256(canonical_request)
    ]

    signing_key =
      ["AWS4" | secret_access_key]
      |> hmac_sha256(amz_short_date)
      |> hmac_sha256(region)
      |> hmac_sha256(service)
      |> hmac_sha256("aws4_request")

    signature = hex_hmac_sha256(signing_key, string_to_sign)

    authorization =
      """
      AWS4-HMAC-SHA256 Credential=#{access_key_id}/#{scope},\
      SignedHeaders=#{signed_headers},\
      Signature=#{signature}\
      """

    [{"authorization", authorization} | headers]
  end

  defp receive_response(conn, timeout) do
    with {:ok, conn, responses} <- recv_all(conn, [], timeout) do
      case responses do
        [status, headers | rest] when status >= 200 and status < 300 ->
          content_type =
            :proplists.get_value("content-type", headers, nil) ||
              raise "missing content-type header"

          String.contains?(content_type, "json") ||
            raise "unexpected content-type: #{content_type}"

          data = IO.iodata_to_binary(rest)

          response =
            case data do
              "" -> nil
              _ -> JSON.decode!(data)
            end

          {:ok, conn, response}

        [status, headers | data] when status >= 400 and status < 600 ->
          content_type = :proplists.get_value("content-type", headers, nil)
          error_type = :proplists.get_value("x-amzn-errortype", headers, nil)
          data = IO.iodata_to_binary(data)

          # TODO
          error =
            if is_binary(content_type) and String.contains?(content_type, "json") do
              json = JSON.decode!(data)

              AWSError.exception(
                type: json["__type"] || error_type,
                message: json["message"] || data
              )
            else
              AWSError.exception(
                type: error_type || Integer.to_string(status),
                message: data
              )
            end

          {:error, conn, error}
      end
    end
  end

  defp recv_all(conn, acc, timeout) do
    case HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case handle_responses(responses, acc) do
          {:ok, responses} -> {:ok, conn, responses}
          {:more, acc} -> recv_all(conn, acc, timeout)
        end

      {:error, conn, reason, _responses} ->
        {:disconnect, conn, reason}
    end
  end

  # TODO trailers?
  for tag <- [:data, :status, :headers] do
    defp handle_responses([{unquote(tag), _ref, data} | rest], acc) do
      handle_responses(rest, [data | acc])
    end
  end

  defp handle_responses([{:done, _ref}], acc), do: {:ok, :lists.reverse(acc)}
  defp handle_responses([], acc), do: {:more, acc}

  # TODO also consider connect timeout
  # TODO
  defp timeout(_conn, opts), do: Keyword.get(opts, :timeout, :timer.seconds(5))

  defp hex(value), do: Base.encode16(value, case: :lower)
  defp sha256(value), do: :crypto.hash(:sha256, value)
  defp hmac_sha256(secret, value), do: :crypto.mac(:hmac, :sha256, secret, value)
  defp hex_sha256(value), do: hex(sha256(value))
  defp hex_hmac_sha256(secret, value), do: hex(hmac_sha256(secret, value))
end
