defmodule Kinesis do
  @moduledoc """
  Documentation for `Kinesis`.
  """

  alias Mint.HTTP1, as: HTTP

  kinesis_actions = [
    "create_stream",
    "delete_stream",
    "list_streams",
    "list_shards",
    "merge_shards",
    "put_record",
    "put_records",
    "describe_stream",
    "get_shard_iterator",
    "get_records",
    "split_shard"
  ]

  for action <- kinesis_actions do
    aws_action = Macro.camelize(action)

    @doc """
    See https://docs.aws.amazon.com/kinesis/latest/APIReference/API_#{aws_action}.html
    """
    def unquote(:"kinesis_#{action}")(conn, payload, opts \\ []) do
      headers = [
        {"x-amz-target", unquote("Kinesis_20131202.#{aws_action}")},
        {"content-type", "application/x-amz-json-1.1"}
      ]

      json = JSON.encode_to_iodata!(payload)
      request("kinesis", conn, headers, json, opts)
    end
  end

  dynamodb_actions = [
    "list_tables",
    "create_table",
    "delete_table",
    "describe_table",
    "put_item",
    "get_item",
    "update_item"
  ]

  for action <- dynamodb_actions do
    aws_action = Macro.camelize(action)

    @doc """
    See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_#{aws_action}.html
    """
    def unquote(:"dynamodb_#{action}")(conn, payload, opts \\ []) do
      headers = [
        {"x-amz-target", unquote("DynamoDB_20120810.#{aws_action}")},
        {"content-type", "application/x-amz-json-1.0"}
      ]

      json = JSON.encode_to_iodata!(payload)
      request("dynamodb", conn, headers, json, opts)
    end
  end

  defp request(service, conn, headers, body, opts) do
    with {:ok, conn, _ref} <- send_request(service, conn, headers, body) do
      receive_response(conn, timeout(conn, opts))
    end
  end

  @dialyzer {:no_improper_lists, send_request: 4}
  defp send_request(service, conn, headers, body) do
    # TODO
    access_key_id = "test"
    # TODO
    secret_access_key = "test"
    # TODO
    host = "localhost"
    # TODO
    region = "us-east-1"

    utc_now = DateTime.utc_now(:second)
    amz_date = Calendar.strftime(utc_now, "%Y%m%dT%H%M%SZ")
    amz_short_date = String.slice(amz_date, 0, 8)
    scope = IO.iodata_to_binary([amz_short_date, ?/, region, ?/, service, ?/, "aws4_request"])

    headers =
      Enum.map(headers, fn {k, v} -> {String.downcase(k), v} end)
      |> put_header("host", host)
      |> put_header("x-amz-date", amz_date)

    signed_headers =
      headers
      |> Enum.map(fn {k, _} -> k end)
      |> Enum.intersperse(?;)
      |> IO.iodata_to_binary()

    canonical_request = [
      "POST\n/\n\n",
      Enum.map(headers, fn {k, v} -> [k, ?:, v, ?\n] end),
      ?\n,
      signed_headers,
      "\nUNSIGNED-PAYLOAD"
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

    authorization = """
    AWS4-HMAC-SHA256 Credential=#{access_key_id}/#{scope},\
    SignedHeaders=#{signed_headers},\
    Signature=#{signature}\
    """

    headers = [{"authorization", authorization} | headers]

    case HTTP.request(conn, "POST", "/", headers, body) do
      {:ok, _conn, _ref} = ok -> ok
      {:error, conn, reason} -> {:disconnect, reason, conn}
    end
  end

  defp receive_response(conn, timeout) do
    with {:ok, conn, responses} <- recv_all(conn, [], timeout) do
      case responses do
        [200, headers | rest] ->
          content_type = :proplists.get_value("content-type", headers, nil)

          # TODO
          response =
            if is_binary(content_type) and String.contains?(content_type, "json") do
              JSON.decode!(IO.iodata_to_binary(rest))
            else
              responses
            end

          {:ok, conn, response}

        [status, headers | data] ->
          message = IO.iodata_to_binary(data)
          {:error, {status, headers, message}, conn}
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
        {:disconnect, reason, conn}
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

  defp timeout(_conn, _opts), do: :timer.seconds(5)

  defp put_header(headers, key, value), do: [{key, value} | List.keydelete(headers, key, 1)]
  defp hex(value), do: Base.encode16(value, case: :lower)
  defp sha256(value), do: :crypto.hash(:sha256, value)
  defp hmac_sha256(secret, value), do: :crypto.mac(:hmac, :sha256, secret, value)
  defp hex_sha256(value), do: hex(sha256(value))
  defp hex_hmac_sha256(secret, value), do: hex(hmac_sha256(secret, value))
end
