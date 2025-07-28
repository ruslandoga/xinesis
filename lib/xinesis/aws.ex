defmodule Xinesis.AWS do
  @moduledoc false
  require Logger
  alias Mint.HTTP1, as: HTTP

  defmodule Error do
    defexception [:type, :message]
    def message(%{type: nil, message: message}), do: message
    def message(%{type: type, message: message}), do: "#{type}: #{message}"
  end

  defmodule Client do
    @moduledoc false
    defstruct [:access_key_id, :secret_access_key, :region, :host]

    defimpl Inspect do
      import Inspect.Algebra

      def inspect(%{access_key_id: akid}, _opts) do
        concat(["#Xinesis.AWS.Client<", akid, ">"])
      end
    end
  end

  def connect(opts) do
    scheme = Keyword.fetch!(opts, :scheme)
    host = Keyword.fetch!(opts, :host)

    # TODO
    default_port =
      case scheme do
        :http -> 80
        :https -> 443
      end

    port = Keyword.get(opts, :port, default_port)
    access_key_id = Keyword.fetch!(opts, :access_key_id)
    secret_access_key = Keyword.fetch!(opts, :secret_access_key)
    region = Keyword.fetch!(opts, :region)

    client = %Client{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region,
      host: host
    }

    with {:ok, conn} <- HTTP.connect(scheme, host, port, mode: :passive) do
      {:ok, HTTP.put_private(conn, :client, client)}
    end
  end

  # https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html
  actions = [
    "create_stream",
    "delete_stream",
    "list_streams",
    "describe_stream_summary",
    "list_shards",
    "split_shard",
    "merge_shards",
    "udpate_shard_count",
    "get_shard_iterator",
    "get_records",
    "put_record",
    "put_records"
  ]

  for action <- actions do
    @doc false
    def unquote(:"#{action}")(conn, payload, opts \\ []) do
      client = HTTP.get_private(conn, :client) || raise "AWS client not found in Mint connection"
      json = JSON.encode_to_iodata!(payload)

      headers =
        headers(client, _service = "kinesis", json, [
          {"x-amz-target", unquote("Kinesis_20131202.#{Macro.camelize(action)}")},
          {"content-type", "application/x-amz-json-1.1"}
        ])

      request(conn, headers, json, opts)
    end
  end

  @dialyzer {:no_improper_lists, headers: 4}
  defp headers(client, service, body, headers) do
    %{
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      region: region,
      host: host
    } = client

    utc_now = DateTime.utc_now(:second)
    amz_date = Calendar.strftime(utc_now, "%Y%m%dT%H%M%SZ")
    amz_short_date = String.slice(amz_date, 0, 8)
    scope = amz_short_date <> "/" <> region <> "/" <> service <> "/aws4_request"

    amz_content_sha256 = hex_sha256(body)

    headers = [
      {"host", host},
      {"x-amz-content-sha256", amz_content_sha256},
      {"x-amz-date", amz_date} | headers
    ]

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

  @compile inline: [hex: 1, sha256: 1, hmac_sha256: 2, hex_sha256: 1, hex_hmac_sha256: 2]
  defp hex(value), do: Base.encode16(value, case: :lower)
  defp sha256(value), do: :crypto.hash(:sha256, value)
  defp hmac_sha256(secret, value), do: :crypto.mac(:hmac, :sha256, secret, value)
  defp hex_sha256(value), do: hex(sha256(value))
  defp hex_hmac_sha256(secret, value), do: hex(hmac_sha256(secret, value))

  defp request(conn, headers, body, opts) do
    result =
      with {:ok, conn, _ref} <- send_request(conn, headers, body) do
        timeout = Keyword.get(opts, :timeout, :timer.seconds(5))
        receive_response(conn, timeout)
      end

    with {:disconnect, conn, reason} <- result do
      HTTP.close(conn)
      {:disconnect, reason}
    end
  end

  defp send_request(conn, headers, body) do
    case HTTP.request(conn, "POST", "/", headers, body) do
      {:ok, _conn, _ref} = ok -> ok
      {:error, conn, reason} -> {:disconnect, conn, reason}
    end
  end

  defp receive_response(conn, timeout) do
    with {:ok, conn, responses} <- recv_all(conn, [], timeout) do
      [status, headers | rest] = responses
      data = IO.iodata_to_binary(rest)
      content_type = :proplists.get_value("content-type", headers, nil)
      content_type_json? = is_binary(content_type) and String.contains?(content_type, "json")

      if status >= 200 and status < 300 do
        content_type_json? || raise "unexpected content-type: #{content_type}"

        response =
          case data do
            "" -> nil
            _ -> JSON.decode!(data)
          end

        {:ok, conn, response}
      else
        error_type = :proplists.get_value("x-amzn-errortype", headers, nil)

        error =
          if content_type_json? do
            json = JSON.decode!(data)
            Error.exception(type: json["__type"] || error_type, message: json["message"] || data)
          else
            Error.exception(type: error_type || Integer.to_string(status), message: data)
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

  for tag <- [:data, :status, :headers] do
    defp handle_responses([{unquote(tag), _ref, data} | rest], acc) do
      handle_responses(rest, [data | acc])
    end
  end

  defp handle_responses([{:done, _ref}], acc), do: {:ok, :lists.reverse(acc)}
  defp handle_responses([], acc), do: {:more, acc}
end
