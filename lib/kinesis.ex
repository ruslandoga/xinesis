defmodule Kinesis do
  @moduledoc """
  Documentation for `Kinesis`.
  """

  use GenServer
  alias Mint.HTTP1, as: HTTP

  def start_link(opts) do
    {gen_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @impl true
  def init(_opts) do
    {:ok, _conn} = HTTP.connect(:http, "localhost", 4566)
  end

  def list_streams(conn) do
    # {:done, ...} and {:more, ...}
    request(
      conn,
      "POST",
      "/",
      [
        {"x-amz-target", "Kinesis_20131202.ListStreams"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      JSON.encode_to_iodata!(%{}),
      []
    )
  end

  def describe_stream(conn, stream_name) when is_binary(stream_name) do
    request(
      conn,
      "POST",
      "/",
      [
        {"x-amz-target", "Kinesis_20131202.DescribeStream"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      JSON.encode_to_iodata!(%{"StreamName" => stream_name}),
      []
    )
  end

  # https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html
  def get_shard_iterator(conn, stream_name, shard_id, payload) do
    payload =
      Map.merge(
        %{"ShardId" => shard_id, "StreamName" => stream_name},
        payload
      )

    request(
      conn,
      "POST",
      "/",
      [
        {"x-amz-target", "Kinesis_20131202.GetShardIterator"},
        {"content-type", "application/x-amz-json-1.1"}
      ],
      JSON.encode_to_iodata!(payload),
      []
    )
  end

  defp request(conn, method, path, headers, body, opts) do
    with {:ok, conn, _ref} <- send_request(conn, method, path, headers, body) do
      receive_response(conn, timeout(conn, opts))
    end
  end

  defp send_request(conn, method, path, headers, body) do
    access_key_id = "test"
    secret_access_key = "test"

    host = "localhost"
    path = path
    query = %{}
    region = "us-east-1"
    method = method
    headers = headers
    service = "kinesis"
    utc_now = DateTime.utc_now(:second)

    amz_date = Calendar.strftime(utc_now, "%Y%m%dT%H%M%SZ")
    amz_short_date = String.slice(amz_date, 0, 8)
    scope = IO.iodata_to_binary([amz_short_date, ?/, region, ?/, service, ?/, "aws4_request"])

    headers =
      Enum.map(headers, fn {k, v} -> {String.downcase(k), v} end)
      |> put_header("host", host)
      |> put_header("x-amz-date", amz_date)

    # |> Enum.sort_by(fn {k, _} -> k end)

    # path =
    #   path
    #   |> String.split("/", trim: true)
    #   |> Enum.map(&:uri_string.quote/1)
    #   |> Enum.join("/")

    signed_headers =
      headers
      |> Enum.map(fn {k, _} -> k end)
      |> Enum.intersperse(?;)
      |> IO.iodata_to_binary()

    # query =
    #   Map.merge(
    #     %{
    #       "X-Amz-Algorithm" => "AWS4-HMAC-SHA256",
    #       "X-Amz-Credential" => "#{access_key_id}/#{scope}",
    #       "X-Amz-Date" => amz_date,
    #       "X-Amz-SignedHeaders" => signed_headers
    #     },
    #     query
    #   )

    query =
      query
      |> Enum.sort_by(fn {k, _} -> k end)
      |> URI.encode_query()

    canonical_request = [
      method,
      ?\n,
      path,
      ?\n,
      query,
      ?\n,
      Enum.map(headers, fn {k, v} -> [k, ?:, v, ?\n] end),
      ?\n,
      signed_headers,
      ?\n,
      "UNSIGNED-PAYLOAD"
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

    case HTTP.request(conn, method, path, headers, body) do
      {:ok, _conn, _ref} = ok -> ok
      {:error, conn, reason} -> {:disconnect, reason, conn}
    end
  end

  defp receive_response(conn, timeout) do
    with {:ok, conn, responses} <- recv_all(conn, [], timeout) do
      case responses do
        [200, _headers | _rest] ->
          {:ok, conn, responses}

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
