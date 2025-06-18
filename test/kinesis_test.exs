defmodule KinesisTest do
  use ExUnit.Case

  setup %{test: test} do
    uniq =
      Base.hex_encode32(
        <<
          System.system_time(:nanosecond)::64,
          :erlang.phash2({node(), self(), test}, 16_777_216)::24,
          :erlang.unique_integer()::32
        >>,
        case: :lower
      )

    stream_name = "knock-elixir-ci-test-stream-#{uniq}"
    table_name = "knock-elixir-ci-checkpoint-table-#{uniq}"
    {:ok, conn: conn(:kinesis), stream_name: stream_name, table_name: table_name}
  end

  describe "basic flow & iterator logic" do
    setup [:create_stream, :await_stream_active]

    test "consume all records with pagination", %{conn: conn, stream_name: stream_name} do
      {:ok, conn, %{"FailedRecordCount" => 0}} =
        Kinesis.api_put_records(conn, %{
          "StreamName" => stream_name,
          "Records" =>
            Enum.map(1..100, fn i ->
              %{
                "Data" => Base.encode64("data-#{i}"),
                "PartitionKey" => "key-#{i}"
              }
            end)
        })

      assert {:ok, conn, %{"ShardIterator" => shard_iterator}} =
               Kinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => "shardId-000000000000",
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, conn, resp} =
               Kinesis.api_get_records(conn, %{
                 "Limit" => 10,
                 "ShardIterator" => shard_iterator
               })

      assert %{"Records" => records, "NextShardIterator" => shard_iterator} = resp
      assert length(records) == 10

      assert {:ok, conn, resp} =
               Kinesis.api_get_records(conn, %{
                 "Limit" => 50,
                 "ShardIterator" => shard_iterator
               })

      assert %{"Records" => records, "NextShardIterator" => shard_iterator} = resp
      assert length(records) == 50

      assert {:ok, conn, resp} =
               Kinesis.api_get_records(conn, %{
                 "Limit" => 50,
                 "ShardIterator" => shard_iterator
               })

      assert %{"Records" => records, "NextShardIterator" => shard_iterator} = resp
      assert length(records) == 40

      assert {:ok, _conn, resp} =
               Kinesis.api_get_records(conn, %{
                 "Limit" => 50,
                 "ShardIterator" => shard_iterator
               })

      assert %{"Records" => []} = resp
    end

    test "shard iterator types", %{conn: conn, stream_name: stream_name} do
      {:ok, conn, %{"SequenceNumber" => seq1, "ShardId" => "shardId-000000000000"}} =
        Kinesis.api_put_record(conn, %{
          "StreamName" => stream_name,
          "Data" => Base.encode64("record-1"),
          "PartitionKey" => "key"
        })

      {:ok, conn, %{"SequenceNumber" => seq2, "ShardId" => "shardId-000000000000"}} =
        Kinesis.api_put_record(conn, %{
          "StreamName" => stream_name,
          "Data" => Base.encode64("record-2"),
          "PartitionKey" => "key"
        })

      # TRIM_HORIZON gets all records
      assert {:ok, conn, %{"ShardIterator" => shard_iterator_trim}} =
               Kinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => "shardId-000000000000",
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, conn,
              %{"Records" => [%{"SequenceNumber" => ^seq1}, %{"SequenceNumber" => ^seq2}]}} =
               Kinesis.api_get_records(conn, %{"ShardIterator" => shard_iterator_trim})

      # LATEST gets no records ...
      assert {:ok, conn, %{"ShardIterator" => shart_iterator_latest}} =
               Kinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => "shardId-000000000000",
                 "ShardIteratorType" => "LATEST"
               })

      assert {:ok, conn, %{"Records" => []}} =
               Kinesis.api_get_records(conn, %{"ShardIterator" => shart_iterator_latest})

      # ... until a new record is added ...
      {:ok, conn, %{"SequenceNumber" => seq3, "ShardId" => "shardId-000000000000"}} =
        Kinesis.api_put_record(conn, %{
          "StreamName" => stream_name,
          "Data" => Base.encode64("record-3"),
          "PartitionKey" => "key"
        })

      # ... and then it gets the latest record
      assert {:ok, conn, %{"Records" => [%{"SequenceNumber" => ^seq3}]}} =
               Kinesis.api_get_records(conn, %{"ShardIterator" => shart_iterator_latest})

      # AT_SEQUENCE_NUMBER
      {:ok, conn, %{"ShardIterator" => shard_iterator_at_seq}} =
        Kinesis.api_get_shard_iterator(conn, %{
          "StreamName" => stream_name,
          "ShardId" => "shardId-000000000000",
          "ShardIteratorType" => "AT_SEQUENCE_NUMBER",
          "StartingSequenceNumber" => seq1
        })

      assert {:ok, _conn,
              %{
                "Records" => [
                  %{"SequenceNumber" => ^seq1},
                  %{"SequenceNumber" => ^seq2},
                  %{"SequenceNumber" => ^seq3}
                ]
              }} =
               Kinesis.api_get_records(conn, %{"ShardIterator" => shard_iterator_at_seq})

      # AFTER_SEQUENCE_NUMBER
      # {:ok, conn, %{"ShardIterator" => shard_iterator_after_seq}} =
      #   Kinesis.api_get_shard_iterator(conn, %{
      #     "StreamName" => stream_name,
      #     "ShardId" => "shardId-000000000000",
      #     "ShardIteratorType" => "AFTER_SEQUENCE_NUMBER",
      #     "StartingSequenceNumber" => seq1
      #   })

      # assert {:ok, _conn,
      #         %{
      #           "Records" => [
      #             %{"SequenceNumber" => ^seq2},
      #             %{"SequenceNumber" => ^seq3}
      #           ]
      #         }} =
      #          Kinesis.api_get_records(conn, %{"ShardIterator" => shard_iterator_after_seq})

      # TODO AT_TIMESTAMP ???
    end

    @tag :skip
    test "expired shard iterator"
  end

  describe "checkpointing" do
    @describetag :skip
    setup [:create_stream, :await_stream_active, :create_table]

    test "processing records and updating checkpoint", %{
      conn: conn,
      stream_name: stream_name,
      table_name: table_name
    } do
      owner = "uniq_lease_owner"

      {:ok, conn, %{"FailedRecordCount" => 0}} =
        Kinesis.api_put_records(conn, %{
          "StreamName" => stream_name,
          "Records" =>
            Enum.map(1..100, fn i ->
              %{
                "Data" => Base.encode64("data-#{i}"),
                "PartitionKey" => "key-#{i}"
              }
            end)
        })

      assert {:ok, conn, %{"Shards" => [%{"ShardId" => shard_id}]}} =
               Kinesis.api_list_shards(conn, %{"StreamName" => stream_name})

      assert {:ok, conn, _not_found = %{}} =
               Kinesis.get_lease(conn, table_name, shard_id)

      assert {:ok, conn, _ok = %{}} =
               Kinesis.create_lease(conn, table_name, shard_id, owner)

      assert {:ok, conn, %{"Item" => lease}} =
               Kinesis.get_lease(conn, table_name, shard_id)

      assert lease == %{
               "completed" => %{"BOOL" => false},
               "lease_count" => %{"N" => "1"},
               "lease_owner" => %{"S" => owner},
               "shard_id" => %{"S" => shard_id}
             }

      assert {:ok, conn, %{"ShardIterator" => shard_iterator}} =
               Kinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => shard_id,
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, conn, %{"NextShardIterator" => shard_iterator, "Records" => [_ | _]}} =
               Kinesis.api_get_records(conn, %{
                 "Limit" => 100,
                 "ShardIterator" => shard_iterator
               })

      assert {:ok, _conn, %{"Attributes" => %{"checkpoint" => %{"S" => ^shard_iterator}}}} =
               Kinesis.update_checkpoint(conn, table_name, shard_id, owner, shard_iterator)
    end

    @tag :skip
    test "resuming processing from checkpoint"
    @tag :skip
    test "failed checkpoint write"
  end

  describe "shard splitting & merging" do
    @describetag :skip
    setup [:create_stream, :await_stream_active, :create_table]

    test "basic flow", %{conn: conn, stream_name: stream_name} = ctx do
      {:ok, conn, %{"FailedRecordCount" => 0}} =
        Kinesis.api_put_records(conn, %{
          "StreamName" => stream_name,
          "Records" =>
            Enum.map(1..10, fn i ->
              %{
                "Data" => Base.encode64("data-#{i}"),
                "PartitionKey" => "key-#{i}"
              }
            end)
        })

      {:ok, conn,
       %{
         "Shards" => [
           %{
             "ShardId" => shard_id,
             "HashKeyRange" => %{
               "EndingHashKey" => ending_hash_key,
               "StartingHashKey" => starting_hash_key
             }
           }
         ]
       }} = Kinesis.api_list_shards(conn, %{"StreamName" => stream_name})

      starting_hash_key = String.to_integer(starting_hash_key)
      ending_hash_key = String.to_integer(ending_hash_key)

      assert {:ok, conn, %{}} =
               Kinesis.api_split_shard(conn, %{
                 "StreamName" => stream_name,
                 "ShardToSplit" => shard_id,
                 "NewStartingHashKey" =>
                   Integer.to_string(div(starting_hash_key + ending_hash_key, 2))
               })

      :ok = await_stream_active(ctx)

      {:ok, conn, %{"FailedRecordCount" => 0}} =
        Kinesis.api_put_records(conn, %{
          "StreamName" => stream_name,
          "Records" =>
            Enum.map(1..10, fn i ->
              %{
                "Data" => Base.encode64("data-#{i}"),
                "PartitionKey" => "key-#{i}"
              }
            end)
        })

      assert {:ok, conn,
              %{
                "Shards" => [
                  %{
                    "HashKeyRange" => %{
                      "EndingHashKey" => "340282366920938463463374607431768211455",
                      "StartingHashKey" => "0"
                    },
                    "ShardId" => ^shard_id
                  },
                  %{
                    "HashKeyRange" => %{
                      "EndingHashKey" => "170141183460469231731687303715884105726",
                      "StartingHashKey" => "0"
                    },
                    "ParentShardId" => ^shard_id,
                    "ShardId" => "shardId-000000000001"
                  },
                  %{
                    "HashKeyRange" => %{
                      "EndingHashKey" => "340282366920938463463374607431768211455",
                      "StartingHashKey" => "170141183460469231731687303715884105727"
                    },
                    "ParentShardId" => ^shard_id,
                    "ShardId" => "shardId-000000000002"
                  }
                ]
              }} = Kinesis.api_list_shards(conn, %{"StreamName" => stream_name})

      assert {:ok, conn, %{}} =
               Kinesis.api_merge_shards(conn, %{
                 "StreamName" => stream_name,
                 "ShardToMerge" => "shardId-000000000001",
                 "AdjacentShardToMerge" => "shardId-000000000002"
               })

      :ok = await_stream_active(ctx)

      {:ok, conn, %{"FailedRecordCount" => 0}} =
        Kinesis.api_put_records(conn, %{
          "StreamName" => stream_name,
          "Records" =>
            Enum.map(1..10, fn i ->
              %{
                "Data" => Base.encode64("data-#{i}"),
                "PartitionKey" => "key-#{i}"
              }
            end)
        })

      assert {:ok, conn, %{"Shards" => [_, _, _, _]}} =
               Kinesis.api_list_shards(conn, %{"StreamName" => stream_name})

      assert {:ok, conn, %{"ShardIterator" => shard_iterator}} =
               Kinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => shard_id,
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, conn,
              %{
                "ChildShards" => [
                  %{"ShardId" => "shardId-000000000001"},
                  %{"ShardId" => "shardId-000000000002"}
                ],
                "Records" => [_ | _]
              }} =
               Kinesis.api_get_records(conn, %{
                 "Limit" => 10,
                 "ShardIterator" => shard_iterator
               })

      assert {:ok, conn, %{"ShardIterator" => shard_iterator_child_1}} =
               Kinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => "shardId-000000000001",
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, conn,
              %{
                "ChildShards" => [
                  %{
                    "HashKeyRange" => %{
                      "EndingHashKey" => "340282366920938463463374607431768211455",
                      "StartingHashKey" => "0"
                    },
                    "ParentShards" => ["shardId-000000000001", "shardId-000000000002"],
                    "ShardId" => "shardId-000000000003"
                  }
                ],
                "Records" => [_ | _]
              }} =
               Kinesis.api_get_records(conn, %{
                 "Limit" => 10,
                 "ShardIterator" => shard_iterator_child_1
               })

      assert {:ok, conn, %{"ShardIterator" => shard_iterator_child_2}} =
               Kinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => "shardId-000000000002",
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, conn,
              %{
                "ChildShards" => [
                  %{
                    "HashKeyRange" => %{
                      "EndingHashKey" => "340282366920938463463374607431768211455",
                      "StartingHashKey" => "0"
                    },
                    "ParentShards" => ["shardId-000000000001", "shardId-000000000002"],
                    "ShardId" => "shardId-000000000003"
                  }
                ],
                "Records" => [_ | _]
              }} =
               Kinesis.api_get_records(conn, %{
                 "Limit" => 10,
                 "ShardIterator" => shard_iterator_child_2
               })

      assert {:ok, conn, %{"ShardIterator" => shard_iterator_child_3}} =
               Kinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => "shardId-000000000003",
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, _conn, %{"NextShardIterator" => _, "Records" => [_ | _]}} =
               Kinesis.api_get_records(conn, %{
                 "Limit" => 10,
                 "ShardIterator" => shard_iterator_child_3
               })
    end
  end

  describe "error handling and edge cases" do
    @describetag :skip
    test "stream not found"
    test "DynamoDB table for checkpoints not found"
    test "throttling (ProvisionedThroughputExceededException)"
    test "processing empty GetRecords response"
    test "stream deletion and recreation"
  end

  # todo https://github.com/uberbrodt/kcl_ex/blob/master/lib/kinesis_client/stream/app_state/dynamo.ex

  defp conn(service) do
    host =
      case service do
        :kinesis -> "kinesis.eu-north-1.amazonaws.com"
        :dynamodb -> "dynamodb.eu-north-1.api.aws"
      end

    {:ok, conn} = Mint.HTTP1.connect(:https, host, 443, mode: :passive)

    conn
  end

  defp with_conn(service, f) when is_function(f, 1) do
    conn = conn(service)

    try do
      f.(conn)
    after
      Mint.HTTP1.close(conn)
    end
  end

  defp create_stream(%{stream_name: stream_name} = ctx) do
    with_conn(:kinesis, fn conn ->
      {:ok, _, _} =
        Kinesis.api_create_stream(conn, %{"StreamName" => stream_name, "ShardCount" => 1})
    end)

    on_exit(fn ->
      :ok = await_stream_active(ctx)

      with_conn(:kinesis, fn conn ->
        {:ok, _, _} = Kinesis.api_delete_stream(conn, %{"StreamName" => stream_name})
      end)
    end)
  end

  defp await_stream_active(%{stream_name: stream_name}) do
    :active =
      with_conn(:kinesis, fn conn ->
        delays = List.duplicate(500, 20)

        Enum.reduce_while(delays, conn, fn delay, conn ->
          IO.puts("Waiting for stream '#{stream_name}' to become ACTIVE...")

          {:ok, conn, resp} =
            Kinesis.api_describe_stream_summary(conn, %{"StreamName" => stream_name})

          %{"StreamDescriptionSummary" => %{"StreamStatus" => status}} = resp

          if status == "ACTIVE" do
            {:halt, :active}
          else
            :timer.sleep(delay)
            {:cont, conn}
          end
        end)
      end)

    :ok
  end

  defp create_table(%{table_name: table_name}) do
    with_conn(:dynamodb, fn conn ->
      {:ok, _, _} =
        Kinesis.dynamodb_create_table(conn, %{
          "TableName" => table_name,
          "KeySchema" => [
            %{
              "AttributeName" => "shard_id",
              "KeyType" => "HASH"
            }
          ],
          "AttributeDefinitions" => [
            %{
              "AttributeName" => "shard_id",
              "AttributeType" => "S"
            }
          ],
          "ProvisionedThroughput" => %{
            "ReadCapacityUnits" => 10,
            "WriteCapacityUnits" => 10
          }
        })
    end)

    on_exit(fn ->
      with_conn(:dynamodb, fn conn ->
        {:ok, _, _} = Kinesis.dynamodb_delete_table(conn, %{"TableName" => table_name})
      end)
    end)

    :ok
  end
end
