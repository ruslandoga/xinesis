defmodule Xinesis.LocalstackTest do
  use ExUnit.Case

  setup %{test: test} do
    test_base64 = Base.encode64(Atom.to_string(test), padding: false)
    stream_name = "test_stream_#{test_base64}"
    table_name = "test_table_#{test_base64}"
    {:ok, conn: conn(), stream_name: stream_name, table_name: table_name}
  end

  test "it works", ctx do
    create_stream(ctx, shard_count: 2)

    test = self()

    {:ok, xinesis} =
      Xinesis.start_link(
        scheme: :http,
        host: "localhost",
        port: 4566,
        region: "us-east-1",
        access_key_id: "test",
        secret_access_key: "test",
        stream_name: ctx.stream_name,
        processor: fn shard_id, records, nil ->
          send(test, {:process, shard_id, records})
        end
      )

    :ok = await_stream_active(ctx)

    {:ok, _conn, _} =
      Xinesis.api_put_records(ctx.conn, %{
        "StreamName" => ctx.stream_name,
        "Records" => [
          %{"Data" => Base.encode64("test-data-1"), "PartitionKey" => "key-1"},
          %{"Data" => Base.encode64("test-data-2"), "PartitionKey" => "key-2"}
        ]
      })

    assert_receive {:process, "shardId-000000000000", [%{"PartitionKey" => "key-1"}]},
                   _timeout = :timer.seconds(1)

    assert_receive {:process, "shardId-000000000001", [%{"PartitionKey" => "key-2"}]},
                   _timeout = :timer.seconds(1)

    # TODO
    Process.exit(xinesis, :shutdown)
  end

  describe "basic flow & iterator logic" do
    setup [:create_stream, :await_stream_active]

    test "consume all records with pagination", %{conn: conn, stream_name: stream_name} do
      {:ok, conn, %{"FailedRecordCount" => 0}} =
        Xinesis.api_put_records(conn, %{
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
               Xinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => "shardId-000000000000",
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, conn, resp} =
               Xinesis.api_get_records(conn, %{
                 "Limit" => 10,
                 "ShardIterator" => shard_iterator
               })

      assert %{"Records" => records, "NextShardIterator" => shard_iterator} = resp
      assert length(records) == 10

      assert {:ok, conn, resp} =
               Xinesis.api_get_records(conn, %{
                 "Limit" => 50,
                 "ShardIterator" => shard_iterator
               })

      assert %{"Records" => records, "NextShardIterator" => shard_iterator} = resp
      assert length(records) == 50

      assert {:ok, conn, resp} =
               Xinesis.api_get_records(conn, %{
                 "Limit" => 50,
                 "ShardIterator" => shard_iterator
               })

      assert %{"Records" => records, "NextShardIterator" => shard_iterator} = resp
      assert length(records) == 40

      assert {:ok, _conn, resp} =
               Xinesis.api_get_records(conn, %{
                 "Limit" => 50,
                 "ShardIterator" => shard_iterator
               })

      assert %{"Records" => []} = resp
    end

    test "shard iterator types", %{conn: conn, stream_name: stream_name} do
      {:ok, conn, %{"SequenceNumber" => seq1, "ShardId" => "shardId-000000000000"}} =
        Xinesis.api_put_record(conn, %{
          "StreamName" => stream_name,
          "Data" => Base.encode64("record-1"),
          "PartitionKey" => "key"
        })

      {:ok, conn, %{"SequenceNumber" => seq2, "ShardId" => "shardId-000000000000"}} =
        Xinesis.api_put_record(conn, %{
          "StreamName" => stream_name,
          "Data" => Base.encode64("record-2"),
          "PartitionKey" => "key"
        })

      # TRIM_HORIZON gets all records
      assert {:ok, conn, %{"ShardIterator" => shard_iterator_trim}} =
               Xinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => "shardId-000000000000",
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, conn,
              %{"Records" => [%{"SequenceNumber" => ^seq1}, %{"SequenceNumber" => ^seq2}]}} =
               Xinesis.api_get_records(conn, %{"ShardIterator" => shard_iterator_trim})

      # LATEST gets no records ...
      assert {:ok, conn, %{"ShardIterator" => shart_iterator_latest}} =
               Xinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => "shardId-000000000000",
                 "ShardIteratorType" => "LATEST"
               })

      assert {:ok, conn, %{"Records" => []}} =
               Xinesis.api_get_records(conn, %{"ShardIterator" => shart_iterator_latest})

      # ... until a new record is added ...
      {:ok, conn, %{"SequenceNumber" => seq3, "ShardId" => "shardId-000000000000"}} =
        Xinesis.api_put_record(conn, %{
          "StreamName" => stream_name,
          "Data" => Base.encode64("record-3"),
          "PartitionKey" => "key"
        })

      # ... and then it gets the latest record
      assert {:ok, conn, %{"Records" => [%{"SequenceNumber" => ^seq3}]}} =
               Xinesis.api_get_records(conn, %{"ShardIterator" => shart_iterator_latest})

      # AT_SEQUENCE_NUMBER
      {:ok, conn, %{"ShardIterator" => shard_iterator_at_seq}} =
        Xinesis.api_get_shard_iterator(conn, %{
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
               Xinesis.api_get_records(conn, %{"ShardIterator" => shard_iterator_at_seq})

      # AFTER_SEQUENCE_NUMBER
      # {:ok, conn, %{"ShardIterator" => shard_iterator_after_seq}} =
      #   Xinesis.api_get_shard_iterator(conn, %{
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
      #          Xinesis.api_get_records(conn, %{"ShardIterator" => shard_iterator_after_seq})

      # TODO AT_TIMESTAMP ???
    end

    @tag :skip
    test "expired shard iterator"
  end

  describe "checkpointing" do
    setup [:create_stream, :await_stream_active, :create_table]

    test "processing records and updating checkpoint", %{
      conn: conn,
      stream_name: stream_name,
      table_name: table_name
    } do
      owner = "uniq_lease_owner"

      {:ok, conn, %{"FailedRecordCount" => 0}} =
        Xinesis.api_put_records(conn, %{
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
               Xinesis.api_list_shards(conn, %{"StreamName" => stream_name})

      assert {:ok, conn, _not_found = %{}} =
               Xinesis.get_lease(conn, table_name, shard_id)

      assert {:ok, conn, _ok = %{}} =
               Xinesis.create_lease(conn, table_name, shard_id, owner)

      assert {:ok, conn, %{"Item" => lease}} =
               Xinesis.get_lease(conn, table_name, shard_id)

      assert lease == %{
               "completed" => %{"BOOL" => false},
               "lease_count" => %{"N" => "1"},
               "lease_owner" => %{"S" => owner},
               "shard_id" => %{"S" => shard_id}
             }

      assert {:ok, conn, %{"ShardIterator" => shard_iterator}} =
               Xinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => shard_id,
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, conn, %{"NextShardIterator" => shard_iterator, "Records" => [_ | _]}} =
               Xinesis.api_get_records(conn, %{
                 "Limit" => 100,
                 "ShardIterator" => shard_iterator
               })

      assert {:ok, _conn, %{"Attributes" => %{"checkpoint" => %{"S" => ^shard_iterator}}}} =
               Xinesis.update_checkpoint(conn, table_name, shard_id, owner, shard_iterator)
    end

    @tag :skip
    test "resuming processing from checkpoint"
    @tag :skip
    test "failed checkpoint write"
  end

  describe "shard splitting & merging" do
    setup [:create_stream, :await_stream_active, :create_table]

    test "basic flow", %{conn: conn, stream_name: stream_name} = ctx do
      {:ok, conn, %{"FailedRecordCount" => 0}} =
        Xinesis.api_put_records(conn, %{
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
       }} = Xinesis.api_list_shards(conn, %{"StreamName" => stream_name})

      starting_hash_key = String.to_integer(starting_hash_key)
      ending_hash_key = String.to_integer(ending_hash_key)

      assert {:ok, conn, %{}} =
               Xinesis.api_split_shard(conn, %{
                 "StreamName" => stream_name,
                 "ShardToSplit" => shard_id,
                 "NewStartingHashKey" =>
                   Integer.to_string(div(starting_hash_key + ending_hash_key, 2))
               })

      :ok = await_stream_active(ctx)

      {:ok, conn, %{"FailedRecordCount" => 0}} =
        Xinesis.api_put_records(conn, %{
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
              }} = Xinesis.api_list_shards(conn, %{"StreamName" => stream_name})

      assert {:ok, conn, %{}} =
               Xinesis.api_merge_shards(conn, %{
                 "StreamName" => stream_name,
                 "ShardToMerge" => "shardId-000000000001",
                 "AdjacentShardToMerge" => "shardId-000000000002"
               })

      :ok = await_stream_active(ctx)

      {:ok, conn, %{"FailedRecordCount" => 0}} =
        Xinesis.api_put_records(conn, %{
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
               Xinesis.api_list_shards(conn, %{"StreamName" => stream_name})

      assert {:ok, conn, %{"ShardIterator" => shard_iterator}} =
               Xinesis.api_get_shard_iterator(conn, %{
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
               Xinesis.api_get_records(conn, %{
                 "Limit" => 10,
                 "ShardIterator" => shard_iterator
               })

      assert {:ok, conn, %{"ShardIterator" => shard_iterator_child_1}} =
               Xinesis.api_get_shard_iterator(conn, %{
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
               Xinesis.api_get_records(conn, %{
                 "Limit" => 10,
                 "ShardIterator" => shard_iterator_child_1
               })

      assert {:ok, conn, %{"ShardIterator" => shard_iterator_child_2}} =
               Xinesis.api_get_shard_iterator(conn, %{
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
               Xinesis.api_get_records(conn, %{
                 "Limit" => 10,
                 "ShardIterator" => shard_iterator_child_2
               })

      assert {:ok, conn, %{"ShardIterator" => shard_iterator_child_3}} =
               Xinesis.api_get_shard_iterator(conn, %{
                 "StreamName" => stream_name,
                 "ShardId" => "shardId-000000000003",
                 "ShardIteratorType" => "TRIM_HORIZON"
               })

      assert {:ok, _conn, %{"NextShardIterator" => _, "Records" => [_ | _]}} =
               Xinesis.api_get_records(conn, %{
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

  defp conn do
    {:ok, conn} =
      Xinesis.connect(%{
        scheme: :http,
        host: "localhost",
        port: 4566,
        region: "us-east-1",
        access_key_id: "test",
        secret_access_key: "test"
      })

    conn
  end

  defp with_conn(f) when is_function(f, 1) do
    conn = conn()

    try do
      f.(conn)
    after
      Mint.HTTP1.close(conn)
    end
  end

  defp create_stream(%{stream_name: stream_name} = ctx, opts \\ []) do
    with_conn(fn conn ->
      {:ok, _, _} =
        Xinesis.api_create_stream(conn, %{
          "StreamName" => stream_name,
          "ShardCount" => opts[:shard_count] || 1
        })
    end)

    on_exit(fn ->
      :ok = await_stream_active(ctx)

      with_conn(fn conn ->
        {:ok, _, _} = Xinesis.api_delete_stream(conn, %{"StreamName" => stream_name})
      end)
    end)
  end

  defp await_stream_active(%{stream_name: stream_name}) do
    :active =
      with_conn(fn conn ->
        delays = List.duplicate(100, 10)

        Enum.reduce_while(delays, conn, fn delay, conn ->
          {:ok, conn, resp} =
            Xinesis.api_describe_stream_summary(conn, %{"StreamName" => stream_name})

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
    with_conn(fn conn ->
      {:ok, _, _} =
        Xinesis.dynamodb_create_table(conn, %{
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
      with_conn(fn conn ->
        {:ok, _, _} = Xinesis.dynamodb_delete_table(conn, %{"TableName" => table_name})
      end)
    end)

    :ok
  end
end
