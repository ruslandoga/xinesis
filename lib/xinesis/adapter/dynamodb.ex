defmodule Xinesis.Adapter.DynamoDB do
  # TODO

  # def create_lease(conn, table, shard, owner, opts \\ []) do
  #   payload = %{
  #     "TableName" => table,
  #     "Item" => %{
  #       "shard_id" => %{"S" => shard},
  #       "lease_owner" => %{"S" => owner},
  #       "lease_count" => %{"N" => "1"},
  #       "completed" => %{"BOOL" => false}
  #     },
  #     "ConditionExpression" => "attribute_not_exists(shard_id)"
  #   }

  #   dynamodb_put_item(conn, payload, opts)
  # end

  # def get_lease(conn, table, shard, opts \\ []) do
  #   payload = %{
  #     "TableName" => table,
  #     "Key" => %{"shard_id" => %{"S" => shard}}
  #   }

  #   dynamodb_get_item(conn, payload, opts)
  # end

  # def renew_lease(conn, table, shard, owner, count, opts \\ []) do
  #   payload = %{
  #     "TableName" => table,
  #     "Key" => %{"shard_id" => %{"S" => shard}},
  #     "UpdateExpression" => "SET lease_count = lease_count + :val",
  #     "ConditionExpression" => "lease_owner = :owner AND lease_count = :count",
  #     "ExpressionAttributeValues" => %{
  #       ":val" => %{"N" => "1"},
  #       ":owner" => %{"S" => owner},
  #       ":count" => %{"N" => Integer.to_string(count)}
  #     },
  #     "ReturnValues" => "UPDATED_NEW"
  #   }

  #   dynamodb_update_item(conn, payload, opts)
  # end

  # def take_lease(conn, table, shard, new_owner, count, opts \\ []) do
  #   payload = %{
  #     "TableName" => table,
  #     "Key" => %{"shard_id" => %{"S" => shard}},
  #     "UpdateExpression" => "SET lease_owner = :new_owner, lease_count = lease_count + :val",
  #     "ConditionExpression" => "lease_count = :count AND lease_owner <> :new_owner",
  #     "ExpressionAttributeValues" => %{
  #       ":new_owner" => %{"S" => new_owner},
  #       ":val" => %{"N" => "1"},
  #       ":count" => %{"N" => Integer.to_string(count)}
  #     },
  #     "ReturnValues" => "UPDATED_NEW"
  #   }

  #   dynamodb_update_item(conn, payload, opts)
  # end

  # def update_checkpoint(conn, table, shard, owner, checkpoint, opts \\ []) do
  #   payload = %{
  #     "TableName" => table,
  #     "Key" => %{"shard_id" => %{"S" => shard}},
  #     "UpdateExpression" => "SET checkpoint = :checkpoint",
  #     "ConditionExpression" => "lease_owner = :owner",
  #     "ExpressionAttributeValues" => %{
  #       ":checkpoint" => %{"S" => checkpoint},
  #       ":owner" => %{"S" => owner}
  #     },
  #     "ReturnValues" => "UPDATED_NEW"
  #   }

  #   dynamodb_update_item(conn, payload, opts)
  # end

  # def mark_shard_completed(conn, table, shard, owner, opts \\ []) do
  #   payload = %{
  #     "TableName" => table,
  #     "Key" => %{"shard_id" => %{"S" => shard}},
  #     "UpdateExpression" => "SET completed = :completed",
  #     "ConditionExpression" => "lease_owner = :owner",
  #     "ExpressionAttributeValues" => %{
  #       ":completed" => %{"BOOL" => true},
  #       ":owner" => %{"S" => owner}
  #     },
  #     "ReturnValues" => "UPDATED_NEW"
  #   }

  #   dynamodb_update_item(conn, payload, opts)
  # end
end
