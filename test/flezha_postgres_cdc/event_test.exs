defmodule FlezhaPostgresCdc.EventTest do
  use ExUnit.Case, async: true

  alias FlezhaPostgresCdc.Event

  @target %{plugin_id: "cdc", entry_channel: "in"}
  @table %{schema: "public", table: "rates", key: ["a", "b"]}
  @columns [{"a", :value}, {"b", :value}]

  defp id(key, lsn, table \\ @table) do
    {_payload, metadata} = Event.copied(table, @columns, key, key, 1, lsn, @target)
    metadata["flz.msg_id"]
  end

  test "a copied row's id names the copy's consistent point, so a second copy is new" do
    refute id(["1", "2"], "0/16B3748") == id(["1", "2"], "0/16C0000")
    assert id(["1", "2"], "0/16B3748") =~ ~r|^cdc/snapshot/0/16B3748/|
  end

  test "a key's values and the table's names cannot run into each other" do
    refute id(["a/b", "c"], "0/1") == id(["a", "b/c"], "0/1")

    refute id(["1", "2"], "0/1", %{@table | schema: "a.b", table: "c"}) ==
             id(["1", "2"], "0/1", %{@table | schema: "a", table: "b.c"})
  end
end
