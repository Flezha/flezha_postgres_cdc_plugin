defmodule FlezhaPostgresCdc.ReplicationProofTest do
  @moduledoc """
  The mechanism the plugin is built on, proved on the dev database alone: a
  slot created with an exported snapshot, a second connection reading in that
  snapshot, and the stream starting at the slot's consistent point.
  """
  use ExUnit.Case, async: false

  @moduletag :postgres_cdc_integration

  @conn [hostname: "localhost", port: 5434, database: "flezha_test", username: "flezha"]

  defmodule Repl do
    use Postgrex.ReplicationConnection

    def init(test), do: {:ok, %{test: test}}

    def handle_connect(state),
      do: {:query, "CREATE_REPLICATION_SLOT proof_slot LOGICAL pgoutput EXPORT_SNAPSHOT", state}

    def handle_result([%Postgrex.Result{rows: [[_slot, lsn, snapshot, _plugin]]}], state) do
      send(state.test, {:created, lsn, snapshot})
      {:noreply, state}
    end

    def handle_info(:start, state) do
      query =
        "START_REPLICATION SLOT proof_slot LOGICAL 0/0 " <>
          "(proto_version '1', publication_names '\"proof_pub\"')"

      {:stream, query, [], state}
    end

    def handle_data(<<?w, _start::64, _end::64, _clock::64, data::binary>>, state) do
      send(state.test, {:wal, data})
      {:noreply, state}
    end

    def handle_data(_keepalive, state), do: {:noreply, state}
  end

  setup do
    {:ok, conn} = Postgrex.start_link(@conn ++ [password: "flezha_test"])

    for sql <- [
          "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'proof_slot'",
          "DROP PUBLICATION IF EXISTS proof_pub",
          "DROP TABLE IF EXISTS proof_t",
          "CREATE TABLE proof_t (id int PRIMARY KEY, v text)",
          "INSERT INTO proof_t VALUES (1, 'before')",
          "CREATE PUBLICATION proof_pub FOR TABLE proof_t"
        ],
        do: Postgrex.query!(conn, sql, [])

    on_exit(fn ->
      {:ok, conn} = Postgrex.start_link(@conn ++ [password: "flezha_test"])

      Postgrex.query!(
        conn,
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'proof_slot'",
        []
      )
    end)

    %{conn: conn}
  end

  test "a slot's exported snapshot is adopted elsewhere and the stream starts where it ends",
       %{conn: conn} do
    {:ok, repl} =
      Postgrex.ReplicationConnection.start_link(Repl, self(), @conn ++ [password: "flezha_test"])

    assert_receive {:created, lsn, snapshot}, 10_000
    assert {:ok, _} = Postgrex.ReplicationConnection.decode_lsn(lsn)

    Postgrex.query!(conn, "INSERT INTO proof_t VALUES (2, 'after')", [])

    {:ok, copied} =
      Postgrex.transaction(conn, fn tx ->
        Postgrex.query!(tx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", [])
        Postgrex.query!(tx, "SET TRANSACTION SNAPSHOT '#{snapshot}'", [])
        Postgrex.query!(tx, "SELECT id, v FROM proof_t ORDER BY id", []).rows
      end)

    assert copied == [[1, "before"]]

    send(repl, :start)
    assert_receive {:wal, <<?I, _relid::32, ?N, _tuple::binary>> = insert}, 10_000
    assert insert =~ "after"
    refute insert =~ "before"
    refute_receive {:wal, <<?I, _::binary>>}, 500
  end
end
