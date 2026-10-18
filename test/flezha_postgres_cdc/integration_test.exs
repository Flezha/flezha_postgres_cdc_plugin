defmodule FlezhaPostgresCdc.IntegrationTest do
  @moduledoc """
  The provider against the dev stack's `postgres-cdc` (logical replication on,
  host port 5434), with a stand-in relay: the copy, the stream, the event,
  the position, the checks, the lag and the slot's life.

      docker compose -f ../../docker/dev/docker-compose.yml up -d postgres-cdc
      POSTGRES_CDC_INTEGRATION=1 mix test --include postgres_cdc_integration
  """
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog
  import FlezhaPostgresCdc.Test.Helpers
  import FlezhaTest.Helper.NodeTestHelper, only: [ensure_stopped: 2, wait_until: 2]
  import FlezhaTest.Helper.Telemetry

  alias FlezhaPostgresCdc.Catalog

  @moduletag :postgres_cdc_integration
  @moduletag timeout: 120_000

  @copied [:flezha, :plugin, :postgres_cdc, :snapshot_done]

  setup do
    capture([@copied])
    conn = connect()
    n = System.unique_integer([:positive])
    names = %{slot: "cdc_slot_#{n}", publication: "cdc_pub_#{n}", table: "cdc_t_#{n}"}

    on_exit(fn ->
      {:ok, conn} = Postgrex.start_link(Map.to_list(database()))
      # A slot is held until the backend of the connection that used it ends.
      wait_until(fn -> drop_slot(conn, names.slot) end, 10_000)

      sql!(conn, [
        "DROP PUBLICATION IF EXISTS #{names.publication}",
        "DROP TABLE IF EXISTS #{names.table}"
      ])
    end)

    Map.put(names, :conn, conn)
  end

  defp drop_slot(conn, slot) do
    match?(
      {:ok, _},
      Postgrex.query(
        conn,
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
        [slot]
      )
    )
  end

  defp opts(ctx, extra \\ %{}) do
    Map.merge(database(), %{
      slot: ctx.slot,
      publication: ctx.publication,
      channel: "in",
      lag_check_ms: 100
    })
    |> Map.merge(extra)
  end

  defp table!(ctx, columns, identity \\ nil) do
    sql!(ctx.conn, [
      "CREATE TABLE #{ctx.table} (#{columns})",
      "CREATE PUBLICATION #{ctx.publication} FOR TABLE #{ctx.table}"
    ])

    if identity, do: sql!(ctx.conn, "ALTER TABLE #{ctx.table} REPLICA IDENTITY #{identity}")
  end

  # The plugin streaming: its slot held by a live replication connection.
  defp streaming?(ctx) do
    wait_until(
      fn ->
        match?(
          %{rows: [[true]]},
          Postgrex.query!(
            ctx.conn,
            "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
            [
              ctx.slot
            ]
          )
        )
      end,
      15_000
    )
  end

  defp health(prepared, pid), do: FlezhaPostgresCdc.health(Map.put(prepared, :owned_pid, pid))

  defp warnings(fun) do
    Logger.configure(level: :warning)
    capture_log(fun)
  after
    Logger.configure(level: :error)
  end

  describe "the copy and the stream" do
    test "writes during the copy: applied in order, the events give the table's final contents",
         ctx do
      table!(ctx, "id int PRIMARY KEY, v text")

      sql!(
        ctx.conn,
        "INSERT INTO #{ctx.table} SELECT g, 'v' || g FROM generate_series(1, 50000) g"
      )

      writer = Task.async(fn -> write_while_copying(ctx.table, 400) end)
      host = stand_in_host()
      {_prepared, _pid} = activate(opts(ctx, %{snapshot_batch: 2_000}), %{overwatcher_pid: host})

      assert_receive {:telemetry, @copied, _, _}, 60_000
      assert Task.await(writer, 60_000) > 0

      final =
        Map.new(Postgrex.query!(ctx.conn, "SELECT id, v FROM #{ctx.table}", []).rows, fn [id, v] ->
          {id, v}
        end)

      assert wait_until(fn -> applied() == final end, 30_000)

      # Applied in order is not enough: no event may have come twice either.
      ids = Process.get(:ids)
      assert length(ids) == length(Enum.uniq(ids))
    end

    test "insert, update and delete in one transaction under FULL identity", ctx do
      table!(ctx, "id int, status text", "FULL")

      {_prepared, _pid} =
        activate(opts(ctx, %{snapshot: "never"}), %{overwatcher_pid: stand_in_host()})

      assert streaming?(ctx)

      Postgrex.transaction(ctx.conn, fn tx ->
        sql!(tx, [
          "INSERT INTO #{ctx.table} VALUES (1, 'new')",
          "UPDATE #{ctx.table} SET status = 'paid' WHERE id = 1",
          "DELETE FROM #{ctx.table} WHERE id = 1"
        ])
      end)

      assert [{c, cmd}, {u, umd}, {d, _}] = taken(1_000)
      assert {c["op"], u["op"], d["op"]} == {"c", "u", "d"}
      assert [1, 2, 3] == Enum.map([c, u, d], & &1["seq"])
      assert c["lsn"] == u["lsn"] and u["lsn"] == d["lsn"] and c["xid"] == d["xid"]
      assert u["before"] == %{"id" => 1, "status" => "new"} and u["after"]["status"] == "paid"
      assert d["before"] == %{"id" => 1, "status" => "paid"}
      assert cmd["flz.msg_id"] == "cdc/#{c["lsn"]}/1" and umd["pg.table"] == "public.#{ctx.table}"
    end

    test "default identity: the key alone before a delete, nothing before an update, TOAST left out",
         ctx do
      table!(ctx, "id int PRIMARY KEY, status text, notes text")
      sql!(ctx.conn, "ALTER TABLE #{ctx.table} ALTER COLUMN notes SET STORAGE EXTERNAL")

      {_prepared, _pid} =
        activate(opts(ctx, %{snapshot: "never"}), %{overwatcher_pid: stand_in_host()})

      assert streaming?(ctx)

      big = String.duplicate("x", 5_000)
      Postgrex.query!(ctx.conn, "INSERT INTO #{ctx.table} VALUES (1, 'new', $1)", [big])

      sql!(ctx.conn, [
        "UPDATE #{ctx.table} SET status = 'paid' WHERE id = 1",
        "DELETE FROM #{ctx.table}"
      ])

      sql!(ctx.conn, "TRUNCATE #{ctx.table}")

      assert [{c, _}, {u, _}, {d, _}, {t, tmd}] = taken(1_000)
      assert c["after"]["notes"] == big
      assert u["before"] == nil and u["unchanged"] == ["notes"]
      assert u["after"] == %{"id" => 1, "status" => "paid"}
      assert d["before"] == %{"id" => 1}
      assert t["op"] == "t" and t["tables"] == ["public.#{ctx.table}"] and tmd["pg.op"] == "t"
    end

    test "a copied row and the same row streamed give the same JSON, arrays included", ctx do
      table!(
        ctx,
        "id int PRIMARY KEY, i8 bigint, f float8, n numeric(8,3), b bool, s text, " <>
          "at timestamptz, ts timestamp(3), d date, tm time, j jsonb, bin bytea, " <>
          "ints int[], texts text[], u uuid, days date[]"
      )

      row =
        "'9000000000', 1.5, 12.34, true, 'é', '2026-10-02 12:30:00.123456+02', " <>
          "'2026-10-02 12:30:00.12', '2026-10-02', '12:30:00', '{\"a\": [1, 2]}', '\\x0102ff', " <>
          "'{1,NULL,3}', '{\"a,b\",\"say \\\"hi\\\"\",NULL}', " <>
          "'6f1c3a52-6f0a-4f43-9a41-0d8f8c2e7b11', '{2026-01-01,NULL}'"

      sql!(ctx.conn, "INSERT INTO #{ctx.table} VALUES (1, #{row})")
      {_prepared, _pid} = activate(opts(ctx), %{overwatcher_pid: stand_in_host()})
      assert_receive {:telemetry, @copied, _, _}, 10_000
      assert streaming?(ctx)
      sql!(ctx.conn, "INSERT INTO #{ctx.table} VALUES (2, #{row})")

      assert [{copied, cmd}, {streamed, _}] = taken(1_000)
      assert copied["op"] == "r"

      assert cmd["flz.msg_id"] ==
               "cdc/snapshot/#{copied["lsn"]}/" <> Jason.encode!(["public", ctx.table, "1"])

      assert Map.delete(copied["after"], "id") == Map.delete(streamed["after"], "id")
      assert copied["after"]["texts"] == ["a,b", ~s(say "hi"), nil]
    end
  end

  describe "the position" do
    test "a refused change stops the stream, which streams it again from the confirmed position",
         ctx do
      table!(ctx, "id int PRIMARY KEY")
      test = self()

      # The host refuses its first input, and takes every one after it.
      host =
        stand_in_host(fn _body, md ->
          case Process.put(:refused, true) do
            true ->
              :ok

            nil ->
              send(test, {:refused, md})
              {:error, :queue_full}
          end
        end)

      log =
        warnings(fn ->
          {_prepared, _pid} = activate(opts(ctx, %{snapshot: "never"}), %{overwatcher_pid: host})
          assert streaming?(ctx)
          sql!(ctx.conn, "INSERT INTO #{ctx.table} VALUES (1)")
          assert_receive {:refused, %{"flz.msg_id" => id}}, 5_000
          assert_receive {:ingest, _body, %{"flz.msg_id" => ^id}}, 10_000
        end)

      assert log =~
               "ingest refused: :queue_full; streaming again from the last confirmed position"
    end

    test "unbound and bound again: the slot is kept, nothing is copied, the stream continues",
         ctx do
      table!(ctx, "id int PRIMARY KEY")
      sql!(ctx.conn, "INSERT INTO #{ctx.table} SELECT generate_series(1, 3)")
      host = stand_in_host()
      binding = opts(ctx)
      {_prepared, pid} = activate(binding, %{overwatcher_pid: host, durable_input: true})

      assert_receive {:telemetry, @copied, _, _}, 10_000
      assert streaming?(ctx)
      sql!(ctx.conn, "INSERT INTO #{ctx.table} VALUES (4)")
      assert [1, 2, 3, 4] == Enum.map(taken(1_000), &elem(&1, 0)["after"]["id"])

      ensure_stopped(pid, :shutdown)
      assert [[false]] = Postgrex.query!(ctx.conn, active_sql(), [ctx.slot]).rows
      sql!(ctx.conn, "INSERT INTO #{ctx.table} VALUES (5)")

      # The binding comes back as a new one: nothing it committed survived.
      {_prepared, _pid} = activate(binding, %{overwatcher_pid: host, durable_input: true})

      assert [{%{"op" => "c", "after" => %{"id" => 5}}, _}] = taken(2_000)
    end
  end

  defp active_sql, do: "SELECT active FROM pg_replication_slots WHERE slot_name = $1"

  # The slots of this test: name and whether temporary.
  defp slots(ctx) do
    Postgrex.query!(
      ctx.conn,
      "SELECT slot_name, temporary FROM pg_replication_slots WHERE slot_name IN ($1, $2) ORDER BY 1",
      [ctx.slot, ctx.slot <> "_copy"]
    ).rows
  end

  describe "a copy and its slot" do
    test "the slot is permanent only once the copy ended; until then a temporary one holds it",
         ctx do
      table!(ctx, "id int PRIMARY KEY")
      sql!(ctx.conn, "INSERT INTO #{ctx.table} SELECT generate_series(1, 10)")

      host =
        stand_in_host(fn _body, _md ->
          if Process.put(:held, true) == nil, do: receive(do: (:release -> :ok)), else: :ok
        end)

      {_prepared, _pid} = activate(opts(ctx), %{overwatcher_pid: host})
      assert wait_until(fn -> slots(ctx) != [] end, 10_000)
      assert slots(ctx) == [[ctx.slot <> "_copy", true]]

      send(host, :release)
      assert_receive {:telemetry, @copied, %{count: 10}, _}, 10_000
      assert wait_until(fn -> slots(ctx) == [[ctx.slot, false]] end, 10_000)
    end

    test "a crash during the copy leaves no slot, and the next start copies every row again",
         ctx do
      table!(ctx, "id int PRIMARY KEY, v text")
      sql!(ctx.conn, "INSERT INTO #{ctx.table} SELECT g, 'v' FROM generate_series(1, 3000) g")
      binding = opts(ctx, %{snapshot_batch: 100})
      {_prepared, pid} = activate(binding, %{overwatcher_pid: stand_in_host()})

      assert_receive {:ingest, _body, _md}, 10_000

      # The reader's connection pool dies with it, and says so.
      log =
        capture_log(fn ->
          Process.exit(pid, :kill)
          assert wait_until(fn -> slots(ctx) == [] end, 10_000)
          Process.sleep(200)
        end)

      assert log =~ "terminating" and log =~ "killed"
      _ = taken(500)

      {_prepared, _pid} = activate(binding, %{overwatcher_pid: stand_in_host()})
      assert_receive {:telemetry, @copied, %{count: 3000}, _}, 30_000
      assert length(taken(500)) == 3000
    end

    test "a second copy under durable input delivers every row at its value then", ctx do
      table!(ctx, "id int PRIMARY KEY, v text")
      sql!(ctx.conn, "INSERT INTO #{ctx.table} VALUES (1, 'v1')")
      host = stand_in_host(fn _body, _md -> :ok end, absorb: true)
      binding = opts(ctx)
      {_prepared, pid} = activate(binding, %{overwatcher_pid: host, durable_input: true})

      assert_receive {:telemetry, @copied, _, _}, 10_000
      assert [{%{"after" => %{"v" => "v1"}}, _}] = taken(500)
      ensure_stopped(pid, :shutdown)

      # The slot is dropped by hand while the row changes: the next start copies again.
      sql!(ctx.conn, "UPDATE #{ctx.table} SET v = 'v2'")
      assert wait_until(fn -> drop_slot(ctx.conn, ctx.slot) and slots(ctx) == [] end, 10_000)
      {_prepared, _pid} = activate(binding, %{overwatcher_pid: host, durable_input: true})

      assert_receive {:telemetry, @copied, _, _}, 10_000
      assert [{%{"op" => "r", "after" => %{"v" => "v2"}}, _}] = taken(500)
    end

    test "a start while the slot's last connection still holds it waits its turn", ctx do
      table!(ctx, "id int PRIMARY KEY")
      sql!(ctx.conn, "SELECT pg_create_logical_replication_slot('#{ctx.slot}', 'pgoutput')")
      {:ok, holder} = hold_slot(ctx)
      assert streaming?(ctx)

      log =
        warnings(fn ->
          {prepared, pid} =
            activate(opts(ctx, %{snapshot: "never"}), %{overwatcher_pid: stand_in_host()})

          assert wait_until(
                   fn ->
                     match?(
                       {:unhealthy, {:postgres_cdc_replication_failed, _}},
                       health(prepared, pid)
                     )
                   end,
                   10_000
                 )

          Process.unlink(holder)
          Process.exit(holder, :kill)
          sql!(ctx.conn, "INSERT INTO #{ctx.table} VALUES (1)")
          assert_receive {:ingest, _body, %{"pg.op" => "c"}}, 30_000
        end)

      assert log =~ "object_in_use"
    end
  end

  defmodule Holder do
    @moduledoc false
    use Postgrex.ReplicationConnection

    def init({slot, pub}), do: {:ok, %{slot: slot, pub: pub}}

    def handle_connect(s),
      do:
        {:stream,
         "START_REPLICATION SLOT #{s.slot} LOGICAL 0/0 (proto_version '1', publication_names '#{s.pub}')",
         [], s}

    def handle_data(_data, s), do: {:noreply, s}
  end

  defp hold_slot(ctx),
    do:
      Postgrex.ReplicationConnection.start_link(
        Holder,
        {ctx.slot, ctx.publication},
        Map.to_list(database())
      )

  describe "the checks" do
    test "a missing publication: unhealthy without tables, created from them", ctx do
      sql!(ctx.conn, "CREATE TABLE #{ctx.table} (id int PRIMARY KEY)")

      log =
        warnings(fn ->
          {prepared, pid} = activate(opts(ctx), %{overwatcher_pid: stand_in_host()})

          assert wait_until(fn -> match?({:unhealthy, _}, health(prepared, pid)) end, 5_000)

          assert {:unhealthy, {:postgres_cdc_publication_missing, %{publication: pub}}} =
                   health(prepared, pid)

          assert pub == ctx.publication
          assert [] = Postgrex.query!(ctx.conn, active_sql(), [ctx.slot]).rows
          ensure_stopped(pid, :shutdown)
        end)

      assert log =~ "postgres_cdc_publication_missing"

      {prepared, pid} =
        activate(opts(ctx, %{tables: "public.#{ctx.table}"}), %{overwatcher_pid: stand_in_host()})

      assert streaming?(ctx)
      assert health(prepared, pid) == :healthy
    end

    test "a table with no replica identity is refused, and no slot is created", ctx do
      table!(ctx, "id int, v text")

      log =
        warnings(fn ->
          {prepared, pid} = activate(opts(ctx), %{overwatcher_pid: stand_in_host()})

          assert wait_until(
                   fn ->
                     health(prepared, pid) ==
                       {:unhealthy,
                        {:postgres_cdc_replica_identity_missing, %{table: "public.#{ctx.table}"}}}
                   end,
                   5_000
                 )
        end)

      assert log =~ "postgres_cdc_replica_identity_missing"
      assert [] = Postgrex.query!(ctx.conn, active_sql(), [ctx.slot]).rows
    end

    test "tables with no replica identity: no publication is created from them either", ctx do
      sql!(ctx.conn, "CREATE TABLE #{ctx.table} (id int, v text)")

      warnings(fn ->
        {prepared, pid} =
          activate(opts(ctx, %{tables: ctx.table}), %{overwatcher_pid: stand_in_host()})

        assert wait_until(
                 fn ->
                   match?(
                     {:unhealthy, {:postgres_cdc_replica_identity_missing, _}},
                     health(prepared, pid)
                   )
                 end,
                 5_000
               )
      end)

      assert [] =
               Postgrex.query!(ctx.conn, "SELECT 1 FROM pg_publication WHERE pubname = $1", [
                 ctx.publication
               ]).rows

      assert [] = Postgrex.query!(ctx.conn, active_sql(), [ctx.slot]).rows
    end

    test "an identity index that was dropped leaves no usable identity", ctx do
      table!(ctx, "id int NOT NULL, v text")

      sql!(ctx.conn, [
        "CREATE UNIQUE INDEX #{ctx.table}_id ON #{ctx.table} (id)",
        "ALTER TABLE #{ctx.table} REPLICA IDENTITY USING INDEX #{ctx.table}_id",
        "DROP INDEX #{ctx.table}_id"
      ])

      warnings(fn ->
        {prepared, pid} = activate(opts(ctx), %{overwatcher_pid: stand_in_host()})

        assert wait_until(
                 fn ->
                   match?(
                     {:unhealthy, {:postgres_cdc_replica_identity_missing, _}},
                     health(prepared, pid)
                   )
                 end,
                 5_000
               )
      end)

      assert slots(ctx) == []
    end

    test "logical replication and the server's version are checked before anything else", ctx do
      spec = %{publication: ctx.publication, tables: nil, slot: ctx.slot}

      for {settings, failure} <- [
            {["replica", 160_015, "16.15"],
             {:postgres_cdc_wal_level_not_logical, %{wal_level: "replica"}}},
            {["logical", 130_004, "13.4"], {:postgres_cdc_server_too_old, %{version: "13.4"}}}
          ] do
        stub(Postgrex, :query, fn conn, sql, params ->
          if sql =~ "wal_level",
            do: {:ok, %Postgrex.Result{rows: [settings]}},
            else: call_original(Postgrex, :query, [conn, sql, params])
        end)

        assert {:error, ^failure} = Catalog.check(ctx.conn, spec)
      end
    end
  end

  describe "the slot's lag and the password" do
    test "a stalled relay: unhealthy past max_lag_bytes, healthy once it catches up", ctx do
      table!(ctx, "id int PRIMARY KEY, v text")
      stalled = :atomics.new(1, [])
      :atomics.put(stalled, 1, 1)

      host =
        stand_in_host(fn _body, _md ->
          if :atomics.get(stalled, 1) == 1, do: {:error, :stalled}, else: :ok
        end)

      warnings(fn ->
        {prepared, pid} =
          activate(opts(ctx, %{snapshot: "never", max_lag_bytes: 100_000}), %{
            overwatcher_pid: host,
            durable_input: true
          })

        assert streaming?(ctx)

        sql!(
          ctx.conn,
          "INSERT INTO #{ctx.table} SELECT g, repeat('x', 200) FROM generate_series(1, 2000) g"
        )

        assert wait_until(
                 fn ->
                   match?({:unhealthy, {:postgres_cdc_slot_lag, _}}, health(prepared, pid))
                 end,
                 10_000
               )

        :atomics.put(stalled, 1, 0)
        assert wait_until(fn -> health(prepared, pid) == :healthy end, 30_000)
      end)
    end

    test "no password in a log line, the plugin's state or the replication connection", ctx do
      table!(ctx, "id int PRIMARY KEY")
      secret = "S3cret-#{System.unique_integer([:positive])}"
      role = "cdc_role_#{System.unique_integer([:positive])}"

      sql!(ctx.conn, [
        "CREATE ROLE #{role} LOGIN REPLICATION PASSWORD '#{secret}'",
        "GRANT SELECT ON #{ctx.table} TO #{role}"
      ])

      on_exit(fn ->
        {:ok, conn} = Postgrex.start_link(Map.to_list(database()))
        sql!(conn, ["DROP OWNED BY #{role}", "DROP ROLE IF EXISTS #{role}"])
      end)

      log =
        warnings(fn ->
          {prepared, pid} =
            activate(opts(ctx, %{username: role, password: secret, snapshot: "never"}), %{
              overwatcher_pid: stand_in_host()
            })

          assert streaming?(ctx)
          stream = :sys.get_state(pid).stream
          assert {:dictionary, []} = Process.info(stream, :dictionary)

          refute inspect(FlezhaPostgresCdc.inspect_state(Map.put(prepared, :owned_pid, pid))) =~
                   secret

          refute inspect(:sys.get_state(pid)) =~ secret

          # A lost replication connection is logged, and names no password.
          sql!(
            ctx.conn,
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '#{ctx.slot}'"
          )

          assert wait_until(
                   fn ->
                     match?(
                       {:unhealthy, {:postgres_cdc_replication_failed, _}},
                       health(prepared, pid)
                     )
                   end,
                   5_000
                 )

          assert streaming?(ctx)
        end)

      assert log =~ "stream failed"
      refute log =~ secret
    end
  end

  # ---------- the copy under writes ----------

  # Inserts, updates and deletes for `ms`, one transaction each; answers how many.
  defp write_while_copying(table, ms) do
    {:ok, conn} = Postgrex.start_link(Map.to_list(database()))
    deadline = System.monotonic_time(:millisecond) + ms
    count = write(conn, table, deadline, 0)
    GenServer.stop(conn)
    count
  end

  defp write(conn, table, deadline, n) do
    if System.monotonic_time(:millisecond) >= deadline do
      n
    else
      id = :rand.uniform(50_000)

      sql =
        case rem(n, 3) do
          0 -> "INSERT INTO #{table} VALUES (#{50_000 + n + 1}, 'new')"
          1 -> "UPDATE #{table} SET v = 'changed #{n}' WHERE id = #{id}"
          2 -> "DELETE FROM #{table} WHERE id = #{id}"
        end

      Postgrex.query!(conn, sql, [])
      write(conn, table, deadline, n + 1)
    end
  end

  # The table as the events that arrived so far build it, applied in order.
  defp applied() do
    events = taken(200)
    Process.put(:ids, Process.get(:ids, []) ++ Enum.map(events, &elem(&1, 1)["flz.msg_id"]))
    model = Enum.reduce(events, Process.get(:model, %{}), &apply_event/2)
    Process.put(:model, model)
    model
  end

  defp apply_event({%{"op" => op, "after" => row}, _md}, model) when op in ["r", "c", "u"],
    do: Map.put(model, row["id"], row["v"])

  defp apply_event({%{"op" => "d", "before" => before}, _md}, model),
    do: Map.delete(model, before["id"])
end
