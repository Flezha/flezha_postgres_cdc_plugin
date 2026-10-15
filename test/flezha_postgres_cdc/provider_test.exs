defmodule FlezhaPostgresCdc.ProviderTest do
  use ExUnit.Case, async: true

  import FlezhaPostgresCdc.Test.Helpers, only: [prepare: 1, prepare: 2]

  alias FlezhaPostgresCdc.Reader

  @base %{
    database: "d",
    username: "u",
    slot: "orders_cdc",
    publication: "orders_pub",
    channel: "in"
  }

  test "a feeding source that is not shared, with its own lag metric and copy event" do
    decl = FlezhaPostgresCdc.__provider__()
    assert %{name: "postgres_cdc", feeds: true, shareable: false} = decl
    assert decl.telemetry == [lag_bytes: :metric, snapshot_done: :event]
    assert %{module: Reader, restart: :temporary} = decl.worker
  end

  test "every name is checked: the slot as Postgres allows it, the rest as identifiers" do
    for {field, value} <- [
          slot: "Orders",
          slot: "orders-cdc",
          publication: "orders pub",
          tables: "public.orders,a.b.c",
          tables: "x;drop"
        ] do
      assert {:error, {:invalid_option_value, ^field, _}} =
               prepare(Map.put(@base, field, value)),
             "#{field}=#{value} was accepted"
    end

    assert {:ok, %{tables: ["public.Orders", "lines"]}} =
             prepare(Map.put(@base, :tables, "public.Orders,lines"))
  end

  test "a member of a partition group and a plugin with no channel are refused" do
    assert {:error, {:partitioned_host, %{provider: "postgres_cdc", relay: "op"}}} =
             prepare(@base, %{partition_count: 4})

    assert {:error, {:entry_channel_required, "postgres_cdc"}} =
             prepare(Map.delete(@base, :channel))
  end

  test "the prepared password is the placeholder; both connections resolve the literal one" do
    {:ok, prepared} = prepare(Map.put(@base, :password, "s3cret"))
    assert prepared.password == "**redacted**"
    refute inspect(prepared) =~ "s3cret"

    args = Reader.args(prepared)
    refute inspect(Map.drop(args, [:conn_opts, :repl_opts])) =~ "s3cret"

    for opts <- [args.conn_opts, args.repl_opts] do
      {configure, rest} = Keyword.pop!(opts, :configure)
      assert configure.(rest)[:password] == "s3cret"
    end
  end

  test "one reader per binding, keyed by the plugin id, with the defaults of the declaration" do
    {:ok, prepared} = prepare(@base)

    assert [{"cdc", %{slot: "orders_cdc", snapshot: "initial", batch_size: 500} = args}] =
             FlezhaPostgresCdc.worker_specs(prepared)

    assert args.commit_interval_ms == 5_000 and args.snapshot_batch == 1_000
    assert args.where == "slot=orders_cdc localhost:5432/d"
  end

  test "the wait before a restart climbs from half a second to half a minute, announced" do
    me = self()
    id = make_ref()
    handler = fn _event, measured, meta, _ -> send(me, {:waited, measured, meta}) end
    :ok = :telemetry.attach(id, [:flezha, :backoff, :wait], handler, nil)
    on_exit(fn -> :telemetry.detach(id) end)

    target = {:postgres_cdc, "cdc-backoff"}

    for {attempt, low, high} <- [{1, 250, 750}, {9, 15_000, 30_000}] do
      wait = Reader.backoff(attempt, "cdc-backoff")
      assert wait in low..high

      assert_receive {:waited, %{delay_ms: ^wait, attempt: ^attempt},
                      %{ladder: :postgres_cdc_reconnect, target: ^target}}
    end
  end
end
