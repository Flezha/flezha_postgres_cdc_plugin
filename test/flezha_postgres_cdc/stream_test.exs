defmodule FlezhaPostgresCdc.StreamTest do
  @moduledoc """
  The replication connection's callbacks driven with recorded `pgoutput`
  messages and a stand-in operator: the events, the batches, the confirmed
  position and a refusal. No database is involved.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import FlezhaPostgresCdc.Test.Helpers, only: [stand_in_host: 0, stand_in_host: 1, taken: 0]

  alias FlezhaPostgresCdc.Stream

  @relid 16_413
  @begin <<?w, 0::64, 0::64, 0::64, 66, 0x19D0188::64, 0::64, 760::32>>
  @insert <<?w, 0::64, 0::64, 0::64, 73, @relid::32, ?N, 3::16, ?t, 1::32, "1", ?t, 3::32, "new",
            ?n>>
  @update <<?w, 0::64, 0::64, 0::64, 85, @relid::32, ?N, 3::16, ?t, 1::32, "1", ?t, 4::32, "paid",
            ?u>>
  @delete <<?w, 0::64, 0::64, 0::64, 68, @relid::32, ?K, 3::16, ?t, 1::32, "1", ?n, ?n>>
  @commit <<?w, 0::64, 0::64, 0::64, 67, 0, 0x19D0188::64, 0x19D01B8::64, 0::64>>

  # A stream's state as `init/1` builds it, built in a process of its own:
  # `init/1` marks its process sensitive.
  defp state(host, extra \\ %{}) do
    args =
      Map.merge(
        %{
          reader: self(),
          overwatcher_pid: host,
          plugin_id: "cdc",
          entry_channel: "in",
          slot: "s",
          publication: "p",
          batch_size: 2,
          commit_interval_ms: 60_000,
          durable_input: false,
          conn: nil,
          mode: :stream
        },
        extra
      )

    {:ok, state} = Task.await(Task.async(fn -> Stream.init(args) end))

    columns = [
      %{name: "id", type: 23, typmod: -1, key?: true, kind: :value},
      %{name: "status", type: 25, typmod: -1, key?: false, kind: :value},
      %{name: "notes", type: 25, typmod: -1, key?: false, kind: :value}
    ]

    put_in(state.relations[@relid], %{schema: "public", table: "orders", columns: columns})
  end

  defp feed(state, messages) do
    Enum.flat_map_reduce(messages, state, fn message, state ->
      case Stream.handle_data(message, state) do
        {:noreply, state} -> {[], state}
        {:noreply, acks, state} -> {acks, state}
      end
    end)
  end

  test "a transaction's changes are events of one lsn and xid, handed over in batches" do
    {acks, state} = feed(state(stand_in_host()), [@begin, @insert, @update, @delete, @commit])

    assert [
             {%{"op" => "c", "after" => %{"id" => 1, "status" => "new", "notes" => nil}} = c,
              %{"flz.msg_id" => "cdc/0/19D0188/1", "pg.table" => "public.orders", "pg.op" => "c"}},
             {%{"op" => "u", "before" => nil, "after" => %{"id" => 1, "status" => "paid"}} = u,
              %{"flz.msg_id" => "cdc/0/19D0188/2", "channel" => "in"}},
             {%{"op" => "d", "after" => nil} = d, %{"pg.op" => "d"}}
           ] = taken()

    # A key tuple's other columns are nulls Postgres sends in their place.
    assert d["before"] == %{"id" => 1}

    assert %{"lsn" => "0/19D0188", "xid" => 760, "seq" => 1, "unchanged" => []} = c
    assert %{"seq" => 2, "unchanged" => ["notes"]} = u
    assert c["commit_time"] == "2000-01-01T00:00:00.000000Z"

    # One status after the batch of two, one at the commit; without durable input
    # and inside the interval the confirmed position stays where it was.
    assert [<<?r, 0::64, 0::64, 0::64, _::64, 0>>, <<?r, 0::64, 0::64, 0::64, _::64, 0>>] = acks

    assert state.accepted == 0x19D01B8 and state.committed == 0
    assert_received {:streaming, _, :taken}

    # The replication started: the reader is told once.
    assert {:noreply, state} = Stream.handle_info(:started, state)
    assert {:noreply, _state} = Stream.handle_info(:started, state)
    assert_received {:streaming, _, :failure}
    refute_received {:streaming, _, :failure}
  end

  test "with durable input the position is confirmed at every accepted commit" do
    {acks, state} =
      feed(state(stand_in_host(), %{durable_input: true}), [@begin, @insert, @commit])

    assert [<<?r, 0x19D01B8::64, 0x19D01B8::64, 0x19D01B8::64, _::64, 0>>] = acks
    assert state.committed == 0x19D01B8
  end

  test "a keepalive outside a transaction confirms the server's wal_end; inside one it does not" do
    state = state(stand_in_host())

    assert {:noreply, [<<?r, 0x500::64, _::binary>>], state} =
             Stream.handle_data(<<?k, 0x500::64, 0::64, 0>>, state)

    {_acks, state} = feed(state, [@begin, @insert])

    assert {:noreply, [<<?r, 0x500::64, _::binary>>], %{committed: 0x500}} =
             Stream.handle_data(<<?k, 0x900::64, 0::64, 1>>, state)
  end

  test "a refused change ends the connection, to stream again from the confirmed position" do
    host = stand_in_host(fn _body, _md -> {:error, :queue_full} end)
    state = state(host, %{batch_size: 10})
    {_acks, state} = feed(state, [@begin, @insert])

    # The refusal's warning reaches the capture: the suite logs errors only.
    Logger.configure(level: :warning)
    on_exit(fn -> Logger.configure(level: :error) end)

    log =
      capture_log(fn ->
        assert catch_exit(Stream.handle_data(@commit, state)) ==
                 {:shutdown, {:ingest_refused, :queue_full}}
      end)

    assert log =~ "slot=s lsn=0/19D0188 changes=1 ingest refused: :queue_full"
    assert taken() == []
    refute_received {:streaming, _, :taken}
  end
end
