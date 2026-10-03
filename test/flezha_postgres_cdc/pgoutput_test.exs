defmodule FlezhaPostgresCdc.PgoutputTest do
  @moduledoc """
  The decoder against messages recorded from PostgreSQL 16 streaming one
  transaction on `rec_t (id int PRIMARY KEY, status text, notes text)` (default
  identity, `notes` stored out of line) and `rec_f (id int, v text)` (identity
  `FULL`), then a truncate of both.
  """
  use ExUnit.Case, async: true

  alias FlezhaPostgresCdc.Pgoutput

  @begin <<66, 0, 0, 0, 0, 1, 157, 1, 136, 0, 2, 255, 228, 116, 8, 34, 85, 0, 0, 2, 248>>
  @relation <<82, 0, 0, 64, 29, 112, 117, 98, 108, 105, 99, 0, 114, 101, 99, 95, 116, 0, 100, 0,
              3, 1, 105, 100, 0, 0, 0, 0, 23, 255, 255, 255, 255, 0, 115, 116, 97, 116, 117, 115,
              0, 0, 0, 0, 25, 255, 255, 255, 255, 0, 110, 111, 116, 101, 115, 0, 0, 0, 0, 25, 255,
              255, 255, 255>>
  @full <<82, 0, 0, 64, 36, 112, 117, 98, 108, 105, 99, 0, 114, 101, 99, 95, 102, 0, 102, 0, 2, 1,
          105, 100, 0, 0, 0, 0, 23, 255, 255, 255, 255, 1, 118, 0, 0, 0, 0, 25, 255, 255, 255,
          255>>
  @toasted String.duplicate("x", 5000)
  @insert <<73, 0, 0, 64, 29, 78, 0, 3, 116, 0, 0, 0, 1, 49, 116, 0, 0, 0, 3, "new", 116,
            5000::32, @toasted::binary>>
  @update_unchanged <<85, 0, 0, 64, 29, 78, 0, 3, 116, 0, 0, 0, 1, 49, 116, 0, 0, 0, 4, 112, 97,
                      105, 100, 117>>
  @update_key <<85, 0, 0, 64, 29, 75, 0, 3, 116, 0, 0, 0, 1, 49, 110, 110, 78, 0, 3, 116, 0, 0, 0,
                1, 50, 116, 0, 0, 0, 4, 112, 97, 105, 100, 117>>
  @delete_key <<68, 0, 0, 64, 29, 75, 0, 3, 116, 0, 0, 0, 1, 50, 110, 110>>
  @update_old <<85, 0, 0, 64, 36, 79, 0, 2, 116, 0, 0, 0, 1, 55, 110, 78, 0, 2, 116, 0, 0, 0, 1,
                55, 116, 0, 0, 0, 1, 97>>
  @delete_old <<68, 0, 0, 64, 36, 79, 0, 2, 116, 0, 0, 0, 1, 55, 116, 0, 0, 0, 1, 97>>
  @commit <<67, 0, 0, 0, 0, 0, 1, 157, 1, 136, 0, 0, 0, 0, 1, 157, 1, 184, 0, 2, 255, 228, 116, 8,
            34, 85>>
  @truncate <<84, 0, 0, 0, 2, 0, 0, 0, 64, 29, 0, 0, 64, 36>>

  test "a transaction's bounds carry its final LSN, commit time, xid and end LSN" do
    assert {:begin, 0x19D0188, %DateTime{} = at, 760} = Pgoutput.decode(@begin)
    assert DateTime.to_iso8601(at) =~ ~r/^20\d\d-\d\d-\d\dT[\d:.]+Z$/
    assert {:commit, 0x19D01B8} = Pgoutput.decode(@commit)
    assert Pgoutput.lsn(0x19D0188) == "0/19D0188"
  end

  test "a relation names its schema, table and columns, the key columns flagged" do
    assert {:relation, 16_413,
            %{
              schema: "public",
              table: "rec_t",
              columns: [
                %{name: "id", type: 23, typmod: -1, key?: true},
                %{name: "status", type: 25, typmod: -1, key?: false},
                %{name: "notes", type: 25, typmod: -1, key?: false}
              ]
            }} = Pgoutput.decode(@relation)

    assert {:relation, 16_420, %{table: "rec_f", columns: [%{key?: true}, %{key?: true}]}} =
             Pgoutput.decode(@full)
  end

  test "rows carry text, null and unchanged values" do
    assert {:insert, 16_413, [{:text, "1"}, {:text, "new"}, {:text, @toasted}]} =
             Pgoutput.decode(@insert)

    assert {:update, 16_413, nil, [{:text, "1"}, {:text, "paid"}, :unchanged]} =
             Pgoutput.decode(@update_unchanged)

    assert {:update, 16_413, {:key, [{:text, "1"}, nil, nil]},
            [{:text, "2"}, {:text, "paid"}, :unchanged]} = Pgoutput.decode(@update_key)

    assert {:delete, 16_413, {:key, [{:text, "2"}, nil, nil]}} = Pgoutput.decode(@delete_key)

    assert {:update, 16_420, {:old, [{:text, "7"}, nil]}, [{:text, "7"}, {:text, "a"}]} =
             Pgoutput.decode(@update_old)

    assert {:delete, 16_420, {:old, [{:text, "7"}, {:text, "a"}]}} = Pgoutput.decode(@delete_old)
  end

  test "a truncate names every relation it cut; type and origin messages carry nothing" do
    assert {:truncate, [16_413, 16_420]} = Pgoutput.decode(@truncate)
    assert :ignored = Pgoutput.decode(<<?Y, 0, 0, 0, 25, "public", 0, "t", 0>>)
    assert :ignored = Pgoutput.decode(<<?O, 0::64, "origin", 0>>)
  end

  test "a standby status update reports one position written, flushed and applied" do
    assert <<?r, 42::64, 42::64, 42::64, _clock::64, 0>> = Pgoutput.status(42)
  end
end
