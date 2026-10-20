defmodule FlezhaPostgresCdc.ValuesTest do
  @moduledoc """
  A streamed column's text form reaches the same JSON the poll source writes
  for the driver's term: the cases of the poll source's row encoding test,
  given as Postgres writes them in text.
  """
  use ExUnit.Case, async: true

  alias Flezha.Plugin.Provider.Postgres.Connection
  alias FlezhaPostgresCdc.Values

  test "every type of the poll source's encoding, from its text, becomes the same JSON" do
    # {name, kind, type oid, type modifier, text}
    cells = [
      {"i", :value, 23, -1, "42"},
      {"f", :value, 701, -1, "1.5"},
      {"nan", :value, 701, -1, "NaN"},
      {"inf", :value, 701, -1, "-Infinity"},
      {"b", :value, 16, -1, "t"},
      {"n", :text, 1700, -1, "12.3400"},
      {"at", :temporal, 1184, -1, "2026-10-02 12:30:00.123456+00"},
      {"day", :temporal, 1082, -1, "2026-10-02"},
      {"t", :temporal, 1083, -1, "12:30:00"},
      {"local", :temporal, 1114, 0, "2026-10-02 12:30:00"},
      {"u", :text, 2950, -1, "6f1c3a52-6f0a-4f43-9a41-0d8f8c2e7b11"},
      {"j", :value, 3802, -1, ~s({"a": [1, 2]})},
      {"bin", :bytea, 17, -1, "\\x0102ff"},
      {"arr", {:array, :value}, 1007, -1, "{1,NULL,3}"},
      {"grid", {:array, :temporal}, 1182, -1, "{{2026-01-01},{NULL}}"},
      {"bins", {:array, :bytea}, 1001, -1, ~s({"\\\\x00"})}
    ]

    columns = for {name, kind, _, _, _} <- cells, do: {name, kind}

    values =
      for {_, kind, type, typmod, text} <- cells, do: Values.parse(kind, type, typmod, text)

    assert Connection.encode(columns, values) ==
             ~s|{"i":42,"f":1.5,"nan":"NaN","inf":"-Infinity","b":true,"n":"12.3400",| <>
               ~s|"at":"2026-10-02T12:30:00.123456Z","day":"2026-10-02","t":"12:30:00.000000",| <>
               ~s|"local":"2026-10-02T12:30:00","u":"6f1c3a52-6f0a-4f43-9a41-0d8f8c2e7b11",| <>
               ~s|"j":{"a":[1,2]},"bin":"AQL/","arr":[1,null,3],| <>
               ~s|"grid":[["2026-01-01"],[null]],"bins":["AA=="]}|
  end

  test "a timestamp keeps the column's declared precision, as the driver decodes it" do
    assert %NaiveDateTime{microsecond: {120_000, 3}} =
             Values.parse(:temporal, 1114, 3, "2026-10-02 12:30:00.12")

    assert %DateTime{microsecond: {0, 6}, time_zone: "Etc/UTC"} =
             Values.parse(:temporal, 1184, -1, "2026-10-02 14:30:00+02")
  end

  test "array text: quoted elements, escapes, empty arrays and explicit bounds" do
    assert Values.parse({:array, :text}, 1009, -1, ~s({"a,b","say \\"hi\\"",NULL,"NULL",plain})) ==
             ["a,b", ~s(say "hi"), nil, "NULL", "plain"]

    assert Values.parse({:array, :value}, 1007, -1, "{}") == []
    assert Values.parse({:array, :value}, 1007, -1, "[0:1]={7,8}") == [7, 8]
    assert Values.parse({:array, :value}, 1016, -1, "{{1,2},{3,4}}") == [[1, 2], [3, 4]]
  end

  test "a value the parser cannot read, or a kind it does not read, stays text" do
    assert Values.parse(:temporal, 1114, -1, "infinity") == "infinity"
    assert Values.parse(:temporal, 1082, -1, "4713-01-01 BC") == "4713-01-01 BC"
    assert Values.parse(:text, 3614, -1, "'a':1") == "'a':1"
  end
end
