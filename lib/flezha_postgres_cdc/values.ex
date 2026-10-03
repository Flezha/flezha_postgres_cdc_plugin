defmodule FlezhaPostgresCdc.Values do
  @moduledoc """
  A streamed column's text form parsed into the term the driver decodes for
  the same column, so `Flezha.Plugin.Provider.PostgresPoll.Query.encode/2`
  writes a streamed row exactly as it writes the copied one.

  The column's kind is the catalog's, as the poll source reads it: `:value`
  types are told apart by their built-in type oid, `:temporal` ones keep the
  column's declared precision as the driver does, `:bytea` is the server's hex
  form, and an array is Postgres's array text, element by element. A value
  this module cannot parse, or a kind it does not read (`:text`), stays its
  text.
  """

  # The built-in `:value` types and their arrays, by type oid.
  @bools [16, 1000]
  @ints [20, 21, 23, 1005, 1007, 1016]
  @floats [700, 701, 1021, 1022]
  @jsons [114, 199, 3802, 3807]
  @dates [1082, 1182]
  @times [1083, 1183]
  @timestamps [1114, 1115]
  @timestamptzs [1184, 1185]

  # The driver's precision for a column declared without one.
  @default_precision 6

  @doc "The term `text` of a column of `kind`, type oid `type` and modifier `typmod` decodes to."
  @spec parse(term(), non_neg_integer(), integer(), binary()) :: term()
  def parse({:array, kind}, type, typmod, text), do: array(text, &parse(kind, type, typmod, &1))
  def parse(:value, type, _typmod, text), do: value(type, text)
  def parse(:temporal, type, typmod, text), do: temporal(type, precision(typmod), text)

  def parse(:bytea, _type, _typmod, "\\x" <> hex = text),
    do: decoded(Base.decode16(hex, case: :mixed), text)

  def parse(_kind, _type, _typmod, text), do: text

  defp value(type, text) when type in @bools, do: text == "t"
  defp value(type, text) when type in @ints, do: String.to_integer(text)
  defp value(type, text) when type in @floats, do: float(text)
  defp value(type, text) when type in @jsons, do: decoded(Jason.decode(text), text)
  defp value(_type, text), do: text

  defp float("NaN"), do: :NaN
  defp float("Infinity"), do: :inf
  defp float("-Infinity"), do: :"-inf"

  defp float(text) do
    case Float.parse(text) do
      {float, ""} -> float
      _other -> text
    end
  end

  defp temporal(type, _precision, text) when type in @dates,
    do: decoded(Date.from_iso8601(text), text)

  defp temporal(type, precision, text) when type in @times,
    do: text |> Time.from_iso8601() |> decoded(text) |> precise(precision)

  defp temporal(type, precision, text) when type in @timestamps,
    do: text |> NaiveDateTime.from_iso8601() |> decoded(text) |> precise(precision)

  defp temporal(type, precision, text) when type in @timestamptzs do
    case DateTime.from_iso8601(text) do
      {:ok, at, _offset} -> precise(at, precision)
      {:error, _} -> text
    end
  end

  defp temporal(_type, _precision, text), do: text

  defp precision(typmod) when typmod < 0, do: @default_precision
  defp precision(typmod), do: typmod

  defp precise(%{microsecond: {us, _}} = at, precision), do: %{at | microsecond: {us, precision}}
  defp precise(text, _precision), do: text

  defp decoded({:ok, value}, _text), do: value
  defp decoded(_error, text), do: text

  # ---------- array text ----------

  @doc """
  Postgres's array text as nested lists, each element parsed by `element`:
  `{1,2}`, `{{1,2},{3,4}}`, `{"a b",NULL}`, `[0:1]={1,2}`.
  """
  @spec array(binary(), (binary() -> term())) :: list() | binary()
  def array("[" <> _ = text, element) do
    [_bounds, rest] = :binary.split(text, "=")
    array(rest, element)
  end

  def array("{" <> rest, element) do
    case items(rest, element, []) do
      {items, ""} -> items
      _other -> "{" <> rest
    end
  end

  defp items("}" <> rest, _element, []), do: {[], rest}

  defp items(text, element, acc) do
    {item, rest} = item(text, element)

    case rest do
      "," <> rest -> items(rest, element, [item | acc])
      "}" <> rest -> {Enum.reverse([item | acc]), rest}
    end
  end

  defp item("{" <> rest, element), do: items(rest, element, [])
  defp item(~s(") <> rest, element), do: quoted(rest, element, [])

  defp item(text, element) do
    size = bare_length(text, 0)
    <<bare::binary-size(^size), rest::binary>> = text
    {if(bare == "NULL", do: nil, else: element.(bare)), rest}
  end

  defp bare_length(<<c, _::binary>>, n) when c in [?,, ?}], do: n
  defp bare_length(<<_c, rest::binary>>, n), do: bare_length(rest, n + 1)

  defp quoted(~s(\\) <> <<c::utf8, rest::binary>>, element, acc),
    do: quoted(rest, element, [<<c::utf8>> | acc])

  defp quoted(~s(") <> rest, element, acc),
    do: {element.(acc |> Enum.reverse() |> IO.iodata_to_binary()), rest}

  defp quoted(<<c::utf8, rest::binary>>, element, acc),
    do: quoted(rest, element, [<<c::utf8>> | acc])
end
