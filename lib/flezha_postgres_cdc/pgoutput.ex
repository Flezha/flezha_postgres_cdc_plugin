defmodule FlezhaPostgresCdc.Pgoutput do
  @moduledoc """
  Postgres's logical replication protocol as the stream reads and writes it:
  the `pgoutput` messages (protocol version 1) inside each `XLogData`, the
  server's keepalive, and the standby status update the stream answers with.

  A row arrives as a tuple of column values, each `{:text, binary}` (the
  column's text form), `nil` (null) or `:unchanged` (a TOAST value the change
  left alone, which Postgres does not send again).
  """

  @typedoc "One column value of a row tuple."
  @type value :: {:text, binary()} | nil | :unchanged

  @typedoc "A relation's column: its name, type oid, type modifier and whether it is in the key."
  @type column :: %{name: binary(), type: non_neg_integer(), typmod: integer(), key?: boolean()}

  @type message ::
          {:begin, lsn :: non_neg_integer(), DateTime.t(), xid :: non_neg_integer()}
          | {:commit, end_lsn :: non_neg_integer()}
          | {:relation, non_neg_integer(),
             %{schema: binary(), table: binary(), columns: [column()]}}
          | {:insert, non_neg_integer(), [value()]}
          | {:update, non_neg_integer(), {:key | :old, [value()]} | nil, [value()]}
          | {:delete, non_neg_integer(), {:key | :old, [value()]}}
          | {:truncate, [non_neg_integer()]}
          | :ignored

  # Postgres counts its timestamps in microseconds from 2000-01-01.
  @epoch_us DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)

  @doc "One `pgoutput` message, from the payload of an `XLogData`."
  @spec decode(binary()) :: message()
  def decode(<<?B, lsn::64, ts::64, xid::32>>), do: {:begin, lsn, timestamp(ts), xid}
  def decode(<<?C, _flags::8, _commit_lsn::64, end_lsn::64, _ts::64>>), do: {:commit, end_lsn}

  def decode(<<?R, relid::32, rest::binary>>) do
    {schema, rest} = cstring(rest)
    {table, <<_identity::8, count::16, rest::binary>>} = cstring(rest)
    {columns, <<>>} = columns(rest, count, [])
    {:relation, relid, %{schema: schema, table: table, columns: columns}}
  end

  def decode(<<?I, relid::32, ?N, tuple::binary>>), do: {:insert, relid, tuple!(tuple)}

  def decode(<<?U, relid::32, kind, rest::binary>>) when kind in [?K, ?O] do
    {old, <<?N, new::binary>>} = tuple(rest)
    {:update, relid, {old_kind(kind), old}, tuple!(new)}
  end

  def decode(<<?U, relid::32, ?N, new::binary>>), do: {:update, relid, nil, tuple!(new)}

  def decode(<<?D, relid::32, kind, old::binary>>) when kind in [?K, ?O],
    do: {:delete, relid, {old_kind(kind), tuple!(old)}}

  def decode(<<?T, count::32, _options::8, relids::binary-size(count * 4)>>),
    do: {:truncate, for(<<relid::32 <- relids>>, do: relid)}

  # Type, Origin and logical decoding messages carry nothing an event holds.
  def decode(<<kind, _rest::binary>>) when kind in [?Y, ?O, ?M], do: :ignored

  @doc """
  A standby status update reporting `lsn` written, flushed and applied, which
  is what moves the slot's confirmed position.
  """
  @spec status(non_neg_integer()) :: binary()
  def status(lsn) do
    now = System.os_time(:microsecond) - @epoch_us
    <<?r, lsn::64, lsn::64, lsn::64, now::64, 0>>
  end

  @doc "An LSN as Postgres writes it, `1/F73E0220`."
  @spec lsn(non_neg_integer()) :: binary()
  def lsn(lsn) do
    {:ok, text} = Postgrex.ReplicationConnection.encode_lsn(lsn)
    text
  end

  defp timestamp(ts), do: DateTime.from_unix!(ts + @epoch_us, :microsecond)

  defp old_kind(?K), do: :key
  defp old_kind(?O), do: :old

  defp columns(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp columns(<<flags::8, rest::binary>>, n, acc) do
    {name, <<type::32, typmod::signed-32, rest::binary>>} = cstring(rest)
    column = %{name: name, type: type, typmod: typmod, key?: Bitwise.band(flags, 1) == 1}
    columns(rest, n - 1, [column | acc])
  end

  defp tuple!(binary) do
    {values, <<>>} = tuple(binary)
    values
  end

  defp tuple(<<count::16, rest::binary>>), do: values(rest, count, [])

  defp values(rest, 0, acc), do: {Enum.reverse(acc), rest}
  defp values(<<?n, rest::binary>>, n, acc), do: values(rest, n - 1, [nil | acc])
  defp values(<<?u, rest::binary>>, n, acc), do: values(rest, n - 1, [:unchanged | acc])

  defp values(<<?t, size::32, text::binary-size(size), rest::binary>>, n, acc),
    do: values(rest, n - 1, [{:text, text} | acc])

  defp cstring(binary) do
    [string, rest] = :binary.split(binary, <<0>>)
    {string, rest}
  end
end
