defmodule FlezhaPostgresCdc.Event do
  @moduledoc """
  A row change as the input the relay receives, and its hand-over.

  The payload is one JSON object: `op`, `schema`, `table`, `before`, `after`,
  `unchanged`, `lsn`, `xid`, `commit_time` and `seq` (a truncate carries
  `tables` in place of a row). A row's values are written by
  `Flezha.Plugin.Provider.Postgres.Connection.encode/2`: a copied row from the terms the driver decoded,
  a streamed one from its text parsed by `FlezhaPostgresCdc.Values`. The
  metadata carries the entry channel, `flz.msg_id`, `pg.table` and `pg.op`.
  """

  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Host.MsgId
  alias Flezha.Plugin.Provider.Postgres.Connection
  alias Flezha.Relay.Ingest.Verdict
  alias FlezhaPostgresCdc.Catalog
  alias FlezhaPostgresCdc.Pgoutput
  alias FlezhaPostgresCdc.Values

  require MsgId
  require Verdict

  # How long one hand-over waits for the relay's verdicts. Well under the
  # server's `wal_sender_timeout` (60 s by default), which ends a replication
  # connection that answers nothing for that long.
  @inject_timeout_ms 20_000

  @doc """
  A streamed insert (`c`), update (`u`) or delete (`d`) of `relation` in the
  transaction `tx`, as the `seq`th change of it.
  """
  @spec change(binary(), map(), term(), list() | nil, map(), map()) :: {binary(), map()}
  def change(op, relation, old, new, tx, spec) do
    {row, unchanged} = if new, do: streamed(relation.columns, new), else: {nil, []}
    rows = {before(relation.columns, old), row, unchanged}
    event(op, relation, rows, position(tx), streamed_id(tx), spec)
  end

  @doc "A truncate of `relations` in `tx`, as the `seq`th change of it."
  @spec truncate([map()], map(), map()) :: {binary(), map()}
  def truncate(relations, tx, spec) do
    names = Enum.map(relations, &Catalog.name/1)

    [{"op", "t"}, {"tables", names} | position(tx)]
    |> input(streamed_id(tx), Enum.join(names, ","), "t", spec)
  end

  @doc """
  A row of the first copy of `table` (`op` `r`): the driver's terms of its
  `columns`, its key's text (empty for a table without one), its position
  `seq` in the table, and the slot's consistent point `lsn`. Its id names the
  consistent point, so a later copy's rows are new inputs, and the table and
  the key as one JSON array, so no two of them run into each other.
  """
  @spec copied(map(), [{binary(), term()}], list(), [binary()], pos_integer(), binary(), map()) ::
          {binary(), map()}
  def copied(table, columns, values, key, seq, lsn, spec) do
    position = if key == [], do: [seq], else: key
    id = "snapshot/#{lsn}/" <> Jason.encode!([table.schema, table.table | position])
    rows = {nil, fragment(columns, values), []}
    event("r", table, rows, position(lsn, nil, nil, seq), id, spec)
  end

  @doc """
  Hand `inputs` to the relay and wait for every verdict: `:ok` when it took
  them all (`Flezha.Relay.Ingest.Verdict.taken?/1`), else the first refusal.
  A hand-over that outlasts its timeout is a refusal.
  """
  @spec hand_over([{binary(), map()}], map()) :: :ok | {:error, term()}
  def hand_over([], _spec), do: :ok

  def hand_over(inputs, spec) do
    spec.overwatcher_pid
    |> Host.inject_many(inputs, :verdict, @inject_timeout_ms)
    |> Enum.find(:ok, &(not Verdict.taken?(&1)))
  end

  # One row's event, copied or streamed: the same fields in the same order.
  defp event(op, table, {before, row, unchanged}, position, id, spec) do
    [
      {"op", op},
      {"schema", table.schema},
      {"table", table.table},
      {"before", before},
      {"after", row},
      {"unchanged", unchanged}
      | position
    ]
    |> input(id, Catalog.name(table), op, spec)
  end

  defp position(tx),
    do: position(Pgoutput.lsn(tx.lsn), tx.xid, DateTime.to_iso8601(tx.time), tx.seq)

  defp position(lsn, xid, commit_time, seq),
    do: [{"lsn", lsn}, {"xid", xid}, {"commit_time", commit_time}, {"seq", seq}]

  # A streamed change's id: its transaction's final LSN and its place in it.
  defp streamed_id(tx), do: Pgoutput.lsn(tx.lsn) <> "/#{tx.seq}"

  defp input(fields, id, table, op, spec) do
    {Jason.encode!(Jason.OrderedObject.new(fields)),
     MsgId.stamp(
       %{"channel" => spec.entry_channel, "pg.table" => table, "pg.op" => op},
       spec.plugin_id,
       id
     )}
  end

  # The key columns of a key tuple, every column of a full old row.
  defp before(_columns, nil), do: nil

  defp before(columns, {:key, values}) do
    {keys, key_values} =
      columns |> Enum.zip(values) |> Enum.filter(&elem(&1, 0).key?) |> Enum.unzip()

    elem(streamed(keys, key_values), 0)
  end

  defp before(columns, {:old, values}), do: elem(streamed(columns, values), 0)

  # A streamed tuple as its JSON object and the unchanged TOAST columns it left out.
  defp streamed(columns, values) do
    {sent, unchanged} =
      columns |> Enum.zip(values) |> Enum.split_with(&(elem(&1, 1) != :unchanged))

    terms =
      for {column, value} <- sent do
        case value do
          nil -> nil
          {:text, text} -> Values.parse(column.kind, column.type, column.typmod, text)
        end
      end

    {fragment(Enum.map(sent, &{elem(&1, 0).name, elem(&1, 0).kind}), terms),
     Enum.map(unchanged, &elem(&1, 0).name)}
  end

  defp fragment(columns, values), do: Jason.Fragment.new(Connection.encode(columns, values))
end
