defmodule FlezhaPostgresCdc.Copy do
  @moduledoc """
  The first copy: every published table read in the snapshot the slot
  exported, so the copy holds exactly the transactions committed before the
  slot's consistent point and the stream every one after it.

  One `REPEATABLE READ, READ ONLY` transaction adopts the snapshot; each table
  is read through a server-side cursor, `snapshot_batch` rows at a time,
  ordered by its key, its columns selected as the poll source selects them.
  Each batch is handed to the relay and waited for; a refused batch is
  handed over again along the reader's backoff curve, the cursor and the
  snapshot held meanwhile, and the reader is told of every refusal. The
  transaction holds back vacuum on the whole database while it runs.
  """

  alias Flezha.Plugin.Provider.PostgresPoll.Query
  alias FlezhaPostgresCdc.Event
  alias FlezhaPostgresCdc.Reader

  @cursor "flz_cdc_copy"

  @doc """
  Copy `tables` in `snapshot`, every row an event at the consistent point
  `lsn`: the rows copied, or the failure that ended the copy.
  """
  @spec run(DBConnection.conn(), [map()], binary(), binary(), map()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def run(conn, tables, snapshot, lsn, spec) do
    conn
    |> Postgrex.transaction(
      fn tx ->
        run!(tx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        # The name is the server's own answer to the slot's creation.
        run!(tx, "SET TRANSACTION SNAPSHOT '#{String.replace(snapshot, "'", "''")}'")
        Enum.reduce(tables, 0, &(&2 + table!(tx, &1, lsn, spec)))
      end,
      timeout: :infinity
    )
  rescue
    e -> {:error, e}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp table!(tx, table, lsn, spec) do
    columns =
      case Query.catalog(tx, table.schema, table.table) do
        {:ok, %{rows: rows}} -> Query.kinds(rows)
        {:error, reason} -> DBConnection.rollback(tx, reason)
      end

    # The key is named through the alias `r`, so ORDER BY reads the table's
    # column, never the selected one of the same name.
    keys = Enum.map(table.key, &"r.#{Query.quote_name(&1)}")
    order = if keys == [], do: "", else: " ORDER BY " <> Enum.join(keys, ", ")

    select = Enum.join([Query.select_list(columns) | Enum.map(keys, &"#{&1}::text")], ", ")

    run!(
      tx,
      "DECLARE #{@cursor} NO SCROLL CURSOR FOR SELECT #{select} FROM #{Query.relation(table.schema, table.table)} r#{order}"
    )

    count = fetch!(tx, table, columns, {lsn, 0}, spec)
    run!(tx, "CLOSE #{@cursor}")
    count
  end

  defp fetch!(tx, table, columns, {lsn, seen}, spec) do
    rows = run!(tx, "FETCH #{spec.snapshot_batch} FROM #{@cursor}").rows
    width = length(columns)

    inputs =
      rows
      |> Enum.with_index(seen + 1)
      |> Enum.map(fn {row, seq} ->
        {values, key} = Enum.split(row, width)
        Event.copied(table, columns, values, key, seq, lsn, spec)
      end)

    :ok = hand_over_until_taken(inputs, spec, 0)

    if length(rows) < spec.snapshot_batch,
      do: seen + length(rows),
      else: fetch!(tx, table, columns, {lsn, seen + length(rows)}, spec)
  end

  defp hand_over_until_taken(inputs, spec, attempt) do
    case Event.hand_over(inputs, spec) do
      :ok ->
        if attempt > 0, do: send(spec.reader, {:copy, :taken})
        :ok

      {:error, reason} ->
        send(spec.reader, {:copy, :refused, reason})
        Process.sleep(Reader.backoff(attempt + 1, spec.plugin_id))
        hand_over_until_taken(inputs, spec, attempt + 1)
    end
  end

  defp run!(tx, sql) do
    case Postgrex.query(tx, sql, []) do
      {:ok, result} -> result
      {:error, reason} -> DBConnection.rollback(tx, reason)
    end
  end
end
