defmodule FlezhaPostgresCdc.Catalog do
  @moduledoc """
  What the plugin asks the database on its normal connection: the start checks
  (logical replication, the server's version, the publication, each published
  table's replica identity), whether the slot exists, keeping the slot a copy
  read into, and how much log it holds back.

  Every name in a statement was validated and is quoted
  (`Flezha.Plugin.Provider.Postgres.Connection`); every value is a parameter.
  A failure the plugin reports as unhealthy is `{tag, details}`, a reason of
  the core's error catalogue.
  """

  alias Flezha.Plugin.Provider.Postgres.Connection

  @settings "SELECT current_setting('wal_level'), current_setting('server_version_num')::int, " <>
              "current_setting('server_version')"

  # Each of the named tables, its replica identity and the columns of its key:
  # the identity index's under `USING INDEX`, the primary key's otherwise.
  @tables """
  SELECT n.nspname, c.relname, c.relreplident,
         coalesce((SELECT array_agg(a.attname ORDER BY k.i)
                   FROM pg_catalog.pg_index x
                   CROSS JOIN LATERAL unnest(x.indkey::int2[]) WITH ORDINALITY AS k(attnum, i)
                   JOIN pg_catalog.pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = k.attnum
                   WHERE x.indrelid = c.oid
                     AND CASE c.relreplident WHEN 'i' THEN x.indisreplident ELSE x.indisprimary END),
                  '{}')
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.oid = ANY($1::text[]::regclass[])
  ORDER BY 1, 2
  """

  @published "SELECT format('%I.%I', schemaname, tablename) " <>
               "FROM pg_catalog.pg_publication_tables WHERE pubname = $1"

  @lag "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint " <>
         "FROM pg_catalog.pg_replication_slots WHERE slot_name = $1"

  # The oldest server whose logical replication the plugin reads.
  @min_version 140_000

  @doc """
  The start checks of `spec`, in order: logical replication, the version, the
  publication or else its `tables`, and a usable replica identity on every
  table it publishes. Only then is a missing publication created `FOR TABLE`
  its `tables`, so a failed check creates nothing. Answers the published
  tables, each `%{schema, table, key}`, and whether the slot exists.
  """
  @spec check(DBConnection.conn(), map()) ::
          {:ok, %{tables: [map()], slot?: boolean()}} | {:error, term()}
  def check(conn, spec) do
    with {:ok, %{rows: [[wal_level, version_num, version]]}} <- query(conn, @settings, []),
         :ok <- logical(wal_level),
         :ok <- recent(version_num, version),
         {:ok, exists?} <- publication?(conn, spec.publication),
         {:ok, names} <- published(conn, spec, exists?),
         {:ok, tables} <- tables(conn, names),
         :ok <- create_publication(conn, spec, exists?),
         {:ok, %{rows: slot}} <-
           query(conn, "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1", [
             spec.slot
           ]) do
      {:ok, %{tables: tables, slot?: slot != []}}
    end
  end

  defp logical("logical"), do: :ok
  defp logical(level), do: {:error, {:postgres_cdc_wal_level_not_logical, %{wal_level: level}}}

  defp recent(num, _version) when num >= @min_version, do: :ok
  defp recent(_num, version), do: {:error, {:postgres_cdc_server_too_old, %{version: version}}}

  defp publication?(conn, publication) do
    with {:ok, %{rows: rows}} <-
           query(conn, "SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1", [publication]),
         do: {:ok, rows != []}
  end

  # The tables the publication holds, or the ones it will be created for.
  defp published(conn, spec, true) do
    with {:ok, %{rows: rows}} <- query(conn, @published, [spec.publication]),
         do: {:ok, List.flatten(rows)}
  end

  defp published(_conn, %{tables: nil} = spec, false),
    do: {:error, {:postgres_cdc_publication_missing, %{publication: spec.publication}}}

  defp published(_conn, spec, false), do: {:ok, Enum.map(spec.tables, &relation/1)}

  defp create_publication(_conn, _spec, true), do: :ok

  defp create_publication(conn, spec, false) do
    sql =
      "CREATE PUBLICATION #{Connection.quote_name(spec.publication)} FOR TABLE " <>
        Enum.map_join(spec.tables, ", ", &relation/1)

    with {:ok, _} <- query(conn, sql, []), do: :ok
  end

  # A table as `tables` writes it, `name` or `schema.name`, quoted.
  defp relation(written) do
    {schema, table} = Connection.split_table(written)
    Connection.relation(schema, table)
  end

  defp tables(conn, names) do
    with {:ok, %{rows: rows}} <- query(conn, @tables, [names]) do
      rows
      |> Enum.map(fn [schema, table, identity, key] ->
        %{schema: schema, table: table, identity: identity, key: key}
      end)
      |> Enum.find(&(&1.identity == "n" or (&1.identity in ["d", "i"] and &1.key == [])))
      |> case do
        nil -> {:ok, Enum.map(rows, &table/1)}
        bad -> {:error, {:postgres_cdc_replica_identity_missing, %{table: name(bad)}}}
      end
    end
  end

  defp table([schema, table, _identity, key]), do: %{schema: schema, table: table, key: key}

  @doc "A table's name as an input and a message id carry it, `schema.table`."
  @spec name(map()) :: binary()
  def name(%{schema: schema, table: table}), do: "#{schema}.#{table}"

  @doc """
  Keep the temporary slot a finished copy read into as the permanent `slot`, at
  the same position: from here on the slot's existence says the copy ended.
  """
  @spec keep_slot(DBConnection.conn(), binary(), binary()) :: :ok | {:error, term()}
  def keep_slot(conn, temporary, slot) do
    with {:ok, _} <-
           query(conn, "SELECT pg_copy_logical_replication_slot($1, $2, false)", [temporary, slot]),
         do: :ok
  end

  @doc "The bytes of log `slot` holds back, or `nil` while it has no confirmed position."
  @spec lag(DBConnection.conn(), binary()) :: {:ok, non_neg_integer() | nil} | {:error, term()}
  def lag(conn, slot) do
    case query(conn, @lag, [slot]) do
      {:ok, %{rows: [[bytes]]}} -> {:ok, bytes}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp query(conn, sql, params) do
    Postgrex.query(conn, sql, params)
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end
end
