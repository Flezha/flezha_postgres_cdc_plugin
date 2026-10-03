defmodule FlezhaPostgresCdc.Stream do
  @moduledoc """
  The replication connection of a `FlezhaPostgresCdc.Reader`, linked to it.

  Started for a copy (`:copy`), it creates the temporary slot `<slot>_copy`
  with an exported snapshot, tells the reader the consistent point and the
  snapshot's name, and stays idle until the reader says `:start` (the snapshot
  lives only while this connection is idle); by then the reader kept the slot
  as the permanent `slot`, and the temporary one is dropped. Started to stream
  without a copy (`:create`), it creates `slot` itself; started on an existing
  slot (`:stream`), it streams at once. The stream begins at the slot's
  confirmed position.

  Each change of a transaction becomes an event (`FlezhaPostgresCdc.Event`),
  handed over in batches of at most `batch_size` and at every commit. A
  transaction whose every change was taken is accepted; the confirmed position
  follows the accepted one by `Host.position_due?/4`: at once with durable
  input, at most every `commit_interval_ms` without, and whenever the stream
  is idle, when a keepalive's `wal_end` is accepted too. A refused hand-over
  ends the connection with `{:shutdown, {:ingest_refused, reason}}`, and the
  reader starts it again from the last confirmed position.

  `Postgrex.ReplicationConnection` keeps its options, the password included,
  in the process dictionary: the process is `:sensitive`, so no process
  inspection or crash report shows them.
  """

  use Postgrex.ReplicationConnection

  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.PostgresPoll.Query
  alias Flezha.Plugin.ProviderBehaviour
  alias FlezhaPostgresCdc.Event
  alias FlezhaPostgresCdc.Pgoutput

  require Logger

  @provider :postgres_cdc

  @doc """
  Start the connection for `args`, its replication options from
  `Connection.conn_opts/2` with the password resolved by their own
  `configure` closure, which the replication connection would not call.
  """
  @spec start_link(map()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    {configure, conn_opts} = Keyword.pop!(args.conn_opts, :configure)

    Postgrex.ReplicationConnection.start_link(
      __MODULE__,
      Map.delete(args, :conn_opts),
      configure.(conn_opts) ++ [sync_connect: false]
    )
  end

  @impl true
  def init(args) do
    Process.flag(:sensitive, true)
    now = :erlang.monotonic_time(:millisecond)

    {:ok,
     Map.merge(args, %{
       relations: %{},
       told: [],
       tx: nil,
       accepted: 0,
       committed: 0,
       committed_at: now
     })}
  end

  @impl true
  def handle_connect(%{mode: :copy} = state),
    do: create(state, "#{temporary(state.slot)} TEMPORARY")

  def handle_connect(%{mode: :create} = state), do: create(state, state.slot)

  def handle_connect(state), do: started(state)

  @impl true
  def handle_result([%Postgrex.Result{rows: [[_slot, lsn, snapshot, _plugin]]}], state) do
    send(state.reader, {:slot_created, self(), lsn, snapshot})
    {:noreply, state}
  end

  def handle_result([%Postgrex.Result{}], %{mode: :dropping} = state),
    do: started(%{state | mode: :stream})

  def handle_result(%Postgrex.Error{} = error, _state), do: exit({:shutdown, error})

  @doc "The temporary slot a copy into `slot` reads through."
  @spec temporary(binary()) :: binary()
  def temporary(slot), do: slot <> "_copy"

  defp create(state, slot),
    do: {:query, "CREATE_REPLICATION_SLOT #{slot} LOGICAL pgoutput EXPORT_SNAPSHOT", state}

  @impl true
  def handle_info(:start, %{mode: :copy} = state),
    do: {:query, "DROP_REPLICATION_SLOT #{temporary(state.slot)}", %{state | mode: :dropping}}

  def handle_info(:start, state), do: started(state)
  # Handled only once the replication started: a refused start ends the process first.
  def handle_info(:started, state), do: {:noreply, tell(state, :failure)}
  def handle_info(_other, state), do: {:noreply, state}

  defp started(state) do
    send(self(), :started)

    query =
      "START_REPLICATION SLOT #{state.slot} LOGICAL 0/0 " <>
        "(proto_version '1', publication_names '#{Query.quote_name(state.publication)}')"

    {:stream, query, [], state}
  end

  @impl true
  def handle_data(<<?w, _start::64, _end::64, _clock::64, data::binary>>, state),
    do: data |> Pgoutput.decode() |> apply_message(state)

  # A keepalive outside a transaction: everything the server sent up to its
  # `wal_end` was taken, so the stream is idle up to there.
  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, state) do
    {acks, state} =
      case state.tx do
        nil -> confirm(%{state | accepted: max(state.accepted, wal_end)}, :caught_up)
        _open -> {[], state}
      end

    {:noreply, if(reply == 1, do: with_status(acks, state), else: acks), state}
  end

  def handle_data(:done, _state), do: exit({:shutdown, :stream_ended})

  # ---------- one message ----------

  defp apply_message({:begin, lsn, time, xid}, state),
    do: {:noreply, %{state | tx: %{lsn: lsn, time: time, xid: xid, seq: 0, pending: []}}}

  defp apply_message({:relation, relid, relation}, state),
    do: {:noreply, put_in(state.relations[relid], kinds(relation, state))}

  defp apply_message({:insert, relid, new}, state),
    do: change(state, &Event.change("c", state.relations[relid], nil, new, &1, state))

  defp apply_message({:update, relid, old, new}, state),
    do: change(state, &Event.change("u", state.relations[relid], old, new, &1, state))

  defp apply_message({:delete, relid, old}, state),
    do: change(state, &Event.change("d", state.relations[relid], old, nil, &1, state))

  defp apply_message({:truncate, relids}, state),
    do:
      change(
        state,
        &Event.truncate(Enum.map(relids, fn id -> state.relations[id] end), &1, state)
      )

  defp apply_message({:commit, end_lsn}, state) do
    state = flush(state)
    {acks, state} = confirm(%{state | tx: nil, accepted: end_lsn}, :more)
    {:noreply, with_status(acks, state), state}
  end

  defp apply_message(:ignored, state), do: {:noreply, state}

  defp change(state, event) do
    tx = %{state.tx | seq: state.tx.seq + 1}
    tx = %{tx | pending: [event.(tx) | tx.pending]}
    state = %{state | tx: tx}

    if length(tx.pending) >= state.batch_size do
      state = flush(state)
      {:noreply, with_status([], state), state}
    else
      {:noreply, state}
    end
  end

  # A relation's columns with the kinds the catalog reads them as, which the
  # copy read them with too.
  defp kinds(relation, state) do
    case Query.catalog(state.conn, relation.schema, relation.table) do
      {:ok, %{rows: rows}} ->
        kinds = Map.new(Query.kinds(rows))
        columns = Enum.map(relation.columns, &Map.put(&1, :kind, Map.get(kinds, &1.name, :text)))
        %{relation | columns: columns}

      {:error, reason} ->
        exit({:shutdown, {:catalog_failed, reason}})
    end
  end

  # ---------- hand-over and position ----------

  defp flush(%{tx: %{pending: []}} = state), do: state

  defp flush(state) do
    inputs = Enum.reverse(state.tx.pending)

    case Event.hand_over(inputs, state) do
      :ok ->
        emit(:shipped, %{count: length(inputs)}, labels(state))
        tell(put_in(state.tx.pending, []), :taken)

      {:error, reason} ->
        refused(state, inputs, reason)
    end
  end

  defp refused(state, inputs, reason) do
    error = "ingest refused: #{inspect(reason)}"

    Logger.warning(
      "[FlezhaPostgresCdc.Stream] slot=#{state.slot} lsn=#{Pgoutput.lsn(state.tx.lsn)} " <>
        "changes=#{length(inputs)} #{error}; streaming again from the last confirmed position"
    )

    emit(:failed, %{count: length(inputs)}, Map.put(labels(state), :reason, error))
    exit({:shutdown, {:ingest_refused, reason}})
  end

  defp confirm(state, next) do
    now = :erlang.monotonic_time(:millisecond)

    if Host.position_due?(state, state.accepted, next, now),
      do:
        {[Pgoutput.status(state.accepted)],
         %{state | committed: state.accepted, committed_at: now}},
      else: {[], state}
  end

  # A status after every hand-over keeps the server from ending a connection
  # that waited on a slow operator.
  defp with_status([], state), do: [Pgoutput.status(state.committed)]
  defp with_status(acks, _state), do: acks

  # The reader is told once that the stream works (`:failure` is over) and once
  # that the operator takes its changes (`:taken`, the refusals are over).
  defp tell(state, what) do
    if what in state.told do
      state
    else
      send(state.reader, {:streaming, self(), what})
      %{state | told: [what | state.told]}
    end
  end

  defp labels(state), do: %{plugin_id: state.plugin_id, slot: state.slot}

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)
end
