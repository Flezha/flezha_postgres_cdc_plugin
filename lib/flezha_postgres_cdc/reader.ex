defmodule FlezhaPostgresCdc.Reader do
  @moduledoc """
  The worker of a `FlezhaPostgresCdc` binding: one process owning a normal
  connection, the replication connection (`FlezhaPostgresCdc.Stream`, linked),
  the run of failures and the health facts.

  It starts with the checks of `FlezhaPostgresCdc.Catalog` in a monitored
  child, so it answers its health probe meanwhile. The slot's existence is the
  one fact of the copy: when `snapshot` is `initial` and the slot is missing,
  the stream creates a temporary slot, `FlezhaPostgresCdc.Copy` reads in the
  snapshot it exported, in a child too, and only a finished copy keeps that
  slot as the permanent one. A copy cut short leaves no slot, so the next start
  copies again; an unbind, a disable or a restart after the copy finds the
  slot and streams. The stream then starts at the slot's consistent point.

  A failed check, copy or stream starts over from the checks along the
  package's backoff curve. Every `lag_check_ms` it reads how much log the slot
  holds back. Log lines, telemetry and verdicts name the slot, a count or a
  server's error code and message, never a row or the connection's options.
  """

  alias Flezha.Config.Backoff
  alias Flezha.Plugin.Provider.Postgres.Connection
  alias Flezha.Plugin.Provider.Worker
  alias Flezha.Plugin.ProviderBehaviour
  alias FlezhaPostgresCdc.Catalog
  alias FlezhaPostgresCdc.Copy
  alias FlezhaPostgresCdc.Stream

  # The facts the provider's `health/1` judges: the failure the plugin is
  # unhealthy with, the run of refused hand-overs and its last error, and the
  # slot's lag.
  use Worker, facts: [:failure, :consecutive_failures, :last_error, :lag_bytes]
  use Flezha.Process.Timer, keys: [:start, :lag]

  require Logger

  @provider :postgres_cdc

  # The wait before starting over after a failure: from half a second, doubling,
  # to half a minute.
  @backoff %{name: :postgres_cdc_reconnect, base: 500, cap: 30_000, exponent_cap: 8, jitter: true}

  # The failures `FlezhaPostgresCdc.Catalog.check/2` names.
  @checked [
    :postgres_cdc_wal_level_not_logical,
    :postgres_cdc_server_too_old,
    :postgres_cdc_publication_missing,
    :postgres_cdc_replica_identity_missing
  ]

  @doc """
  The reader's start arguments, from the plugin's prepared options: the normal
  connection's options, with no pool and a session in UTC, and the replication
  connection's, in UTC too, so a timestamp's text is the same on both. The
  password reaches it only inside their closure.
  """
  @spec args(map()) :: map()
  def args(opts) do
    opts
    |> Map.take([
      :plugin_id,
      :source_plugin_id,
      :overwatcher_pid,
      :entry_channel,
      :durable_input,
      :slot,
      :publication,
      :tables,
      :snapshot,
      :snapshot_batch,
      :batch_size,
      :commit_interval_ms,
      :max_lag_bytes,
      :lag_check_ms
    ])
    |> Map.merge(%{
      conn_opts: Connection.conn_opts(opts, pool_size: 1, parameters: [TimeZone: "UTC"]),
      repl_opts: Connection.conn_opts(opts, parameters: [TimeZone: "UTC"]),
      where: "slot=#{opts.slot} #{Connection.describe(Connection.label(opts))}"
    })
  end

  @impl true
  def init(args) do
    # A stop closes both connections, which only a trapped shutdown reaches.
    Process.flag(:trap_exit, true)

    case Postgrex.start_link(args.conn_opts) do
      {:ok, conn} ->
        state =
          args
          |> Map.drop([:name, :conn_opts])
          |> Map.merge(Worker.fields(args))
          |> Map.merge(%{
            conn: conn,
            published: [],
            stream_mode: nil,
            stream: nil,
            tasks: %{},
            failure: nil,
            attempt: 0,
            lag_bytes: nil
          })

        # Scheduled, never run here: activation is synchronous in the host.
        {:ok, state |> arm_timer(:start, 0, :start) |> arm_timer(:lag, args.lag_check_ms, :lag)}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:start, state) do
    state = timer_fired(state, :start)
    {:noreply, run(state, :check, fn conn -> Catalog.check(conn, state) end)}
  end

  def handle_info(:lag, state) do
    state = state |> timer_fired(:lag) |> arm_timer(:lag, state.lag_check_ms, :lag)
    {:noreply, run(state, :lag, &Catalog.lag(&1, state.slot))}
  end

  def handle_info({:slot_created, stream, lsn, snapshot}, %{stream: stream} = state),
    do: {:noreply, state |> recovered(:failure) |> copy(lsn, snapshot)}

  def handle_info({:copy, :refused, reason}, state), do: {:noreply, copy_refused(state, reason)}
  def handle_info({:copy, :taken}, state), do: {:noreply, recovered(state, :taken)}

  # The stream answered its first message, or the relay took its first batch.
  def handle_info({:streaming, stream, what}, %{stream: stream} = state),
    do: {:noreply, recovered(state, what)}

  def handle_info({:done, kind, pid, result}, state) do
    case Map.pop(state.tasks, kind) do
      {{^pid, ref}, tasks} ->
        Process.demonitor(ref, [:flush])
        {:noreply, done(%{state | tasks: tasks}, kind, result)}

      _stale ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.tasks, fn {_kind, {_pid, r}} -> r == ref end) do
      {kind, _} ->
        {:noreply,
         done(%{state | tasks: Map.delete(state.tasks, kind)}, kind, {:error, {:exit, reason}})}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, stream, reason}, %{stream: stream} = state),
    do: {:noreply, stream_lost(%{state | stream: nil}, reason)}

  def handle_info({:EXIT, conn, reason}, %{conn: conn} = state),
    do: {:stop, {:connection_exit, reason}, state}

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.tasks, fn {_kind, {pid, _ref}} -> Process.exit(pid, :kill) end)
    if state.stream, do: Process.exit(state.stream, :kill)
    Process.exit(state.conn, :shutdown)
    :ok
  end

  # ---------- children ----------

  defp run(state, kind, fun) do
    if Map.has_key?(state.tasks, kind) do
      state
    else
      {worker, conn} = {self(), state.conn}

      # Linked so it dies with the reader; monitored for its answer.
      {pid, ref} =
        spawn_monitor(fn ->
          Process.link(worker)
          send(worker, {:done, kind, self(), fun.(conn)})
        end)

      put_in(state.tasks[kind], {pid, ref})
    end
  end

  defp done(state, :check, {:ok, %{tables: tables, slot?: slot?}}) do
    mode =
      cond do
        slot? -> :stream
        state.snapshot == "initial" -> :copy
        true -> :create
      end

    start_stream(%{state | published: tables}, mode)
  end

  defp done(state, :copy, {:ok, count}), do: copied(state, count)
  defp done(state, :lag, {:ok, bytes}), do: lagged(state, bytes)
  defp done(state, :lag, {:error, _reason}), do: state
  defp done(state, kind, {:error, reason}), do: failed(state, kind, reason)

  # ---------- the stream and the copy ----------

  defp start_stream(state, mode) do
    args =
      state
      |> Map.take([
        :plugin_id,
        :overwatcher_pid,
        :entry_channel,
        :durable_input,
        :slot,
        :publication,
        :batch_size,
        :commit_interval_ms,
        :conn
      ])
      |> Map.merge(%{reader: self(), conn_opts: state.repl_opts, mode: mode})

    case Stream.start_link(args) do
      {:ok, stream} -> %{state | stream: stream, stream_mode: mode}
      {:error, reason} -> failed(state, :stream, reason)
    end
  end

  # The slot is new: copy through it when it is the temporary one, then stream.
  defp copy(%{stream_mode: :create} = state, _lsn, _snapshot), do: begin_stream(state)

  defp copy(state, lsn, snapshot) do
    {tables, slot} = {state.published, state.slot}

    spec =
      state
      |> Map.take([:plugin_id, :overwatcher_pid, :entry_channel, :snapshot_batch])
      |> Map.put(:reader, self())

    run(state, :copy, fn conn ->
      with {:ok, count} <- Copy.run(conn, tables, snapshot, lsn, spec),
           :ok <- Catalog.keep_slot(conn, Stream.temporary(slot), slot),
           do: {:ok, count}
    end)
  end

  defp copied(state, count) do
    Logger.info("[FlezhaPostgresCdc] #{state.where} copied #{count} rows; streaming")
    emit(:snapshot_done, %{count: count}, labels(state))
    state |> recovered(:taken) |> begin_stream()
  end

  defp begin_stream(state) do
    send(state.stream, :start)
    state
  end

  defp lagged(state, bytes) do
    if bytes, do: emit(:lag_bytes, %{bytes: bytes}, labels(state))
    %{state | lag_bytes: bytes}
  end

  # ---------- failures ----------

  defp stream_lost(state, {:shutdown, {:ingest_refused, reason}}),
    do: state |> stop_tasks() |> refused(reason)

  defp stream_lost(state, reason), do: state |> stop_tasks() |> failed(:stream, reason)

  # A failure a start check names is the verdict as it stands.
  defp failed(state, _kind, {tag, %{}} = failure) when tag in @checked do
    if state.failure != failure,
      do: Logger.warning("[FlezhaPostgresCdc] #{state.where} #{inspect(failure)}")

    %{state | failure: failure} |> stop_stream() |> retry()
  end

  defp failed(state, kind, reason) do
    error = Connection.sanitize_error(unwrap(reason))
    Logger.warning("[FlezhaPostgresCdc] #{state.where} #{kind} failed: #{error}")
    emit(:failed, %{count: 1}, Map.put(labels(state), :reason, error))

    %{state | failure: {:postgres_cdc_replication_failed, %{where: state.where, reason: error}}}
    |> stop_stream()
    |> retry()
  end

  defp refused(state, reason) do
    state |> Worker.failed("ingest refused: #{inspect(reason)}") |> retry()
  end

  defp unwrap({:shutdown, reason}), do: unwrap(reason)
  defp unwrap({:exit, reason}), do: unwrap(reason)
  defp unwrap(reason), do: reason

  # A copy keeps its place: the batch is handed over again, nothing starts over.
  defp copy_refused(state, reason) do
    error = "ingest refused: #{inspect(reason)}"

    Logger.warning(
      "[FlezhaPostgresCdc] #{state.where} copy #{error}; handing the batch over again"
    )

    emit(:failed, %{count: 1}, Map.put(labels(state), :reason, error))
    Worker.failed(state, error)
  end

  defp retry(state) do
    attempt = state.attempt + 1
    %{state | attempt: attempt} |> arm_timer(:start, backoff(attempt, state.plugin_id), :start)
  end

  @doc "The wait before `attempt` (1-based) of the package's backoff curve, announced for `plugin_id`."
  @spec backoff(pos_integer(), binary()) :: non_neg_integer()
  def backoff(attempt, plugin_id), do: Backoff.arm(@backoff, attempt, {@provider, plugin_id})

  # Connected again clears the failure; the backoff starts over only once the
  # relay takes something, so a refusing relay is not re-fed at once.
  defp recovered(state, :failure), do: %{state | failure: nil}
  defp recovered(state, :taken), do: Worker.recovered(%{state | attempt: 0})

  defp stop_stream(%{stream: nil} = state), do: state

  defp stop_stream(state) do
    Process.unlink(state.stream)
    Process.exit(state.stream, :kill)
    %{state | stream: nil}
  end

  defp stop_tasks(state) do
    for {kind, {pid, ref}} <- state.tasks, kind != :lag do
      Process.demonitor(ref, [:flush])
      Process.exit(pid, :kill)
    end

    %{state | tasks: Map.take(state.tasks, [:lag])}
  end

  defp labels(state), do: %{plugin_id: state.plugin_id, slot: state.slot}

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)
end
