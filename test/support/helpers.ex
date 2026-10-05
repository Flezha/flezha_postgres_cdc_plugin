defmodule FlezhaPostgresCdc.Test.Helpers do
  @moduledoc """
  What the package's tests need from a running controller and nothing more: a
  poll, a telemetry capture, a plugin prepared and activated as the extension
  table does it, a stand-in relay, and the dev database.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.ProviderBehaviour.Options

  @doc "Where the dev stack's `postgres-cdc` listens, and who the tests connect as."
  def database,
    do: %{
      hostname: "localhost",
      port: 5434,
      database: "flezha_test",
      username: "flezha",
      password: "flezha_test"
    }

  @doc "A connection to the dev database, closed when the test exits."
  def connect() do
    {:ok, conn} = Postgrex.start_link(Map.to_list(database()))
    Process.unlink(conn)
    on_exit(fn -> Process.exit(conn, :shutdown) end)
    conn
  end

  @doc "Run each statement of `sqls` on `conn`."
  def sql!(conn, sqls), do: Enum.map(List.wrap(sqls), &Postgrex.query!(conn, &1, []))

  @doc "Poll `fun` every `interval_ms` until it is truthy or `timeout_ms` passes."
  def wait_until(fun, timeout_ms, interval_ms \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll(fun, deadline, interval_ms)
  end

  defp poll(fun, deadline, interval_ms) do
    case fun.() do
      falsy when falsy in [nil, false] ->
        if System.monotonic_time(:millisecond) >= deadline do
          falsy
        else
          Process.sleep(interval_ms)
          poll(fun, deadline, interval_ms)
        end

      truthy ->
        truthy
    end
  end

  @doc """
  Forward every emission of `events` to the calling process as
  `{:telemetry, event, measurements, metadata}` until the test exits.
  """
  def capture(events) do
    test_pid = self()
    handler_id = {__MODULE__, events, System.unique_integer([:positive])}

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  @doc "The options the provider prepares from `opts`, as the extension table builds them."
  def prepare(opts, host \\ %{}) do
    host =
      Map.merge(
        %{
          overwatcher_pid: self(),
          source_namespace: :default,
          source_relay_id: "op",
          source_plugin_id: "cdc",
          subscribed_extensions: [:on_terminate]
        },
        host
      )

    with {:ok, normalised} <- Options.normalise_opts(FlezhaPostgresCdc, Map.merge(host, opts)),
         do: FlezhaPostgresCdc.init_provider(normalised)
  end

  @doc """
  Prepare `opts` and start the declared worker through the table's own
  `Workers.start/5`; answers the prepared opts and the plugin's worker.
  """
  def activate(opts, host \\ %{}) do
    {:ok, prepared} = prepare(opts, host)
    plugin_id = prepared.plugin_id

    {started, :ok} =
      Workers.start(
        %{entries: %{}, monitors: %{}},
        plugin_id,
        FlezhaPostgresCdc,
        prepared,
        & &1.()
      )

    pid = Entry.owned_pid(started.entries[plugin_id])
    on_exit(fn -> stop(pid) end)
    {prepared, pid}
  end

  @doc "Stop the worker `pid`, as its supervisor would, if it is still alive."
  def stop(pid) do
    GenServer.stop(pid, :shutdown)
  catch
    :exit, _gone -> :ok
  end

  @doc """
  The host relay of a feeding worker, linked to the caller: it answers each
  ingest with the verdict `verdict.(body, metadata)` gives, `:ok` by default,
  and tells the caller `{:ingest, body, metadata}` for every one it takes, and
  `{:persist, key, value}` for every option the worker commits. With `absorb:
  true` it takes an input whose `flz.msg_id` it took before without telling,
  as a durable input log absorbs it.
  """
  def stand_in_host(verdict \\ fn _body, _metadata -> :ok end, opts \\ []) do
    test = self()
    spawn_link(fn -> host_loop(test, verdict, opts[:absorb] == true) end)
  end

  defp host_loop(test, verdict, absorb?) do
    receive do
      {:"$gen_call", from, {:ingest, body, metadata}} ->
        answer = verdict.(body, metadata)
        seen? = absorb? and Process.put({:seen, metadata["flz.msg_id"]}, true) == true
        if answer == :ok and not seen?, do: send(test, {:ingest, body, metadata})
        GenServer.reply(from, answer)

      {:"$gen_cast", {:plugin_opt_persist, _plugin_id, key, value}} ->
        send(test, {:persist, key, value})
    end

    host_loop(test, verdict, absorb?)
  end

  @doc "Every input the stand-in host took so far, as `{decoded body, metadata}`."
  def taken(acc \\ []) do
    receive do
      {:ingest, body, metadata} -> taken([{Jason.decode!(body), metadata} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
