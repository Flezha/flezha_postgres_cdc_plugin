defmodule FlezhaPostgresCdc.Test.Helpers do
  @moduledoc """
  What the package's tests need beyond the core's shared helpers: a plugin
  prepared and activated as the extension table does it, a stand-in relay, and
  the dev database.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]
  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 2]

  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias FlezhaTest.Helper.NodeTestHelper

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
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid, :shutdown) end)
    {prepared, pid}
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

  @doc """
  Every input the stand-in host took, as `{decoded body, metadata}`: what it
  took so far, and what it takes within `ms` of the last one.
  """
  def taken(ms \\ 0) do
    collect after: ms do
      {:ingest, body, metadata} -> {Jason.decode!(body), metadata}
    end
  end
end
