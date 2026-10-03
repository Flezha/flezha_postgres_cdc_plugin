defmodule FlezhaPostgresCdc do
  @moduledoc """
  The `postgres_cdc` provider: the row changes of a Postgres publication read
  from the database's write-ahead log through a logical replication slot, each
  one input on the operator's entry channel, after a first copy of the
  published tables.

  Each binding runs one `FlezhaPostgresCdc.Reader`. One slot per binding: not
  shareable, and refused on a partitioned operator, whose members a row's key
  decides between. The slot is kept on every disable, since dropping it loses
  the position. Options, the event, the copy, acknowledgement, health and
  telemetry: `docs/plugins/postgres_cdc.md` in the core.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Postgres.Connection
  alias Flezha.Plugin.Provider.PostgresPoll.Query
  alias Flezha.Plugin.ProviderBehaviour
  alias FlezhaPostgresCdc.Reader

  use ProviderBehaviour,
    name: "postgres_cdc",
    feeds: true,
    shareable: false,
    telemetry: [lag_bytes: :metric, snapshot_done: :event],
    options:
      Connection.rows() ++
        [
          channel: [
            type: :names,
            doc:
              "The channel this plugin is attached to: the entry channel every change is injected on, and one name. Required, unless `channel_in` names it"
          ],
          channel_in: [
            type: :names,
            doc:
              "Entry channel every change is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it"
          ],
          plugin_id: [type: :string, doc: "Plugin id; defaults to the id it was enabled under"],
          slot: [
            type: :string,
            required: true,
            doc:
              "The replication slot, one per binding: lower-case letters, digits and underscores, at most 58; the first copy reads through a temporary `<slot>_copy`"
          ],
          publication: [type: :string, required: true, doc: "The publication to read"],
          tables: [
            type: :names,
            doc:
              "With it, the publication is created `FOR TABLE` these, `name` or `schema.name`, when it does not exist"
          ],
          snapshot: [
            type: :enum,
            values: ["initial", "never"],
            default: "initial",
            doc: "`initial` copies the published tables before streaming; `never` only streams"
          ],
          snapshot_batch: [type: :int, default: 1_000, min: 1, doc: "Rows per fetch of the copy"],
          batch_size: [
            type: :int,
            default: 500,
            min: 1,
            doc: "Most changes of one transaction handed over at once"
          ],
          commit_interval_ms: [
            type: :int,
            default: 5_000,
            min: 1,
            doc:
              "Longest the read position waits to be confirmed while changes stream, unless the operator's input is durable"
          ],
          max_lag_bytes: [
            type: :int,
            default: 1_073_741_824,
            min: 1,
            doc: "Write-ahead log the slot may hold back before the plugin reports unhealthy"
          ],
          lag_check_ms: [
            type: :int,
            default: 30_000,
            min: 1,
            doc: "How often the slot's lag is read"
          ],
          failure_threshold: [
            type: :int,
            default: 3,
            min: 1,
            doc: "Refused hand-overs in a row before the plugin reports unhealthy"
          ]
        ],
    worker: [module: Reader, restart: :temporary]

  # A slot name as Postgres allows it, with room for the copy's `_copy`.
  @slot ~r/\A[a-z0-9_]{1,58}\z/

  # A channel written as an atom, which an Elixir-side spec may carry.
  defp convert_value(field, value) when field in [:channel, :channel_in] and is_atom(value),
    do: convert_value(field, Atom.to_string(value))

  @doc "Validate and prepare. Connects to nothing: the reader does, at activation."
  @impl true
  def init_provider(opts) do
    with :ok <- ProviderBehaviour.require_entry_channel(opts, __MODULE__),
         :ok <- ProviderBehaviour.refuse_partitioned(opts, __MODULE__),
         :ok <- check_names(opts) do
      {:ok,
       opts
       |> Connection.prepare()
       |> Map.put(:plugin_id, opts.plugin_id || opts[:source_plugin_id])}
    end
  end

  defp check_names(opts) do
    with :ok <- slot(opts.slot) do
      Query.check_names([
        {:publication, opts.publication, [opts.publication]},
        {:tables, opts.tables, Enum.flat_map(opts.tables || [], &Query.table_names/1)}
      ])
    end
  end

  defp slot(slot) do
    if Regex.match?(@slot, slot), do: :ok, else: ProviderBehaviour.refuse_value(:slot, slot)
  end

  @doc "One reader per binding, started with `FlezhaPostgresCdc.Reader.args/1`."
  @impl true
  def worker_specs(opts), do: [{opts.plugin_id, Reader.args(opts)}]

  @doc """
  The slot outlives every disable: teardown is the host's. This callback
  exists so the provider can be hooked at all: every hook requires the matching
  export.
  """
  @impl true
  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok

  @doc """
  Health verdict from the reader: a failed start check or a lost connection
  names itself, then a slot past `max_lag_bytes` names its lag, then
  `failure_threshold` refused hand-overs in a row name the refusal.
  """
  @impl true
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    ProviderBehaviour.worker_health(
      opts,
      [
        absent: :no_reader,
        dead: :reader_dead,
        budget_ms: 2 * ProviderBehaviour.probe_budget_ms()
      ],
      fn pid ->
        case Reader.probe(pid) do
          {:ok, facts} -> verdict(facts, opts)
          :timeout -> {:fail_open, :probe_timeout}
        end
      end
    )
  end

  defp verdict(%{failure: failure}, _opts) when failure != nil, do: {:unhealthy, failure}

  # The log a slot holds back fills the database server's disk: it outranks a
  # refusing operator, which is what usually makes it grow.
  defp verdict(%{lag_bytes: bytes}, opts) when is_integer(bytes) and bytes > opts.max_lag_bytes,
    do:
      {:unhealthy,
       {:postgres_cdc_slot_lag,
        %{slot: opts.slot, lag_bytes: bytes, max_lag_bytes: opts.max_lag_bytes}}}

  defp verdict(%{refusals: n, last_error: error}, opts) when n >= opts.failure_threshold,
    do: {:unhealthy, {:read_failed, error}}

  defp verdict(_facts, _opts), do: :healthy

  @doc "What `flz operator info` shows beneath the plugin: the slot, the publication and its verdict."
  @impl true
  def inspect_state(opts),
    do: %{slot: opts.slot, publication: opts.publication, health: health(opts)}
end
