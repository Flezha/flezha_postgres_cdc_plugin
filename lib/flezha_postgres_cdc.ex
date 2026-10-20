defmodule FlezhaPostgresCdc do
  @moduledoc """
  The `postgres_cdc` provider: the row changes of a Postgres publication read
  from the database's write-ahead log through a logical replication slot, each
  one input on the relay's entry channel, after a first copy of the
  published tables.

  Each binding runs one `FlezhaPostgresCdc.Reader`. One slot per binding: not
  shareable, and refused on a partitioned relay, whose members a row's key
  decides between. The slot is kept on every disable, since dropping it loses
  the position. Options, the event, the copy, acknowledgement, health and
  telemetry: `docs/public/plugins/postgres_cdc.md` in the core.
  """

  alias Flezha.Plugin.Provider.Postgres.Connection
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias FlezhaPostgresCdc.Reader

  use ProviderBehaviour,
    name: "postgres_cdc",
    shareable: false,
    source: [partitioned: :refuse],
    telemetry: [lag_bytes: :metric, snapshot_done: :event],
    options:
      Connection.rows() ++
        [
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
              "Longest the read position waits to be confirmed while changes stream, unless the relay's input is durable"
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
          ]
        ] ++
        Options.channel(:in, "change") ++
        Options.plugin_id("the id it was enabled under") ++ Options.failure_threshold(),
    worker: [module: Reader, restart: :temporary]

  # A slot name as Postgres allows it, with room for the copy's `_copy`.
  @slot ~r/\A[a-z0-9_]{1,58}\z/

  @doc "Validate and prepare. Connects to nothing: the reader does, at activation."
  @impl true
  def init_provider(opts) do
    with :ok <- check_names(opts), do: {:ok, Connection.prepare(opts)}
  end

  defp check_names(opts) do
    with :ok <- slot(opts.slot) do
      Connection.check_names([
        {:publication, opts.publication, [opts.publication]},
        {:tables, opts.tables, Enum.flat_map(opts.tables || [], &Connection.table_names/1)}
      ])
    end
  end

  defp slot(slot) do
    if Regex.match?(@slot, slot), do: :ok, else: ProviderBehaviour.refuse_value(:slot, slot)
  end

  # Before the source's shared verdicts: a failed start check or a lost
  # connection names itself, then a slot past `max_lag_bytes` names its lag.
  defp verdict(%{failure: failure}, _opts) when failure != nil, do: {:unhealthy, failure}

  # The log a slot holds back fills the database server's disk: it outranks a
  # refusing relay, which is what usually makes it grow.
  defp verdict(%{lag_bytes: bytes}, opts) when is_integer(bytes) and bytes > opts.max_lag_bytes,
    do:
      {:unhealthy,
       {:postgres_cdc_slot_lag,
        %{slot: opts.slot, lag_bytes: bytes, max_lag_bytes: opts.max_lag_bytes}}}

  @doc "What `flz relay info` shows beneath the plugin: the slot, the publication and its verdict."
  @impl true
  def inspect_state(opts),
    do: %{slot: opts.slot, publication: opts.publication, health: health(opts)}
end
