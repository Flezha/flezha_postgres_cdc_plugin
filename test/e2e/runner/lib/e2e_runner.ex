defmodule E2eRunner do
  @moduledoc """
  The relays the end-to-end run binds to the Postgres change capture plugin.
  Each writes one line per event on the sink `out`: the event's identity, a
  copied row's table and id or a change's LSN and position.

    * `echo` (flow, at least once): the identity alone.
    * `count_once` (flow, effectively once): the identity and how many times
      the relay has seen it, so a change it took twice shows a 2.
  """

  @doc "An event's identity: `r/<schema.table>/<id>` for a copied row, `<lsn>/<seq>` otherwise."
  def identity(input) do
    case Jason.decode!(input) do
      %{"op" => "r"} = e -> "r/#{e["schema"]}.#{e["table"]}/#{e["after"]["id"]}"
      e -> "#{e["lsn"]}/#{e["seq"]}"
    end
  end

  defmodule Echo do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "echo", input: "in", sink: "out"

    def run(input, state, ctx) do
      emit(ctx, E2eRunner.identity(input))
      {:ok, state}
    end
  end

  defmodule CountTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "count_once_tasklet"

    @impl true
    def init_state(_key), do: %{}

    # A keyed sink refuses a plain `emit`; the identity is the output's key.
    @impl true
    def run(input, seen, ctx) do
      id = E2eRunner.identity(input)
      seen = Map.update(seen, id, 1, &(&1 + 1))
      :ok = emit_keyed(ctx, id, "#{id} #{seen[id]}")
      {:ok, seen}
    end
  end

  # A plugin-bound channel of an effectively-once relay is keyed: its outputs
  # carry their key as `flz.key`.
  defmodule CountOnce do
    @moduledoc false
    use Flezha.SDK.Relay, factory_id: "count_once", run: {:flow, exactly: :effectively_once}

    @impl true
    def channels(c), do: c |> input("in") |> into(E2eRunner.CountTask) |> keyed() |> sink("out")
  end
end
