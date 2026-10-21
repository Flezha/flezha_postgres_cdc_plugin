defmodule FlezhaPostgresCdc.CatalogueTest do
  @moduledoc """
  The refusals and failures the provider reports are codes of the core's one
  catalogue. Built against a core that lacks one, this suite fails, not a
  user's enable or a health verdict.
  """
  use ExUnit.Case, async: true

  @catalogue Mix.Project.deps_paths()[:flezha]
             |> Path.join("crates/flz_transport/errors.json")
             |> File.read!()
             |> Jason.decode!()

  test "every reason the provider gives maps to its code" do
    for {reason, code} <- [
          {{:postgres_cdc_wal_level_not_logical, %{wal_level: "replica"}},
           "postgres_cdc.wal_level_not_logical"},
          {{:postgres_cdc_server_too_old, %{version: "13.4"}}, "postgres_cdc.server_too_old"},
          {{:postgres_cdc_publication_missing, %{publication: "p"}},
           "postgres_cdc.publication_missing"},
          {{:postgres_cdc_replica_identity_missing, %{table: "public.t"}},
           "postgres_cdc.replica_identity_missing"},
          {{:postgres_cdc_slot_lag, %{slot: "s", lag_bytes: 2, max_lag_bytes: 1}},
           "postgres_cdc.slot_lag"},
          {{:postgres_cdc_replication_failed, %{where: "slot=s h:5432/d", reason: "closed"}},
           "postgres_cdc.replication_failed"}
        ] do
      assert Map.has_key?(@catalogue["codes"], code), "#{code} is not in the catalogue"
      assert %Flezha.Error{code: ^code} = Flezha.Error.from_reason(reason, nil)
    end
  end
end
