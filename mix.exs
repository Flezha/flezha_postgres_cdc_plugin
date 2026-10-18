defmodule FlezhaPostgresCdc.MixProject do
  use Mix.Project

  # The Postgres change capture plugin package, packaged with
  # `mix flezha.plugin.package` and loaded by a Flezha controller at boot.
  # `flezha` is the core this package is a submodule of, two directories up,
  # until it is published.
  def project do
    [
      app: :flezha_postgres_cdc,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: [test: "test --no-start"],
      # The end-to-end script is run by hand against a release.
      test_ignore_filters: [&String.starts_with?(&1, "test/e2e/")],
      package: [licenses: ["Apache-2.0"]]
    ]
  end

  # No `mod`: the provider's worker is started under its family by the
  # controller.
  def application do
    [env: [flezha_providers: %{"postgres_cdc" => FlezhaPostgresCdc}]]
  end

  # The core's shared test helpers it uses: a dependency compiles its `lib`
  # alone, and Mix takes a source outside the project only by absolute path.
  defp elixirc_paths(:test) do
    core_helpers = ~w(message_test_helper node_test_helper telemetry)

    [
      "lib",
      "test/support"
      | Enum.map(core_helpers, &Path.expand("../../test/helpers/#{&1}.ex", __DIR__))
    ]
  end

  defp elixirc_paths(_env), do: ["lib"]

  # The driver is the one the controller release ships, pinned to its version,
  # so the archive carries no library but this one.
  defp deps do
    [
      {:flezha, path: "../.."},
      {:postgrex, "0.22.4"}
    ]
  end
end
