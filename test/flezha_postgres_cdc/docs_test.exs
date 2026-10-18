defmodule FlezhaPostgresCdc.DocsTest do
  use ExUnit.Case, async: true

  alias Flezha.Plugin.ProviderBehaviour.Options

  @page Mix.Project.deps_paths()[:flezha] |> Path.join("docs/public/plugins/postgres_cdc.md")

  test "the core's postgres_cdc.md carries the option table the provider renders" do
    rendered = Options.render_options_table(FlezhaPostgresCdc)

    documented =
      @page
      |> File.read!()
      |> String.split("<!-- provider-options:begin postgres_cdc -->\n")
      |> Enum.at(1, "")
      |> String.split("<!-- provider-options:end -->")
      |> hd()

    assert documented == rendered,
           "postgres_cdc.md's option table has drifted from its declaration.\n" <>
             "Replace the block between its provider-options markers with:\n\n" <> rendered
  end
end
