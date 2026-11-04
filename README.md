# flezha_postgres_cdc

The Postgres change capture plugin for Flezha: a plugin package, `flezha_postgres_cdc`, with one
provider, `postgres_cdc`. It reads a publication's row changes from the database's
write-ahead log through a logical replication slot, with Postgres's built-in `pgoutput` format,
and injects one JSON event per insert, update, delete and truncate on a relay's entry
channel, in commit order, after a first copy of the published tables taken in the slot's own
snapshot. It confirms the read position as the relay's input durability says.

The contract (database setup, options, the event, the copy, acknowledgement, the slot and its
lag, health, telemetry) is the core's page `docs/public/plugins/postgres_cdc.md`; this README covers
the repository.

## Layout

```
lib/flezha_postgres_cdc.ex            the provider: options, refusals, health
lib/flezha_postgres_cdc/reader.ex     one reader per binding: checks, the copy, the stream's life, lag
lib/flezha_postgres_cdc/catalog.ex    the start checks, the slot kept after a copy, the lag query
lib/flezha_postgres_cdc/copy.ex       the first copy, in the slot's exported snapshot
lib/flezha_postgres_cdc/stream.ex     the replication connection: events, batches, the confirmed position
lib/flezha_postgres_cdc/pgoutput.ex   the pgoutput messages and the standby status update
lib/flezha_postgres_cdc/values.ex     a column's text parsed into the driver's term
lib/flezha_postgres_cdc/event.ex      a change as its input, and the hand-over
test/flezha_postgres_cdc/             unit tests from recorded messages, the integration test
                                      against a database, the catalogue and docs checks
test/support/                         the stand-ins for a controller and a relay
test/e2e/                             the e2e runner (an Elixir SDK runner) and run.sh
```

## Building and testing

The project depends on the Flezha core as a path dependency: it is the core's submodule at
`plugins/flezha_postgres_cdc_plugin`, so the core is `../..`. Nothing is published before
go-live. `mix.lock` pins every library the core also ships to the core's version, which the
packaging task checks; the Postgres driver is the core's, pinned to its version.

```
mix deps.get
RUSTLER_SKIP_COMPILATION=true mix compile --warnings-as-errors
RUSTLER_SKIP_COMPILATION=true mix test
RUSTLER_SKIP_COMPILATION=true mix format --check-formatted
```

`mix test` runs with `--no-start`: no controller boots, and the tests start the provider's
worker family and stand in for the relay. The catalogue test fails when a code the provider
reports is missing from the core's `crates/flz_transport/errors.json`, and the docs test when the
option table of `docs/public/plugins/postgres_cdc.md` differs from the provider's declaration,
printing the table to put there.

The integration tests need the core's dev database `postgres-cdc` on `localhost:5434`, which
runs with `wal_level=logical`:

```
docker compose -f ../../docker/dev/docker-compose.yml up -d postgres-cdc
POSTGRES_CDC_INTEGRATION=1 RUSTLER_SKIP_COMPILATION=true mix test --include postgres_cdc_integration
```

## Packaging

On the host, with the same OTP and Elixir as the controller release:

```
MIX_ENV=prod mix deps.get
MIX_ENV=prod RUSTLER_SKIP_COMPILATION=true mix flezha.plugin.package
```

The archive is `_build/flezha_postgres_cdc-0.1.1.fpz`. It bundles no library: the driver is the
release's own.

## Installing into a local release

Install `_build/flezha_postgres_cdc-0.1.1.fpz` on a controller and restart it, as
[Installing plugins](https://github.com/flezha/flezha/blob/main/docs/public/topics/plugins/installing_plugins.md#install-uninstall-and-sync)
describes. Then enable `postgres_cdc` on a relay as its page shows.

## The end-to-end run

`test/e2e/run.sh` checks the package against a controller release built from the core
(`MIX_ENV=prod mix release flezha --path /tmp/flzrel` there, then
`FLEZHA_RELEASE=/tmp/flzrel test/e2e/run.sh`; with no `FLEZHA_RELEASE` it refuses to run) and
the dev database. It first starts the controller without the package and checks the provider
is unknown, then packages the plugin, installs it under a temporary plugin root, starts the
controller and the e2e runner, and checks: the archive holds only the plugin and
it loads; a plain relay receives a copy of 1 000 rows and 1 000 streamed inserts; a copy cut
by a killed controller while the runner is away leaves no slot, runs again, and the relay
holds every row; an effectively-once relay receives every one of 10 000 transactions across
a controller killed while they stream; and 50 changes handed over while the runner is away
reach it once each after the replication connection is cut. It prints PASS or FAIL per check,
keeps its logs when one fails, and exits non-zero on any failure.

## Licence

Apache-2.0, see `LICENSE`.
