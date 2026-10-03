# The suite runs with `--no-start`: no controller boots. What a controller would
# have running for the provider is started here: its family's supervisor, which
# the tests start workers under, and the driver.
alias Flezha.Plugin.Provider.WorkerSup

{:ok, _} = Application.ensure_all_started([:telemetry, :postgrex, :mimic])

{:ok, family} = WorkerSup.start_link(WorkerSup.family(FlezhaPostgresCdc))
Process.unlink(family)

# The integration test answers a server setting it cannot change on the dev database.
Mimic.copy(Postgrex)

# The core's own suite runs at this level: a log line a test provokes on purpose
# is captured and asserted where it is.
Logger.configure(level: :error)

# A database on the dev stack is needed by the integration tests alone. What a
# worker does in answer to a test's message is awaited, not timed: a loaded host
# can take longer than ExUnit's default 100 ms.
ExUnit.start(exclude: [:postgres_cdc_integration], assert_receive_timeout: 2_000)
