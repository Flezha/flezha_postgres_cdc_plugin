# What run.sh uses: the paths, a controller's environment, the package, the runner
# and the database helpers. Sourced, never run.
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PLUGIN=$(cd "$HERE/../.." && pwd)
CORE=$(cd "$PLUGIN/../.." && pwd)
# Never a default: a release someone keeps for their own use must not be run from here.
REL=${FLEZHA_RELEASE:?set FLEZHA_RELEASE to a controller release built for this run, e.g. mix release flezha --path /tmp/flzrel}
COMPOSE=(docker compose -f "$CORE/docker/dev/docker-compose.yml")
# The dev database the plugin reads, as the controller reaches it, and its container.
DB_PORT=${POSTGRES_CDC_E2E_PORT:-5434}
DB_CONTAINER=flezha_postgres_cdc
# A Unix socket path must fit in 108 bytes, so the directory stays short.
TMP=$(mktemp -d /tmp/flzcdc.XXXX)
RUN=$(date +%s)
FAILED=0
mkdir -p "$TMP/out"

free_port() { python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }

say() { echo "[e2e] $*"; }
pass() { echo "PASS ($1) $2"; }
fail() { echo "FAIL ($1) $2"; FAILED=1; }

# Poll `command` every 0.5 s for up to `seconds`.
until_ok() {
  local n=$(($1 * 2))
  shift
  for _ in $(seq "$n"); do eval "$*" >/dev/null 2>&1 && return 0; sleep 0.5; done
  return 1
}

# `controller_env <dir> <node> <release>`: the environment `flz` and `flzvm` of
# one controller run in, its node name, state, plugin root, sockets and ports all
# its own under <dir>; authorization off.
controller_env() {
  export PATH=$3/bin:$PATH
  export FLEZHA_CTL_DIR=$1/ctl
  export FLEZHA_SOCKET_FILES_PATH=$1/s
  export FLEZHA_NODE_NAME=$2
  export FLEZHA_STATE_DIR=$1/state
  export FLEZHA_PLUGIN_ROOT=$1/plugins
  export FLEZHA_AUTHZ_ENABLED=false
  # The file sink the runner's outputs are written by, and the password the plugin reads.
  export FLEZHA_FILE_PLUGIN_ROOT=$1/files
  export CDC_E2E_PASSWORD=flezha_test
  FLEZHA_HEALTHZ_PORT=$(free_port) FLEZHA_PROMETHEUS_PORT=$(free_port) FLEZHA_TESTNET_WS_PORT=$(free_port)
  FLEZHA_QUIC_LISTEN_ADDRESS=127.0.0.1:$(free_port)
  export FLEZHA_HEALTHZ_PORT FLEZHA_PROMETHEUS_PORT FLEZHA_TESTNET_WS_PORT FLEZHA_QUIC_LISTEN_ADDRESS
  mkdir -p "$1/ctl" "$1/s" "$1/files"
}

# The release a run uses, and when it was built: it must be built from the core
# checkout as it is now, or every check reads an older controller.
say_release() {
  say "release $1, built $(date -r "$1/bin/flzvm" '+%Y-%m-%d %H:%M:%S')"
}

package_plugin() {
  say "packaging the plugin"
  (cd "$PLUGIN" && MIX_ENV=prod RUSTLER_SKIP_COMPILATION=true mix flezha.plugin.package --output "$TMP") \
    >"$TMP/out/package.log" 2>&1 || { cat "$TMP/out/package.log"; exit 1; }
}

install_plugin() {
  (cd "$FLEZHA_CTL_DIR" && flz plugin install "$TMP/flezha_postgres_cdc-0.1.0.fpz") >>"$TMP/out/install.log" 2>&1 ||
    { cat "$TMP/out/install.log"; exit 1; }
}

start_controller() {
  (cd "$FLEZHA_CTL_DIR" && nohup flzvm start </dev/null >>"$TMP/out/controller-$FLEZHA_NODE_NAME.log" 2>&1 &)
  until_ok 90 "flz controller status" || { say "controller $FLEZHA_NODE_NAME did not come up"; exit 1; }
  until_ok 30 "flz plugin list --installed | grep -q ' loaded'" || { say "the package did not load"; exit 1; }
}

stop_controller() {
  (cd "$FLEZHA_CTL_DIR" && flzvm stop >/dev/null 2>&1)
  until_ok 30 "! pgrep -f 'beam.smp.* $FLEZHA_NODE_NAME@'" || pkill -KILL -f "beam.smp.* $FLEZHA_NODE_NAME@"
}

build_runner() {
  (cd "$HERE/runner" && mix deps.get && mix compile) >"$TMP/out/runner-build.log" 2>&1 ||
    { cat "$TMP/out/runner-build.log"; exit 1; }
}

# `start_runner <name> <namespace>`: the e2e runner as a runner of the
# controller whose environment is set. Its standard input stays open until
# `stop_runner`, whose closing it reads as its stop.
start_runner() {
  mkfifo "$TMP/$1.in"
  (sleep infinity >"$TMP/$1.in" & echo $! >"$TMP/$1.hold")
  (cd "$HERE/runner" && FLEZHA_RUNNER_ID_PATH="$TMP/runner-id-$1" \
    setsid mix run --no-compile --no-deps-check --no-halt -- "$2" <"$TMP/$1.in" >"$TMP/out/$1.log" 2>&1 &
    echo $! >"$TMP/$1.pid")
}

stop_runner() {
  [ -f "$TMP/$1.pid" ] || return 0
  kill "$(cat "$TMP/$1.hold")" 2>/dev/null
  local pid
  pid=$(cat "$TMP/$1.pid")
  until_ok 10 "! kill -0 $pid" || kill -KILL -- "-$pid" 2>/dev/null
}

# `start_relay <factory> <id>`, retried until the runner serves the factory.
start_relay() {
  for _ in $(seq 60); do
    flz relay start "$1" --id "$2" >>"$TMP/out/start-$2.log" 2>&1 && return 0
    sleep 1
  done
  say "$2 did not start: $(tail -3 "$TMP/out/start-$2.log")"
  exit 1
}

# `sql <statement>`: run it on the dev database, answering rows unaligned.
sql() { docker exec -i "$DB_CONTAINER" psql -U flezha -d flezha_test -tAq -v ON_ERROR_STOP=1 -c "$1"; }

# `table <name> <rows>`: a published table of `rows` rows, its publication `<name>_pub`.
table() {
  sql "CREATE TABLE $1 (id int PRIMARY KEY, v text)" >/dev/null &&
    sql "INSERT INTO $1 SELECT g, 'v' || g FROM generate_series(1, $2) g" >/dev/null &&
    sql "CREATE PUBLICATION $1_pub FOR TABLE $1" >/dev/null
}

# `cdc <name> <table> <relay> [X<key>=<value>...]`: a postgres_cdc plugin reading the
# table's publication through the slot `<name>` onto the relay's entry channel `in`.
cdc() {
  local name=$1 table=$2 op=$3
  shift 3
  flz plugin enable "$name" postgres_cdc Xhostname=localhost "Xport=$DB_PORT" Xdatabase=flezha_test \
    Xusername=flezha Xpassword_env=CDC_E2E_PASSWORD "Xslot=$name" "Xpublication=${table}_pub" \
    Xchannel=in "$@" >>"$TMP/out/enable-$name.log" 2>&1 &&
    flz plugin bind "$name" --relay_id "$op" --hook=on_terminate >>"$TMP/out/bind-$name.log" 2>&1 ||
    { cat "$TMP/out/enable-$name.log" "$TMP/out/bind-$name.log"; exit 1; }
}

# `sink <relay>`: a file sink writing the relay's outputs to `<relay>.out`.
sink() {
  flz plugin enable "$1_out" relay_file_sink "Xpath=$1.out" >>"$TMP/out/enable-$1_out.log" 2>&1 &&
    flz plugin bind "$1_out" --relay_id "$1" --hook=on_output >>"$TMP/out/bind-$1_out.log" 2>&1 ||
    { cat "$TMP/out/enable-$1_out.log" "$TMP/out/bind-$1_out.log"; exit 1; }
}

# The distinct identities the relay wrote, and the most times it saw any one.
ids() { cut -d' ' -f1 "$FLEZHA_FILE_PLUGIN_ROOT/$1.out" 2>/dev/null | sort -u | wc -l; }
most_seen() { cut -d' ' -f2 "$FLEZHA_FILE_PLUGIN_ROOT/$1.out" 2>/dev/null | sort -n | tail -1; }

# `await_ids <relay> <n> [seconds]`: wait until the relay wrote `n` identities.
await_ids() { until_ok "${3:-60}" "[ \"\$(ids $1)\" -ge $2 ]"; }

# `active <slot>`: the slot is held by a replication connection.
active() { [ "$(sql "SELECT active FROM pg_replication_slots WHERE slot_name = '$1'")" = t ]; }

kill_controller() { pkill -KILL -f "beam.smp.* $FLEZHA_NODE_NAME@"; until_ok 30 "! pgrep -f 'beam.smp.* $FLEZHA_NODE_NAME@'"; }

# Every slot, publication and table of this run.
drop_run() {
  sql "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '%\_$RUN' AND NOT active" >/dev/null 2>&1
  for t in $(sql "SELECT tablename FROM pg_tables WHERE tablename LIKE 'e2e\_%\_$RUN'"); do
    sql "DROP PUBLICATION IF EXISTS ${t}_pub" >/dev/null 2>&1
    sql "DROP TABLE IF EXISTS $t" >/dev/null 2>&1
  done
}
