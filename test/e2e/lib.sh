# What run.sh uses: the plugin e2e library, and the database helpers. Sourced,
# never run.
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PLUGIN=$(cd "$HERE/../.." && pwd)
CORE=$(cd "$PLUGIN/../.." && pwd)
PACKAGE=flezha_postgres_cdc
TMP_PREFIX=flzcdc
# shellcheck source=../../../../test/helpers/plugin_e2e.sh
. "$CORE/test/helpers/plugin_e2e.sh"

# The dev database the plugin reads, as the controller reaches it, and its container.
DB_PORT=${POSTGRES_CDC_E2E_PORT:-5434}
DB_CONTAINER=flezha_postgres_cdc

# The file output plugin the runner's outputs are written by, and the password the plugin reads.
plugin_env() {
  export FLEZHA_FILE_PLUGIN_ROOT=$1/files
  export CDC_E2E_PASSWORD=flezha_test
  mkdir -p "$1/files"
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

# `write_outputs <relay>`: a file output plugin writing the relay's outputs to `<relay>.out`.
write_outputs() {
  flz plugin enable "$1_out" relay_file_output "Xpath=$1.out" >>"$TMP/out/enable-$1_out.log" 2>&1 &&
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
