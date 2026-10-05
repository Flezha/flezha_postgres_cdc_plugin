#!/usr/bin/env bash
# The Postgres change capture package against a real controller release and the
# core's dev database: it packages the plugin, installs it into a release under a
# temporary plugin root, starts the controller and the e2e runner, and
# checks what a user would see. PASS or FAIL per check; exits non-zero on any failure.
#
#   docker compose -f ../../docker/dev/docker-compose.yml up -d postgres-cdc   # in the core
#   MIX_ENV=prod mix release flezha --path /tmp/flzrel                         # in the core
#   FLEZHA_RELEASE=/tmp/flzrel test/e2e/run.sh
set -uo pipefail
. "$(dirname "$0")/lib.sh"

cleanup() {
  local code=$?
  stop_runner runner
  stop_controller
  drop_run
  if [ "$FAILED" -eq 0 ] && [ "$code" -eq 0 ]; then rm -rf "$TMP"; else say "logs kept in $TMP/out"; fi
}
trap cleanup EXIT

controller_env "$TMP/c1" "flzcdc$RUN" "$REL"
say_release "$REL"
until_ok 60 "sql 'SELECT 1'" || { say "the dev database does not answer"; exit 1; }

# ---------- without the package ----------

check_z() {
  local out
  out=$(flz plugin enable nope postgres_cdc Xslot=nope Xpublication=nope 2>&1)
  if [ $? -ne 0 ] && grep -q "postgres_cdc" <<<"$out" && ! ls "$REL/lib" | grep -q '^flezha_postgres_cdc'; then
    pass z "without the package the provider is unknown: $(grep -m1 -i error <<<"$out")"
  else
    fail z "enable without the package answered: $out"
  fi
}

say "starting the controller without the package"
(cd "$FLEZHA_CTL_DIR" && nohup flzvm start </dev/null >>"$TMP/out/controller-bare.log" 2>&1 &)
until_ok 90 "flz controller status" || { say "the controller did not come up"; exit 1; }
check_z
stop_controller

# ---------- with it ----------

package_plugin
install_plugin
say "starting the controller with the package"
start_controller
build_runner
start_runner runner default

check_a() {
  local lib
  lib=$(tar -tzf "$TMP/flezha_postgres_cdc-0.1.0.fpz" | grep '^lib/' | cut -d/ -f2 | sort -u | tr '\n' ' ')
  if [ "$lib" = "flezha_postgres_cdc-0.1.0 " ] && flz plugin list --installed | grep -q 'flezha_postgres_cdc.* loaded'; then
    pass a "the package loaded; its archive's lib/ holds only $lib"
  else
    fail a "archive lib/: $lib; $(flz plugin list --installed 2>&1 | tail -2)"
  fi
}

check_b() {
  local t=e2e_plain_$RUN
  table "$t" 1000 || { fail b "no table"; return; }
  start_relay echo plain
  sink plain
  cdc "$t" "$t" plain
  await_ids plain 1000 || { fail b "the copy wrote $(ids plain) of 1000"; return; }
  sql "INSERT INTO $t SELECT g, 'new' FROM generate_series(1001, 2000) g" >/dev/null
  if await_ids plain 2000; then
    pass b "a plain relay received the copy of 1000 rows and 1000 streamed inserts: $(ids plain) events"
  else
    fail b "plain wrote $(ids plain) of 2000"
  fi
}

# A copy cut short by a killed controller while the runner is away, then copied again.
check_c() {
  local t=e2e_copy_$RUN seen
  table "$t" 20000 || { fail c "no table"; return; }
  start_relay count_once copied
  sink copied
  stop_runner runner
  cdc "$t" "$t" copied Xsnapshot_batch=200
  until_ok 30 "active ${t}_copy" || { fail c "the copy did not start"; return; }
  say "killing the controller during the copy"
  kill_controller
  until_ok 30 "[ -z \"\$(sql \"SELECT 1 FROM pg_replication_slots WHERE slot_name LIKE '$t%'\")\" ]" ||
    { fail c "a slot outlived the cut copy"; return; }
  start_controller
  start_runner runner default
  if await_ids copied 20000 300 && until_ok 30 "active $t"; then
    seen=$(most_seen copied)
    pass c "a copy cut by a controller kill left no slot, ran again, and the relay holds all 20000 rows, each seen at most $seen times"
  else
    fail c "copied wrote $(ids copied) of 20000; slots: $(sql "SELECT slot_name, active FROM pg_replication_slots WHERE slot_name LIKE '$t%'")"
  fi
}

# 10 000 transactions with the controller killed while they stream.
check_d() {
  local t=e2e_kill_$RUN
  table "$t" 0 || { fail d "no table"; return; }
  start_relay count_once killed
  sink killed
  cdc "$t" "$t" killed Xsnapshot=never
  until_ok 30 "active $t" || { fail d "the plugin did not stream"; return; }
  (for i in $(seq 1 10000); do echo "INSERT INTO $t VALUES ($i, 'v');"; done |
    docker exec -i "$DB_CONTAINER" psql -U flezha -d flezha_test -q >/dev/null 2>&1) &
  local writer=$!
  await_ids killed 100 || { fail d "nothing reached the relay"; return; }
  say "killing the controller while changes stream"
  kill_controller
  start_controller
  wait "$writer"
  if await_ids killed 10000 300; then
    pass d "an effectively-once relay lost nothing across a controller kill: 10000 transactions, each seen at most $(most_seen killed) times"
  else
    fail d "killed wrote $(ids killed) of 10000"
  fi
}

# 50 changes handed over while the runner is away, the replication connection cut.
check_e() {
  local t=e2e_again_$RUN behind
  table "$t" 0 || { fail e "no table"; return; }
  start_relay count_once again
  sink again
  cdc "$t" "$t" again Xsnapshot=never
  until_ok 30 "active $t" || { fail e "the plugin did not stream"; return; }
  stop_runner runner
  sql "INSERT INTO $t SELECT g, 'v' FROM generate_series(1, 50) g" >/dev/null
  sleep 2
  behind=$(sql "SELECT confirmed_flush_lsn < pg_current_wal_lsn() FROM pg_replication_slots WHERE slot_name = '$t'")
  sql "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '$t'" >/dev/null
  until_ok 60 "active $t" || { fail e "the plugin did not reconnect"; return; }
  start_runner runner default
  if await_ids again 50 120 && [ "$(most_seen again)" = 1 ]; then
    pass e "50 changes handed over before the connection was cut (position behind at the cut: $behind) reached the relay once each"
  else
    fail e "again wrote $(ids again) of 50, one seen $(most_seen again) times"
  fi
}

check_a
check_b
check_c
check_d
check_e

[ "$FAILED" -eq 0 ] && say "all checks passed"
exit "$FAILED"
