# dbt helper functions and completions

# --- choose dbt from active venv if present ---
_dbt_bin() {
  if [[ -n "$VIRTUAL_ENV" && -x "$VIRTUAL_ENV/bin/dbt" ]]; then
    echo "$VIRTUAL_ENV/bin/dbt"
  else
    echo "dbt"
  fi
}

# dbtls = "Search dbt ls for a specific output. Takes a selector as the first arg and search pattern as the second arg"
dbtls() {
  if (( $# < 2 )); then
    echo "Usage: dbtls <dbt selector> <rg pattern...>"
    return 1
  fi

  local selector="$1"
  shift

  local dbt_bin="$(_dbt_bin)"
  "$dbt_bin" ls -s "$selector" --resource-type model --output name --quiet 2>/dev/null \
    | rg --color=always "$@"
}

# dbt build with deferral. Args are passed as selectors
dbd() {
  local dbt_bin="$(_dbt_bin)"
  echo "Running: dbt build -s $* --vars 'dev_disable: true' --defer --state deferral"
  "$dbt_bin" build -s "$@" --vars 'dev_disable: true' --defer --state deferral
}

# dbt build with deferral + log formatting. Args are passed as selectors
dbdp() {
  local dbt_bin="$(_dbt_bin)"
  echo "Running: dbt build -s $* --vars 'dev_disable: true' --defer --state deferral --log-format json (piped)"
  setopt localoptions pipefail
  "$dbt_bin" build -s "$@" --vars 'dev_disable: true' --defer --state deferral --log-format json 2>&1 | python3 -u -c '
import sys, json, datetime as dt

def parse_ts(s):
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None

starts = {}
active = {}

def alloc_slot():
    s = 1
    while s in active:
        s += 1
    return s

def fmt_time(ts):
    return ts.strftime("%H:%M:%S") if ts else "        "

PASS_THROUGH = {"MainReportVersion", "EndOfRunSummary", "LogFreshnessResult",
                "RunResultError", "RunResultWarning", "RunResultFailure",
                "CommandCompleted", "LogSeedResult", "LogSnapshotResult"}
STARTED = {"compiling", "executing", "started"}
FINISHED = {"success", "error", "fail", "skipped"}
seen_started = set()

for line in sys.stdin:
    line = line.strip()
    if not line.startswith("{"):
        print(line)
        sys.stdout.flush()
        continue
    try:
        ev = json.loads(line)
    except Exception:
        print(line)
        sys.stdout.flush()
        continue

    info = ev.get("info", {})
    data = ev.get("data", {})
    event_name = info.get("name", "")
    ts = parse_ts(info.get("ts"))
    msg = info.get("msg", "")

    if event_name in PASS_THROUGH:
        print(f"{fmt_time(ts)}  {msg}")
        sys.stdout.flush()
        continue

    node = data.get("node_info") or {}
    unique_id = node.get("unique_id") or node.get("node_id")
    rtype = node.get("resource_type")
    name = node.get("node_name") or node.get("name")
    status = (node.get("node_status") or "").lower()

    if not unique_id or rtype != "model" or not name:
        continue

    if status in STARTED and unique_id not in seen_started:
        seen_started.add(unique_id)
        slot = alloc_slot()
        active[slot] = unique_id
        starts[unique_id] = (ts, slot)
        print(f"{fmt_time(ts)}  [{slot}/{len(active)}]  ▶ START  {name}")
        sys.stdout.flush()

    elif status in FINISHED and unique_id in seen_started:
        seen_started.discard(unique_id)
        start_ts, slot = starts.pop(unique_id, (None, None))
        if slot in active:
            active.pop(slot, None)
        duration = (ts - start_ts).total_seconds() if ts and start_ts else None
        dur_str = f"{duration:.2f}s" if duration is not None else "?"
        icon = "✓" if status == "success" else "✗"
        label = status.upper()
        print(f"{fmt_time(ts)}  [{slot or chr(63)}->{len(active)}]  {icon} {label:7s} {name}  ({dur_str})")
        sys.stdout.flush()
'
}

# dbt run with deferral. Args are passed as selectors
dbr() {
  local dbt_bin="$(_dbt_bin)"
  echo "Running: dbt run -s $* --vars 'dev_disable: true' --defer --state deferral"
  "$dbt_bin" run -s "$@" --vars 'dev_disable: true' --defer --state deferral
}

# dbt build full-refresh with deferral. Args are passed as selectors
dbdf() {
  local dbt_bin="$(_dbt_bin)"
  echo "Running: dbt build -s $* --vars 'dev_disable: true' --defer --state deferral --full-refresh"
  "$dbt_bin" build -s "$@" --vars 'dev_disable: true' --full-refresh --defer --state deferral
}

# ---- fast model-name completion from filesystem (zsh) ----
typeset -ga _DBT_FS_MODEL_CACHE
typeset -gi _DBT_FS_MODEL_CACHE_TS=0
typeset -gi _DBT_FS_MODEL_CACHE_TTL=5  # seconds

_dbt_fs_refresh_models() {
  local now=$EPOCHSECONDS
  if (( now - _DBT_FS_MODEL_CACHE_TS < _DBT_FS_MODEL_CACHE_TTL )) && (( ${#_DBT_FS_MODEL_CACHE} > 0 )); then
    return
  fi

  _DBT_FS_MODEL_CACHE=()

  # Only build cache if models/ exists (adjust if your dbt project uses a different folder)
  if [[ -d models ]]; then
    # models/**/*.sql(N) -> recursive glob; (N) makes it expand to nothing if no matches
    local -a files
    files=(models/**/*.sql(N))

    # :t = tail (basename), :r = root (strip extension)
    _DBT_FS_MODEL_CACHE=(${${files:t}:r})
    # unique + stable sort
    _DBT_FS_MODEL_CACHE=(${(ou)_DBT_FS_MODEL_CACHE})
  fi

  _DBT_FS_MODEL_CACHE_TS=$now
}

_dbd_complete() {
  _dbt_fs_refresh_models

  local cur="${words[CURRENT]}"

  # key:value selector syntax — offer selector prefixes only
  if [[ "$cur" == *:* ]]; then
    compadd -- 'tag:' 'path:' 'model:' 'source:' 'snapshot:' 'seed:' 'test:' 'exposure:' 'metric:'
    return
  fi

  # Strip graph operator prefix (e.g. "+", "2+") to complete the model name part
  local graph_prefix=""
  if [[ "$cur" =~ '^([0-9]*\+)' ]]; then
    graph_prefix="${match[1]}"
  fi

  if [[ -n "$graph_prefix" ]]; then
    # Offer model names with the graph prefix prepended so zsh matches correctly
    local -a prefixed
    prefixed=("${_DBT_FS_MODEL_CACHE[@]/#/${graph_prefix}}")
    compadd -- $prefixed
  else
    # Offer selector prefixes and bare model names
    compadd -- 'tag:' 'path:' 'model:' 'source:' 'snapshot:' 'seed:' 'test:' 'exposure:' 'metric:' '+'
    compadd -- $_DBT_FS_MODEL_CACHE
  fi
}

compdef _dbd_complete dbd
compdef _dbd_complete dbdp
compdef _dbd_complete dbdf
compdef _dbd_complete dbr
