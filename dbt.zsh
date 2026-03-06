# dbt helper functions and completions

# --- choose dbt from active venv if present ---
_dbt_bin() {
  if [[ -n "$VIRTUAL_ENV" && -x "$VIRTUAL_ENV/bin/dbt" ]]; then
    echo "$VIRTUAL_ENV/bin/dbt"
  else
    echo "dbt"
  fi
}

# --- shared log formatter (Python, no single quotes) ---
read -r -d '' _DBT_FMT <<'PYEOF'
import sys, json, datetime as dt, select, signal

W = sys.stdout.write
YELLOW = "\033[33m"; GREEN = "\033[32m"; RED = "\033[31m"
DIM = "\033[2m"; RESET = "\033[0m"; CLR = "\033[2K"
SPIN = ["\u28cb","\u28d9","\u28f9","\u28f8","\u28fc","\u28f4","\u28e6","\u28e7","\u28c7","\u28cf"]

def parse_ts(s):
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None

nodes = []
id_idx = {}
total = 0
extra = 0
tick = 0
issues = []
model_tests = {}  # model_name -> [uid, ...]
source_tests = {}  # source_name -> [uid, ...]
source_headers = {}  # source_name -> uid of virtual header node

_source_test_info = {}
try:
    import os
    _mpath = os.path.join("target", "manifest.json")
    if os.path.exists(_mpath):
        with open(_mpath) as _mf:
            _manifest = json.load(_mf)
        for _uid, _node in _manifest.get("nodes", {}).items():
            if _node.get("resource_type") != "test":
                continue
            _deps = _node.get("depends_on", {}).get("nodes", [])
            _src_deps = [d for d in _deps if d.startswith("source.")]
            if not _src_deps:
                continue
            _sp = _src_deps[0].split(".")
            if len(_sp) < 4:
                continue
            _tmeta = _node.get("test_metadata", {})
            _ttype = _tmeta.get("name", "test")
            _col = _tmeta.get("kwargs", {}).get("column_name", "")
            _source_test_info[_uid] = (_sp[2], _ttype, _sp[3], _col)
        del _manifest
except Exception:
    pass

PASS_THROUGH = {"MainReportVersion", "EndOfRunSummary", "LogFreshnessResult",
                "CommandCompleted", "LogSeedResult", "LogSnapshotResult"}
CAPTURE_ISSUES = {"RunResultError", "RunResultFailure", "RunResultWarning"}
STARTED = {"compiling", "executing", "started"}
FINISHED = {"success", "error", "fail", "skipped", "pass", "warn"}
SHOW_TYPES = {"model", "test"}

cw = {"name": 0, "mat": 0, "rel": 0, "dur": 0}

def pad(val, col):
    return val.ljust(cw[col])

INDENT_MODEL = "  "
INDENT_TEST = "      "
INDENT_EXTRA = len(INDENT_TEST) - len(INDENT_MODEL)

def render(m):
    s = m["st"]
    is_test = m.get("rtype") == "test"
    is_source = m.get("rtype") == "source"
    indent = INDENT_TEST if is_test else INDENT_MODEL
    name_w = max(cw["name"] - (INDENT_EXTRA if is_test else 0), len(m["name"]))
    n = m["name"].ljust(name_w)
    mat = pad(m.get("mat", ""), "mat")
    if s == "running":
        if is_source:
            return f"{indent}{DIM}{SPIN[tick % len(SPIN)]} {n}  {mat}{RESET}"
        elapsed = ""
        st = m.get("start_ts")
        if st:
            e = (dt.datetime.now(dt.timezone.utc) - st).total_seconds()
            if e >= 60:
                elapsed = f"{int(e // 60)}m{int(e % 60):02d}s"
            else:
                elapsed = f"{e:.0f}s"
        return f"{indent}{YELLOW}{SPIN[tick % len(SPIN)]} {n}  {mat}  {elapsed}{RESET}"
    dur = pad(m.get("dur", ""), "dur")
    ts = m.get("end_ts", "")
    rel = pad(m.get("rel", ""), "rel")
    cols = f"  {DIM}{mat}  {rel}  {dur}  {ts}{RESET}"
    if s in ("success", "pass"):
        return f"{indent}{GREEN}\u2713 {n}{RESET}{cols}"
    if s in ("error", "fail"):
        return f"{indent}{RED}\u2717 {n}{RESET}{cols}"
    if s == "warn":
        return f"{indent}{YELLOW}! {n}{RESET}{cols}"
    return f"{indent}{DIM}\u25cb {n}  skipped{RESET}"

def update_line(idx):
    up = total + extra - nodes[idx]["line"] - 1
    if up > 0:
        W(f"\033[{up}A")
    W(f"\r{CLR}{render(nodes[idx])}")
    if up > 0:
        W(f"\033[{up}B")
    W("\r")
    sys.stdout.flush()

def rerender_all():
    for i in range(len(nodes)):
        update_line(i)

def grow_col(col, val):
    if len(val) > cw[col]:
        cw[col] = len(val)
        return True
    return False

def tick_spinners():
    global tick
    tick += 1
    for m in nodes:
        if m["st"] == "running":
            update_line(id_idx[m["uid"]])

def info_line(text):
    global extra
    W(text + "\n")
    sys.stdout.flush()
    extra += 1

def process(line):
    global total, extra
    line = line.strip()
    if not line.startswith("{"):
        info_line(line)
        return
    try:
        ev = json.loads(line)
    except Exception:
        info_line(line)
        return
    info = ev.get("info", {})
    data = ev.get("data", {})
    event_name = info.get("name", "")
    ts = parse_ts(info.get("ts"))
    msg = info.get("msg", "")
    if event_name in CAPTURE_ISSUES and msg:
        issues.append((event_name, msg))
    if event_name == "LogTestResult" and msg and "PASS" not in msg:
        issues.append((event_name, msg))
    if event_name in PASS_THROUGH:
        info_line(f"  {DIM}{msg}{RESET}")
        return
    node = data.get("node_info") or {}
    uid = node.get("unique_id") or node.get("node_id")
    rtype = node.get("resource_type")
    name = node.get("node_name") or node.get("name")
    status = (node.get("node_status") or "").lower()
    if not uid or rtype not in SHOW_TYPES or not name:
        return
    parent_name = None
    is_source_test = False
    if rtype == "test":
        max_test_name = 80 - INDENT_EXTRA
        for n in nodes:
            if n.get("rtype") == "model" and n["raw_name"] in (uid or ""):
                parent_name = n["raw_name"]
                break
        if parent_name:
            parts = name.split(parent_name)
            ttype = parts[0].rstrip("_") if parts[0] else name
            rest = parts[1].lstrip("_") if len(parts) > 1 and parts[1] else ""
            label = f"\u21b3 {parent_name}: {ttype}({rest})" if rest else f"\u21b3 {parent_name}: {ttype}"
        else:
            src_info = _source_test_info.get(uid)
            if src_info:
                src, ttype, table, col = src_info
                parent_name = src
                is_source_test = True
                detail = f"{table}.{col}" if col else table
                label = f"\u21b3 {ttype}({detail})" if detail else f"\u21b3 {ttype}"
            else:
                label = f"test: {name}"
        if len(label) > max_test_name:
            label = label[:max_test_name - 1] + "\u2026"
    else:
        label = name
    mat = node.get("materialized", "") if rtype == "model" else rtype
    nr = node.get("node_relation") or {}
    db = nr.get("database") or node.get("database", "")
    schema = nr.get("schema") or node.get("schema", "")
    alias = nr.get("alias") or node.get("alias", "") or name
    rel = ".".join(p for p in [db, schema, alias] if p) if rtype == "model" else ""
    if status in STARTED and uid not in id_idx:
        if is_source_test and parent_name and parent_name not in source_headers:
            src_uid = f"__source__.{parent_name}"
            src_m = {"uid": src_uid, "name": parent_name, "raw_name": parent_name,
                     "rtype": "source", "mat": "source", "st": "running",
                     "start_ts": ts, "line": total}
            grow_col("name", parent_name)
            grow_col("mat", "source")
            source_headers[parent_name] = src_uid
            id_idx[src_uid] = len(nodes)
            nodes.append(src_m)
            if len(nodes) > 1:
                rerender_all()
            W(render(src_m) + "\n")
            sys.stdout.flush()
            total += 1
        m = {"uid": uid, "name": label, "raw_name": name, "rtype": rtype, "start_ts": ts, "st": "running", "line": total}
        if mat:
            m["mat"] = mat
        if is_source_test and parent_name:
            m["parent"] = parent_name
            if parent_name not in source_tests:
                source_tests[parent_name] = []
            source_tests[parent_name].append(uid)
        elif rtype == "test" and parent_name:
            m["parent"] = parent_name
            if parent_name not in model_tests:
                model_tests[parent_name] = []
            model_tests[parent_name].append(uid)
        eff_name = label if rtype != "test" else label + " " * INDENT_EXTRA
        changed = grow_col("name", eff_name) or grow_col("mat", mat)
        id_idx[uid] = len(nodes)
        nodes.append(m)
        if changed and len(nodes) > 1:
            rerender_all()
        W(render(m) + "\n")
        sys.stdout.flush()
        total += 1
    elif status in FINISHED and uid in id_idx:
        idx = id_idx[uid]
        m = nodes[idx]
        m["st"] = status
        if mat:
            m["mat"] = mat
        if rel:
            m["rel"] = rel
        if ts and m.get("start_ts"):
            d = (ts - m["start_ts"]).total_seconds()
            m["dur"] = f"{d:.1f}s"
            m["_dur_raw"] = d
            grow_col("dur", m["dur"])
        if ts:
            m["end_ts"] = ts.strftime("%H:%M:%S")
            m["_end_ts_raw"] = ts
        changed = grow_col("mat", mat) or grow_col("rel", rel)
        if changed:
            rerender_all()
        else:
            update_line(idx)
        parent = m.get("parent")
        if parent and parent in source_headers:
            test_uids = source_tests.get(parent, [])
            if test_uids and all(nodes[id_idx[u]]["st"] in FINISHED for u in test_uids if u in id_idx):
                src_idx = id_idx[source_headers[parent]]
                any_fail = any(nodes[id_idx[u]]["st"] in ("error", "fail") for u in test_uids if u in id_idx)
                nodes[src_idx]["st"] = "fail" if any_fail else "success"
                update_line(src_idx)

def _summarize_tests(group, label_prefix=""):
    for parent, test_uids in group.items():
        test_nodes = [nodes[id_idx[u]] for u in test_uids if u in id_idx]
        if not test_nodes:
            continue
        starts = [t["start_ts"] for t in test_nodes if t.get("start_ts")]
        ends = [t.get("_end_ts_raw") for t in test_nodes if t.get("_end_ts_raw")]
        agg = sum((t.get("_dur_raw", 0) for t in test_nodes), 0)
        wall = ""
        if starts and ends:
            span = (max(ends) - min(starts)).total_seconds()
            wall = f"wall {span:.1f}s"
        ct = len(test_nodes)
        passed = sum(1 for t in test_nodes if t["st"] in ("pass", "success"))
        failed = ct - passed
        agg_s = f"sum {agg:.1f}s"
        status_txt = f"{passed}/{ct} passed"
        if failed:
            status_txt = f"{RED}{status_txt}{RESET}"
        name = f"{label_prefix}{parent}" if label_prefix else parent
        W(f"      {DIM}\u2514\u2500 {name} tests: {status_txt}  {DIM}{agg_s}  {wall}{RESET}\n")
    sys.stdout.flush()

def print_test_summaries():
    if model_tests:
        _summarize_tests(model_tests)
    if source_tests:
        _summarize_tests(source_tests)

def print_issues():
    if not issues:
        return
    rule = "-" * 60
    W(f"\n{RED}{rule}{RESET}\n")
    for evt, msg in issues:
        if "Error" in evt or "Failure" in evt:
            W(f"  {RED}\u2717 {msg}{RESET}\n")
        else:
            W(f"  {YELLOW}! {msg}{RESET}\n")
    W(f"{RED}{rule}{RESET}\n")
    sys.stdout.flush()

def _sigint(sig, frame):
    W("\n")
    print_test_summaries()
    print_issues()
    sys.exit(130)
signal.signal(signal.SIGINT, _sigint)

fd = sys.stdin.fileno()
while True:
    ready, _, _ = select.select([fd], [], [], 0.08)
    if ready:
        line = sys.stdin.readline()
        if not line:
            break
        process(line)
    else:
        if any(m["st"] == "running" for m in nodes):
            tick_spinners()
print_test_summaries()
print_issues()
PYEOF

# --- core helpers ---

# Parse --vars from args, merging with dev_disable: true.
# Sets _dbt_merged_vars and _dbt_remaining_args in the caller's scope.
_dbt_extract_vars() {
  _dbt_remaining_args=()
  local user_vars=""
  local i=1
  while (( i <= $# )); do
    if [[ "${@[$i]}" == "--vars" ]] && (( i < $# )); then
      user_vars="${@[$((i+1))]}"
      (( i += 2 ))
    elif [[ "${@[$i]}" == --vars=* ]]; then
      user_vars="${@[$i]#--vars=}"
      (( i++ ))
    else
      _dbt_remaining_args+=("${@[$i]}")
      (( i++ ))
    fi
  done
  local _default="dev_disable: true"
  if [[ -n "$user_vars" ]]; then
    # Strip surrounding braces from both sides before merging
    local _u="${user_vars#\{}"; _u="${_u%\}}"
    local _d="${_default#\{}"; _d="${_d%\}}"
    _dbt_merged_vars="{${_d}, ${_u}}"
  else
    _dbt_merged_vars="{${_default}}"
  fi
}

# Run a dbt subcommand with standard deferral flags
_dbt_deferred() {
  local dbt_bin="$(_dbt_bin)"
  local -a _dbt_remaining_args
  local _dbt_merged_vars
  _dbt_extract_vars "$@"
  echo "Running: dbt ${_dbt_remaining_args[*]} --vars '${_dbt_merged_vars}' --defer --state deferral"
  "$dbt_bin" "${_dbt_remaining_args[@]}" --vars "$_dbt_merged_vars" --defer --state deferral
}

# Run a dbt subcommand with deferral flags + pretty log formatter
_dbt_deferred_pretty() {
  local dbt_bin="$(_dbt_bin)"
  local -a _dbt_remaining_args
  local _dbt_merged_vars
  _dbt_extract_vars "$@"
  echo "Running: dbt ${_dbt_remaining_args[*]} --vars '${_dbt_merged_vars}' --defer --state deferral (pretty)"
  setopt localoptions localtraps nomonitor
  local fifo=$(mktemp -u "${TMPDIR:-/tmp}/dbt-fmt.XXXXXX")
  mkfifo "$fifo"
  "$dbt_bin" "${_dbt_remaining_args[@]}" --vars "$_dbt_merged_vars" --defer --state deferral --log-format json >"$fifo" 2>&1 &
  local dbt_pid=$!
  trap "kill $dbt_pid 2>/dev/null; wait $dbt_pid 2>/dev/null; rm -f $fifo; trap - INT; return 130" INT
  python3 -u -c "$_DBT_FMT" < "$fifo"
  local rc=$?
  wait $dbt_pid 2>/dev/null
  rm -f "$fifo"
  trap - INT
  return $rc
}

# --- user-facing commands ---

# dbtls: search dbt ls output. Takes a selector as the first arg and search pattern as the second
dbtls() {
  if (( $# < 2 )); then
    echo "Usage: dbtls <dbt selector> <rg pattern...>"
    return 1
  fi
  local selector="$1"; shift
  local dbt_bin="$(_dbt_bin)"
  "$dbt_bin" ls -s "$selector" --resource-type model --output name --quiet 2>/dev/null \
    | rg --color=always "$@"
}

dbd()   { _dbt_deferred build -s "$@"; }
dbdp()  { _dbt_deferred_pretty build -s "$@"; }
dbr()   { _dbt_deferred run -s "$@"; }
dbrp()  { _dbt_deferred_pretty run -s "$@"; }
dbdf()  { _dbt_deferred build -s "$@" --full-refresh; }
dbdfp() { _dbt_deferred_pretty build -s "$@" --full-refresh; }

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

# --- sqlfluff wrappers ---

# Resolve a dbt model name to its .sql file path under models/
_dbt_model_to_file() {
  local model="$1"
  if [[ -z "$model" ]]; then
    echo "Error: no model name provided" >&2
    return 1
  fi
  # If it's already a file path, use it directly
  if [[ "$model" == *.sql ]]; then
    echo "$model"
    return 0
  fi
  local -a matches
  matches=(models/**/${model}.sql(N))
  if (( ${#matches} == 0 )); then
    echo "Error: no file found for model '$model'" >&2
    return 1
  fi
  if (( ${#matches} > 1 )); then
    echo "Warning: multiple files found for '$model', using first match" >&2
  fi
  echo "${matches[1]}"
}

# sqlfluff lint on one or more dbt models (by name)
dbl() {
  if (( $# < 1 )); then
    echo "Usage: dbl <model> [model ...]"
    return 1
  fi
  local -a files
  for model in "$@"; do
    local f
    f="$(_dbt_model_to_file "$model")" || return 1
    files+=("$f")
  done
  echo "Running: sqlfluff lint ${files[*]}"
  sqlfluff lint "${files[@]}"
}

# sqlfluff fix on one or more dbt models (by name)
dbf() {
  if (( $# < 1 )); then
    echo "Usage: dbf <model> [model ...]"
    return 1
  fi
  local -a files
  for model in "$@"; do
    local f
    f="$(_dbt_model_to_file "$model")" || return 1
    files+=("$f")
  done
  echo "Running: sqlfluff fix ${files[*]}"
  sqlfluff fix "${files[@]}"
}

# Completion for sqlfluff wrappers — model names only, no graph operators
_dbl_complete() {
  _dbt_fs_refresh_models
  compadd -- $_DBT_FS_MODEL_CACHE
}

compdef _dbl_complete dbl
compdef _dbl_complete dbf

compdef _dbd_complete dbd
compdef _dbd_complete dbdp
compdef _dbd_complete dbr
compdef _dbd_complete dbrp
compdef _dbd_complete dbdf
compdef _dbd_complete dbdfp
