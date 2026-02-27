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
import sys, json, datetime as dt, select

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
    indent = INDENT_TEST if is_test else INDENT_MODEL
    name_w = max(cw["name"] - (INDENT_EXTRA if is_test else 0), len(m["name"]))
    n = m["name"].ljust(name_w)
    mat = pad(m.get("mat", ""), "mat")
    if s == "running":
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
        m = {"uid": uid, "name": label, "raw_name": name, "rtype": rtype, "start_ts": ts, "st": "running", "line": total}
        if mat:
            m["mat"] = mat
        if rtype == "test" and parent_name:
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

def print_test_summaries():
    if not model_tests:
        return
    for parent, test_uids in model_tests.items():
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
        W(f"      {DIM}\u2514\u2500 {parent} tests: {status_txt}  {DIM}{agg_s}  {wall}{RESET}\n")
    sys.stdout.flush()

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

# Run a dbt subcommand with standard deferral flags
_dbt_deferred() {
  local dbt_bin="$(_dbt_bin)"
  echo "Running: dbt $* --vars 'dev_disable: true' --defer --state deferral"
  "$dbt_bin" "$@" --vars 'dev_disable: true' --defer --state deferral
}

# Run a dbt subcommand with deferral flags + pretty log formatter
_dbt_deferred_pretty() {
  local dbt_bin="$(_dbt_bin)"
  echo "Running: dbt $* --vars 'dev_disable: true' --defer --state deferral (pretty)"
  setopt localoptions pipefail
  "$dbt_bin" "$@" --vars 'dev_disable: true' --defer --state deferral --log-format json 2>&1 \
    | python3 -u -c "$_DBT_FMT"
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

compdef _dbd_complete dbd
compdef _dbd_complete dbdp
compdef _dbd_complete dbr
compdef _dbd_complete dbrp
compdef _dbd_complete dbdf
compdef _dbd_complete dbdfp
