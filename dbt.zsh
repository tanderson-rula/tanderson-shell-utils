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
  local -a prefixes
  prefixes=( 'tag:' 'path:' 'model:' 'source:' 'snapshot:' 'seed:' 'test:' 'exposure:' 'metric:' '+' )

  # Always offer prefixes
  compadd -- $prefixes

  # If it looks like selector syntax (tag:, path:, +something), don't force model-name completion
  if [[ "$cur" == *:* || "$cur" == +* ]]; then
    return
  fi

  # Offer model names
  compadd -- $_DBT_FS_MODEL_CACHE
}

compdef _dbd_complete dbd
compdef _dbd_complete dbdf
compdef _dbd_complete dbr
