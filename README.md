# zsh functions

Custom zsh shell functions and aliases, sourced into the shell environment. Each `.zsh` file is a self-contained module of related helpers.

## dbt.zsh

Wrappers around the dbt CLI. All build/run commands automatically apply `--vars 'dev_disable: true' --defer --state deferral`. The dbt binary is resolved from the active virtualenv (`$VIRTUAL_ENV/bin/dbt`) when available, falling back to `dbt` on `$PATH`.

All selector-based commands support zsh tab completion for model names (filesystem-cached with a 5s TTL) and dbt selector prefixes (`tag:`, `path:`, `+`, etc.).

| Function | Usage | Description |
|----------|-------|-------------|
| `dbd`    | `dbd <selectors>` | `dbt build` with deferral. |
| `dbdp`   | `dbdp <selectors>` | Same as `dbd` but pipes JSON logs through a formatter that shows a live timeline of model starts, concurrency slots, and durations. Only model events are displayed. |
| `dbr`    | `dbr <selectors>` | `dbt run` with deferral. |
| `dbdf`   | `dbdf <selectors>` | `dbt build --full-refresh` with deferral. |
| `dbtls`  | `dbtls <selector> <pattern>` | Runs `dbt ls` for models matching the selector, then filters output through `rg` with the given pattern. |

## git.zsh

Git shortcuts and environment helpers.

| Function/Alias | Usage | Description |
|----------------|-------|-------------|
| `gw`    | `gw <branch-name>` | Creates and checks out a new branch (`git checkout -b`). |
| `gmain` | `gmain` | Checks out `main` and pulls latest (`git checkout main && git pull`). |
| `va`    | `va` | Activates the local virtualenv (`source .venv/bin/activate`). |

## utils.zsh

General-purpose shell utilities.

| Function | Usage | Description |
|----------|-------|-------------|
| `output_dir_to_file` | `output_dir_to_file` | Recursively walks the current directory and writes a CSV (`dir_output.csv`) with columns `type,path,name,extension` for every file and directory found. |
