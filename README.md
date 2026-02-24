# zsh functions

Custom zsh shell functions and aliases, sourced into the shell environment. Each `.zsh` file is a self-contained module of related helpers.

## Installation

Add the following to your `~/.zshrc` to source all modules:

```zsh
for f in ~/.zsh/functions/*.zsh; do
  source "$f"
done
```

Or source individual files if you only want specific modules:

```zsh
source ~/.zsh/functions/git.zsh
source ~/.zsh/functions/dbt.zsh
source ~/.zsh/functions/utils.zsh
```

Restart your shell or run `source ~/.zshrc` to pick up changes.

## dbt.zsh

Wrappers around the dbt CLI. All build/run commands automatically apply `--vars 'dev_disable: true' --defer --state deferral`. The dbt binary is resolved from the active virtualenv (`$VIRTUAL_ENV/bin/dbt`) when available, falling back to `dbt` on `$PATH`.

All selector-based commands support zsh tab completion for model names (filesystem-cached with a 5s TTL) and dbt selector prefixes (`tag:`, `path:`, `+`, etc.).

Each command has a standard version and a pretty (`p`) variant. The pretty variants pipe JSON logs through a live formatter with colored spinners, elapsed timers, aligned columns, and an error/warning summary at the end.

| Function | Usage | Description |
|----------|-------|-------------|
| `dbd`    | `dbd <selectors>` | `dbt build` with deferral. |
| `dbdp`   | `dbdp <selectors>` | Pretty variant of `dbd`. |
| `dbr`    | `dbr <selectors>` | `dbt run` with deferral. |
| `dbrp`   | `dbrp <selectors>` | Pretty variant of `dbr`. |
| `dbdf`   | `dbdf <selectors>` | `dbt build --full-refresh` with deferral. |
| `dbdfp`  | `dbdfp <selectors>` | Pretty variant of `dbdf`. |
| `dbtls`  | `dbtls <selector> <pattern>` | Runs `dbt ls` for models matching the selector, then filters output through `rg` with the given pattern. |

## git.zsh

Git shortcuts and environment helpers.

| Function/Alias | Usage | Description |
|----------------|-------|-------------|
| `gw`    | `gw <branch-name>` | Creates and checks out a new branch (`git checkout -b`). |
| `gmain` | `gmain` | Checks out `main` and pulls latest (`git checkout main && git pull`). |
| `grum`  | `grum` | Rebases the current branch onto latest `origin/main` (`git pull origin main --rebase`). |
| `gwt`   | `gwt <branch>` | Creates a worktree at `../.worktrees/<branch>`, creates the branch from `main` if new, auto-activates the venv, and `cd`s into it. |
| `gwtl`  | `gwtl` | Lists all active worktrees. |
| `gwtcd` | `gwtcd <branch>` | `cd` into an existing worktree. |
| `gwtd`  | `gwtd <branch>` | Removes the worktree and prompts to delete the branch. |

## utils.zsh

General-purpose shell utilities.

| Function | Usage | Description |
|----------|-------|-------------|
| `output_dir_to_file` | `output_dir_to_file` | Recursively walks the current directory and writes a CSV (`dir_output.csv`) with columns `type,path,name,extension` for every file and directory found. |
