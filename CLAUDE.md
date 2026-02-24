# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This repo contains custom zsh shell functions and aliases, sourced into the user's shell environment. Each `.zsh` file is a self-contained module of related helpers.

## Architecture

- **dbt.zsh** — dbt CLI wrappers that auto-resolve the dbt binary from the active virtualenv (`_dbt_bin`). Standard commands (`dbd`, `dbr`, `dbdf`) and pretty variants (`dbdp`, `dbrp`, `dbdfp`) that pipe JSON logs through an inline Python formatter with colored output, spinners, elapsed timers, and an error summary. Includes a model search function (`dbtls`) and filesystem-based zsh tab completion for model names.
- **git.zsh** — Git shortcuts: `gw <branch>` (create + checkout branch), `gmain` (checkout main + pull), `gup` (rebase current branch onto latest `origin/main`). Worktree helpers: `gwt <branch>` (create worktree in `../.worktrees/<branch>`, auto-activate venv, and cd into it), `gwtl` (list worktrees), `gwtcd <branch>` (cd into existing worktree), `gwtd <branch>` (remove worktree, prompt to delete branch).
- **utils.zsh** — General utilities: `output_dir_to_file` exports a directory tree to `dir_output.csv`.

## Conventions

- All dbt wrappers pass `--vars 'dev_disable: true' --defer --state deferral` by default via shared helpers (`_dbt_deferred`, `_dbt_deferred_pretty`).
- dbt binary resolution prefers `$VIRTUAL_ENV/bin/dbt` when a virtualenv is active.
- Functions print their effective command before running it (the `echo "Running: ..."` pattern in dbt wrappers).
- The Python log formatter is stored once in `_DBT_FMT` (shell variable via heredoc) and shared by all pretty variants. It must not contain single quotes. Literal Unicode characters use `\u` escapes to avoid encoding issues.
- Zsh completion is registered via `compdef` at the bottom of the file that defines the function. Completion supports graph operator prefixes (`+`, `2+`, etc.).
- Model name completion uses a time-cached filesystem glob (`_DBT_FS_MODEL_CACHE_TTL=5` seconds) rather than calling `dbt ls`.
