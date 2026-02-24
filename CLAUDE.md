# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This repo contains custom zsh shell functions and aliases, sourced into the user's shell environment. Each `.zsh` file is a self-contained module of related helpers.

## Architecture

- **dbt.zsh** — dbt CLI wrappers that auto-resolve the dbt binary from the active virtualenv (`_dbt_bin`), shorthand build/run commands with deferral (`dbd`, `dbr`, `dbdf`), a model search function (`dbtls`), and filesystem-based zsh tab completion for model names.
- **git.zsh** — Git shortcuts: `gw <branch>` (create + checkout branch), `gmain` (checkout main + pull), `va` (activate `.venv`).
- **utils.zsh** — General utilities: `output_dir_to_file` exports a directory tree to `dir_output.csv`.

## Conventions

- All dbt wrappers pass `--vars 'dev_disable: true' --defer --state deferral` by default.
- dbt binary resolution prefers `$VIRTUAL_ENV/bin/dbt` when a virtualenv is active.
- Functions print their effective command before running it (the `echo "Running: ..."` pattern in dbt wrappers).
- Zsh completion is registered via `compdef` at the bottom of the file that defines the function.
- Model name completion uses a time-cached filesystem glob (`_DBT_FS_MODEL_CACHE_TTL=5` seconds) rather than calling `dbt ls`.
