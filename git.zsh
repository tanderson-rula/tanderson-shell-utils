# Git helper functions and aliases

gw() {
  if [ -z "$1" ]; then
    echo "Usage: gw <branch-name>"
    return 1
  fi
  git checkout -b "$1"
}

alias gmain="git checkout main && git pull"

# --- rebase current branch onto latest main ---
grum() {
  git pull origin main --rebase
}

# --- worktree helpers (stored in ../.worktrees/ relative to repo root) ---

_gwt_root() {
  local repo_root
  repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || { echo "Not in a git repo" >&2; return 1; }
  echo "${repo_root:h}/.worktrees"
}

gwt() {
  if [[ -z "$1" ]]; then
    echo "Usage: gwt <branch>"
    return 1
  fi
  local branch="$1"
  local repo_root
  repo_root="$(git rev-parse --show-toplevel 2>/dev/null)"
  local wt_root
  wt_root="$(_gwt_root)" || return 1
  local wt_path="${wt_root}/${branch}"

  if git show-ref --verify --quiet "refs/heads/${branch}"; then
    git worktree add "$wt_path" "$branch"
  else
    git worktree add -b "$branch" "$wt_path" main
  fi && cd "$wt_path" && {
    if [[ -f ".venv/bin/activate" ]]; then
      source .venv/bin/activate
    elif [[ -n "$repo_root" && -f "${repo_root}/.venv/bin/activate" ]]; then
      source "${repo_root}/.venv/bin/activate"
    fi
  }
}

gwtl() {
  git worktree list
}

gwtcd() {
  if [[ -z "$1" ]]; then
    echo "Usage: gwtcd <branch>"
    return 1
  fi
  local branch="$1"
  local wt_root
  wt_root="$(_gwt_root)" || return 1
  local wt_path="${wt_root}/${branch}"

  if [[ -d "$wt_path" ]]; then
    cd "$wt_path"
  else
    echo "No worktree found at ${wt_path}"
    return 1
  fi
}

gwtd() {
  if [[ -z "$1" ]]; then
    echo "Usage: gwtd <branch>"
    return 1
  fi
  local branch="$1"
  local wt_root
  wt_root="$(_gwt_root)" || return 1
  local wt_path="${wt_root}/${branch}"

  git worktree remove "$wt_path" || return 1
  echo "Worktree removed: ${wt_path}"

  if git show-ref --verify --quiet "refs/heads/${branch}"; then
    read -q "reply?Delete branch '${branch}'? [y/N] "
    echo
    if [[ "$reply" == "y" ]]; then
      git branch -d "$branch"
    fi
  fi
}
