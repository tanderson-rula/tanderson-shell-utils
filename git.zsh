# Git helper functions and aliases

gw() {
  if [ -z "$1" ]; then
    echo "Usage: gw <branch-name>"
    return 1
  fi
  git checkout -b "$1"
}

alias gmain="git checkout main && git pull"
alias va="source .venv/bin/activate"
