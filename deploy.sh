#!/usr/bin/env bash
# deploy.sh — Build the static site and push to GitHub Pages.
#
# GitHub Pages must be configured once in the repo settings:
#   Settings → Pages → Source: Deploy from a branch
#   Branch: main   /docs
#
# After that, every run of this script updates the live site.

set -euo pipefail

cd "$(dirname "$0")"

echo "=== Building static site ==="
./venv/bin/python build_static.py

echo ""
echo "=== Committing docs/ ==="
git add docs/
if git diff --cached --quiet; then
    echo "No changes to commit."
else
    git commit -m "Deploy: update static site data ($(date '+%Y-%m-%d %H:%M'))"
fi

echo ""
echo "=== Pushing to GitHub ==="
git push

echo ""
echo "=== Done ==="
echo "Live at: https://resistmanagement.github.io/sandiego/"
