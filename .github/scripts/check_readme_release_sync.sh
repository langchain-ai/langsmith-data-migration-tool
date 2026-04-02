#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${BASE_REF:-}" ]]; then
  echo "BASE_REF is not set; skipping README release sync check."
  exit 0
fi

git fetch origin "${BASE_REF}" --depth=1

if ! changed_files="$(git diff --name-only "origin/${BASE_REF}...HEAD" 2>/dev/null)"; then
  echo "Three-dot diff failed; falling back to two-dot diff."
  changed_files="$(git diff --name-only "origin/${BASE_REF}" "HEAD")"
fi

if [[ -z "${changed_files}" ]]; then
  echo "No changed files found."
  exit 0
fi

release_touched=0
readme_touched=0

while IFS= read -r file; do
  [[ "${file}" == "pyproject.toml" || "${file}" == "CHANGELOG.md" ]] && release_touched=1
  [[ "${file}" == "README.md" ]] && readme_touched=1
done <<< "${changed_files}"

if [[ "${release_touched}" -eq 1 && "${readme_touched}" -eq 0 ]]; then
  echo "Release-related files changed (pyproject.toml/CHANGELOG.md) without README.md update."
  echo "Please update README.md as part of release changes."
  exit 1
fi

echo "README release sync check passed."
