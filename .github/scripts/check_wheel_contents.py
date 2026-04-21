#!/usr/bin/env python3
"""Verify built wheels include the CLI module required by the console script."""

from __future__ import annotations

import configparser
import sys
import zipfile
from pathlib import Path

REQUIRED_FILES = ("langsmith_migrator/cli/main.py",)
REQUIRED_CONSOLE_SCRIPTS = {
    "langsmith-migrator": "langsmith_migrator.__main__:main",
}


def _validate_wheel(path: Path) -> list[str]:
    errors: list[str] = []

    with zipfile.ZipFile(path) as wheel:
        names = set(wheel.namelist())

        for required_file in REQUIRED_FILES:
            if required_file not in names:
                errors.append(f"missing required module `{required_file}`")

        entry_points_path = next(
            (name for name in names if name.endswith(".dist-info/entry_points.txt")),
            None,
        )
        if entry_points_path is None:
            errors.append("missing `entry_points.txt` in the wheel metadata")
            return errors

        entry_points = configparser.ConfigParser()
        entry_points.read_string(wheel.read(entry_points_path).decode("utf-8"))

        if not entry_points.has_section("console_scripts"):
            errors.append("missing `[console_scripts]` section in `entry_points.txt`")
            return errors

        console_scripts = dict(entry_points.items("console_scripts"))
        for script_name, target in REQUIRED_CONSOLE_SCRIPTS.items():
            actual = console_scripts.get(script_name)
            if actual != target:
                errors.append(
                    f"console script `{script_name}` points to `{actual}` instead of `{target}`"
                )

    return errors


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: check_wheel_contents.py <wheel> [<wheel> ...]", file=sys.stderr)
        return 2

    has_errors = False
    for wheel_arg in argv[1:]:
        wheel_path = Path(wheel_arg)
        errors = _validate_wheel(wheel_path)
        if errors:
            has_errors = True
            print(f"[FAIL] {wheel_path}")
            for error in errors:
                print(f"  - {error}")
            continue

        print(f"[PASS] {wheel_path}")

    return 1 if has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
