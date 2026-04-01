"""Version metadata tests."""

from pathlib import Path
import tomllib

from langsmith_migrator import __version__


def test_package_version_matches_pyproject():
    """The exported package version should stay aligned with pyproject metadata."""

    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))

    assert __version__ == data["project"]["version"]
