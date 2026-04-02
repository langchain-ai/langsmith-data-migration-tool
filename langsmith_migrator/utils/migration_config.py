"""Migration config file management for project name mappings and workspace selections."""

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional


logger = logging.getLogger(__name__)

DEFAULT_CONFIG_DIR = os.path.expanduser("~/.langsmith-migrator")
DEFAULT_CONFIG_PATH = os.path.join(DEFAULT_CONFIG_DIR, "config.json")

CONFIG_VERSION = "1"


@dataclass
class WorkspaceConfig:
    """Workspace selection for source or destination."""
    workspace_id: str = ""
    workspace_name: str = ""


def _parse_workspace(data: Optional[dict]) -> Optional[WorkspaceConfig]:
    """Parse a workspace config dict, ignoring unknown keys for forward compat."""
    if not data or not isinstance(data, dict):
        return None
    # Only pass keys that WorkspaceConfig knows about (forward compat)
    known = {k: v for k, v in data.items() if k in WorkspaceConfig.__dataclass_fields__}
    return WorkspaceConfig(**known)


@dataclass
class MigrationFileConfig:
    """Persistent migration configuration stored on disk."""
    version: str = CONFIG_VERSION
    source_workspace: Optional[WorkspaceConfig] = None
    destination_workspace: Optional[WorkspaceConfig] = None
    project_name_mapping: Dict[str, str] = field(default_factory=dict)
    workspace_mapping: Dict[str, str] = field(default_factory=dict)  # source_ws_id -> dest_ws_id
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        """Serialize to a JSON-compatible dict."""
        return {
            "version": self.version,
            "source_workspace": asdict(self.source_workspace) if self.source_workspace else None,
            "destination_workspace": asdict(self.destination_workspace) if self.destination_workspace else None,
            "project_name_mapping": self.project_name_mapping,
            "workspace_mapping": self.workspace_mapping,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "MigrationFileConfig":
        """Deserialize from a dict. Unknown keys are ignored for forward compat."""
        config = cls()
        config.version = data.get("version", CONFIG_VERSION)
        config.project_name_mapping = data.get("project_name_mapping", {})
        config.workspace_mapping = data.get("workspace_mapping", {})
        config.created_at = data.get("created_at", "")
        config.updated_at = data.get("updated_at", "")
        config.source_workspace = _parse_workspace(data.get("source_workspace"))
        config.destination_workspace = _parse_workspace(data.get("destination_workspace"))
        return config


def load_config(path: Optional[str] = None) -> Optional[MigrationFileConfig]:
    """Load migration config from disk.

    Args:
        path: Config file path. Defaults to ~/.langsmith-migrator/config.json.

    Returns:
        MigrationFileConfig if file exists and is valid, None otherwise.
    """
    config_path = path or DEFAULT_CONFIG_PATH
    try:
        with open(config_path, "r") as f:
            data = json.load(f)
        return MigrationFileConfig.from_dict(data)
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        logger.warning("Failed to parse migration config at %s: %s", config_path, e)
        return None


def save_config(config: MigrationFileConfig, path: Optional[str] = None) -> Path:
    """Save migration config to disk.

    Args:
        config: The config to save.
        path: Config file path. Defaults to ~/.langsmith-migrator/config.json.

    Returns:
        The Path where the config was saved.
    """
    config_path = Path(path or DEFAULT_CONFIG_PATH)
    config_path.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc).isoformat()
    data = config.to_dict()
    if not data.get("created_at"):
        data["created_at"] = now
    data["updated_at"] = now

    fd, tmp_path = tempfile.mkstemp(dir=str(config_path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, str(config_path))
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return config_path
