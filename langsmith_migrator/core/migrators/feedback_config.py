"""Feedback config migration: the label types and scales of feedback keys."""

import json
from typing import Any, Dict, List, Optional, Tuple

from ..api_client import APIError
from .base import BaseMigrator

FEEDBACK_CONFIGS = "/feedback-configs"


def config_definition(raw: Any) -> Optional[Dict[str, Any]]:
    """A config's settings, comparable across instances.

    Omitted and null fields are the same, as are an omitted and a false
    ``allows_multiple_categories``; legacy configs are stored as a JSON string.
    """
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return None
    if not isinstance(raw, dict):
        return None
    definition = {k: v for k, v in raw.items() if v is not None and v != []}
    if definition.get("allows_multiple_categories") is False:
        definition.pop("allows_multiple_categories")
    return definition


class FeedbackConfigMigrator(BaseMigrator):
    """Copies the feedback configs of a workspace that the destination lacks."""

    def _list(self, client) -> List[Dict[str, Any]]:
        configs = []
        for config in client.get_paginated(
            FEEDBACK_CONFIGS, params={"include_all_configs": True}, raise_errors=True
        ):
            if isinstance(config, dict) and not config.get("is_deleted"):
                configs.append(config)
        return configs

    @staticmethod
    def _identity(config: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        definition = config_definition(config.get("feedback_config"))
        key = config.get("feedback_key")
        if not key or definition is None:
            return None
        return key, json.dumps(definition, sort_keys=True)

    @staticmethod
    def _creation_order(configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Plain configs before the enforced one, the key's first config first.

        The destination makes a key's first created config its first config,
        which is the key's default when none is enforced, and it refuses other
        scales for a key once one is enforced.
        """
        return sorted(
            configs,
            key=lambda c: (
                bool(c.get("is_enforced")),
                not c.get("is_first"),
                str(c.get("modified_at") or ""),
            ),
        )

    def migrate_all(self) -> Dict[str, Any]:
        """Create every live source config with no equal destination config.

        Configs are equal when they share a key and settings. A source config
        that is its key's enforced one is created enforced when the destination
        key has none and the key may create it; otherwise it is created plain.
        """
        result: Dict[str, Any] = {"created": 0, "existing": 0, "failed": 0, "errors": []}
        source = self._list(self.source)
        destination = self._list(self.dest)
        present = {self._identity(c) for c in destination}
        enforced_keys = {c.get("feedback_key") for c in destination if c.get("is_enforced")}
        for config in self._creation_order(source):
            identity = self._identity(config)
            if identity is None:
                result["failed"] += 1
                result["errors"].append((str(config.get("feedback_key")), "unreadable config"))
                continue
            if identity in present:
                result["existing"] += 1
                continue
            if self.config.migration.dry_run:
                self.log(f"[DRY RUN] Would create feedback config '{identity[0]}'")
                result["created"] += 1
                present.add(identity)
                continue
            enforce = bool(config.get("is_enforced")) and identity[0] not in enforced_keys
            if self._create(config, enforce, result):
                present.add(identity)
                if config.get("is_enforced"):
                    enforced_keys.add(identity[0])
        return result

    def _create(self, config: Dict[str, Any], enforce: bool, result: Dict[str, Any]) -> bool:
        key = config["feedback_key"]
        payload = {
            "feedback_key": key,
            "feedback_config": config_definition(config.get("feedback_config")),
            "is_lower_score_better": config.get("is_lower_score_better") or False,
        }
        try:
            self.dest.post(FEEDBACK_CONFIGS, {**payload, "is_enforced": True} if enforce else payload)
        except APIError as e:
            if not enforce:
                result["failed"] += 1
                result["errors"].append((key, str(e)))
                return False
            self.log(f"Could not create '{key}' as the enforced config ({e}); creating it plain", "warning")
            return self._create(config, enforce=False, result=result)
        result["created"] += 1
        self.log(f"Created feedback config '{key}'", "success")
        return True
