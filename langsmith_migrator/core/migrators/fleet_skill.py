"""Fleet shared skills migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator
from ..api_client import NotFoundError


class FleetSkillMigrator(BaseMigrator):
    """Handles migration of Fleet shared workspace skills."""

    def list_skills(self) -> List[Dict[str, Any]]:
        """List all shared skills from the source workspace."""
        skills = []
        try:
            for skill in self.source.get_cursor_paginated("/v1/fleet/skills"):
                if isinstance(skill, dict):
                    skills.append(skill)
        except NotFoundError:
            self.log("Fleet skills endpoint not found", "warning")
        except Exception as e:
            self.log(f"Failed to list Fleet skills: {e}", "warning")
        return skills

    def get_skill(self, skill_id: str) -> Dict[str, Any]:
        """Get a specific skill with file content."""
        return self.source.get(f"/v1/fleet/skills/{skill_id}")

    def find_existing_skill(self, name: str) -> Optional[str]:
        """Check if a skill with the same name already exists in destination."""
        skill = self.dest_index(
            "_dest_skills",
            "/v1/fleet/skills",
            "name",
            error_label="skill",
        ).get(name)
        return skill.get("id") if skill else None

    def create_skill(self, skill: Dict[str, Any]) -> str:
        """Create or update a skill in the destination workspace.

        Returns the destination skill ID (repo handle).
        """
        name = skill.get("name", "")
        existing_id = self.find_existing_skill(name)

        if existing_id:
            self.log(f"Skill '{name}' already exists, skipping", "warning")
            return existing_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create skill: {name}")
            return f"dry-run-{skill.get('id', name)}"

        payload = {
            "name": name,
            "files": skill.get("files", {}),
        }

        response = self.dest.post("/v1/fleet/skills", payload)

        if not isinstance(response, dict):
            from ..api_client import APIError
            raise APIError(
                f"Invalid response creating skill: expected dict, got {type(response)}"
            )
        if "id" not in response:
            from ..api_client import APIError
            raise APIError(
                f"Invalid response creating skill: missing 'id' field. Response: {response}"
            )

        self.register_dest_item("_dest_skills", name, response)
        return response["id"]

    def _update_skill_files(self, skill_id: str, skill: Dict[str, Any]) -> None:
        """Update files on an existing destination skill."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update skill: {skill.get('name')} ({skill_id})")
            return

        files = skill.get("files", {})
        if not files:
            return

        patch_payload = {path: entry for path, entry in files.items()}
        self.dest.patch(f"/v1/fleet/skills/{skill_id}", {"files": patch_payload})
        self.log(f"Updated skill: {skill.get('name')} ({skill_id})", "success")
