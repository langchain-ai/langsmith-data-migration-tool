"""Unit tests for FleetSkillMigrator."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FleetSkillMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestFleetSkillMigrator:
    """Test cases for FleetSkillMigrator."""

    @pytest.fixture
    def skill_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        return FleetSkillMigrator(source, dest, migration_state, sample_config)

    @pytest.fixture
    def sample_skill(self):
        return {
            "id": "web-research",
            "name": "web-research",
            "description": "Researches a topic across the web and writes a brief.",
            "visibility": "workspace",
            "files": {
                "SKILL.md": {"content": "# web-research\nResearch a topic.", "type": "file"},
            },
        }

    def test_list_skills(self, skill_migrator, sample_skill):
        """Test listing skills."""
        skill_migrator.source.get_cursor_paginated.return_value = [sample_skill]

        result = skill_migrator.list_skills()

        assert len(result) == 1
        assert result[0] == sample_skill
        skill_migrator.source.get_cursor_paginated.assert_called_once_with("/v1/fleet/skills")

    def test_list_skills_not_found(self, skill_migrator):
        """Test listing skills when endpoint not found."""
        from langsmith_migrator.core.api_client import NotFoundError

        skill_migrator.source.get_cursor_paginated.side_effect = NotFoundError(
            "Not found", status_code=404, request_info={}
        )

        result = skill_migrator.list_skills()

        assert len(result) == 0

    def test_create_skill(self, skill_migrator, sample_skill):
        """Test creating a skill."""
        skill_migrator.dest.get_cursor_paginated.return_value = []
        skill_migrator.dest.post.return_value = {"id": "new-skill-id"}

        result = skill_migrator.create_skill(sample_skill)

        assert result == "new-skill-id"
        skill_migrator.dest.post.assert_called_once()
        call_args = skill_migrator.dest.post.call_args
        assert call_args[0][0] == "/v1/fleet/skills"
        assert call_args[0][1]["name"] == "web-research"
        assert "SKILL.md" in call_args[0][1]["files"]

    def test_create_skill_existing_skip(self, skill_migrator, sample_skill):
        """Test skipping an existing skill (always skips)."""
        skill_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "existing-id", "name": "web-research"}
        ]

        result = skill_migrator.create_skill(sample_skill)

        assert result == "existing-id"
        skill_migrator.dest.post.assert_not_called()

    def test_create_skill_dry_run(self, skill_migrator, sample_skill):
        """Test dry run mode."""
        skill_migrator.dest.get_cursor_paginated.return_value = []
        skill_migrator.config.migration.dry_run = True

        result = skill_migrator.create_skill(sample_skill)

        assert result.startswith("dry-run-")
        skill_migrator.dest.post.assert_not_called()

    def test_find_existing_skill(self, skill_migrator):
        """find_existing_skill should search destination by name."""
        skill_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "dest-skill", "name": "web-research"},
        ]

        result = skill_migrator.find_existing_skill("web-research")

        assert result == "dest-skill"

    def test_find_existing_skill_not_found(self, skill_migrator):
        """find_existing_skill should return None when no match."""
        skill_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "other", "name": "other-skill"},
        ]

        result = skill_migrator.find_existing_skill("web-research")

        assert result is None

    def test_create_skill_existing_no_overwrite(self, skill_migrator, sample_skill):
        """Existing skills should not be overwritten (always skips)."""
        skill_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "existing-id", "name": "web-research"},
        ]

        result = skill_migrator.create_skill(sample_skill)

        assert result == "existing-id"
        skill_migrator.dest.patch.assert_not_called()

    def test_get_skill(self, skill_migrator, sample_skill):
        """Test getting a specific skill with file content."""
        skill_migrator.source.get.return_value = sample_skill

        result = skill_migrator.get_skill("web-research")

        assert result == sample_skill
        skill_migrator.source.get.assert_called_once_with("/v1/fleet/skills/web-research")

    def test_list_skills_error(self, skill_migrator):
        """Test listing skills with a general error returns empty."""
        skill_migrator.source.get_cursor_paginated.side_effect = Exception("API error")

        result = skill_migrator.list_skills()

        assert len(result) == 0
