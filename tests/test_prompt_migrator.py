"""Unit tests for PromptMigrator."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from langsmith_migrator.core.migrators import PromptMigrator


class TestPromptMigrator:
    """Test cases for PromptMigrator."""

    @pytest.fixture
    def prompt_migrator(self, mock_api_client, sample_config, migration_state):
        """Create a PromptMigrator instance."""
        with patch('langsmith_migrator.core.migrators.prompt.Client') as mock_client:
            migrator = PromptMigrator(
                mock_api_client,
                mock_api_client,
                migration_state,
                sample_config
            )
            migrator.source_ls_client = Mock()
            migrator.dest_ls_client = Mock()
            return migrator

    @pytest.fixture
    def sample_prompt(self):
        """Sample prompt data."""
        return {
            'id': 'prompt-123',
            'repo_handle': 'user/test-prompt',
            'description': 'Test prompt',
            'readme': '# Test Prompt',
            'is_public': False,
            'is_archived': False,
            'tags': ['test'],
            'num_likes': 5,
            'num_downloads': 10,
            'num_commits': 3,
            'updated_at': '2024-01-01T00:00:00Z',
        }

    def test_list_prompts(self, prompt_migrator, sample_prompt):
        """Test listing prompts."""
        mock_response = Mock()
        mock_prompt_obj = Mock()
        mock_prompt_obj.id = 'prompt-123'
        mock_prompt_obj.repo_handle = 'user/test-prompt'
        mock_prompt_obj.description = 'Test prompt'
        mock_prompt_obj.readme = '# Test Prompt'
        mock_prompt_obj.is_public = False
        mock_prompt_obj.is_archived = False
        mock_prompt_obj.tags = ['test']
        mock_prompt_obj.num_likes = 5
        mock_prompt_obj.num_downloads = 10
        mock_prompt_obj.num_commits = 3
        mock_prompt_obj.updated_at = '2024-01-01T00:00:00Z'
        
        mock_response.repos = [mock_prompt_obj]
        prompt_migrator.source_ls_client.list_prompts.return_value = mock_response

        result = prompt_migrator.list_prompts()

        assert len(result) == 1
        assert result[0]['repo_handle'] == 'user/test-prompt'
        prompt_migrator.source_ls_client.list_prompts.assert_called()

    def test_list_prompts_empty(self, prompt_migrator):
        """Test listing prompts when none exist."""
        mock_response = Mock()
        mock_response.repos = []
        prompt_migrator.source_ls_client.list_prompts.return_value = mock_response

        result = prompt_migrator.list_prompts()

        assert len(result) == 0

    def test_list_prompts_uses_workspace_scoped_listing(self, prompt_migrator):
        """Prompt listing should rely on workspace scoping rather than visibility sweeps."""
        scoped_response = Mock()
        private_prompt = Mock()
        private_prompt.id = "private-1"
        private_prompt.repo_handle = "team/private-prompt"
        private_prompt.description = "private"
        private_prompt.readme = ""
        private_prompt.is_public = False
        private_prompt.is_archived = False
        private_prompt.tags = []
        private_prompt.num_likes = 0
        private_prompt.num_downloads = 0
        private_prompt.num_commits = 1
        private_prompt.updated_at = None
        scoped_response.repos = [private_prompt]
        prompt_migrator.source_ls_client.list_prompts.return_value = scoped_response

        result = prompt_migrator.list_prompts()

        assert [prompt["repo_handle"] for prompt in result] == ["team/private-prompt"]
        assert prompt_migrator.source_ls_client.list_prompts.call_args.kwargs["is_public"] is False

    def test_find_existing_prompt_paginates_destination_results(self, prompt_migrator):
        """Destination prompt existence checks should scan all pages, not just the first page."""
        first_page = Mock()
        first_page.repos = []
        for index in range(100):
            prompt = Mock()
            prompt.repo_handle = f"team/prompt-{index}"
            first_page.repos.append(prompt)

        second_page = Mock()
        target_prompt = Mock()
        target_prompt.repo_handle = "team/target-prompt"
        second_page.repos = [target_prompt]

        empty_response = Mock()
        empty_response.repos = []
        prompt_migrator.dest_ls_client.list_prompts.side_effect = [
            first_page,
            second_page,
        ]

        assert prompt_migrator.find_existing_prompt("team/target-prompt") is True
        assert prompt_migrator.dest_ls_client.list_prompts.call_args_list[0].kwargs["offset"] == 0
        assert prompt_migrator.dest_ls_client.list_prompts.call_args_list[1].kwargs["offset"] == 100
        assert prompt_migrator.dest_ls_client.list_prompts.call_args_list[0].kwargs["is_public"] is False

    def test_migrate_prompt_dry_run(self, prompt_migrator, sample_config):
        """Test migrating prompt in dry-run mode."""
        sample_config.migration.dry_run = True

        result = prompt_migrator.migrate_prompt('user/test-prompt')

        assert result == 'user/test-prompt'
        prompt_migrator.dest_ls_client.push_prompt.assert_not_called()

    def test_migrate_prompt_success(self, prompt_migrator, sample_config):
        """Test successful prompt migration using direct API (manifest-based)."""
        sample_config.migration.dry_run = False

        # Mock the direct API methods that use raw manifests (no model instantiation)
        mock_manifest = {"id": ["langchain", "schema", "runnable", "RunnableSequence"], "kwargs": {}}
        prompt_migrator._pull_prompt_manifest = Mock(return_value={
            "commit_hash": "abc123",
            "manifest": mock_manifest
        })
        prompt_migrator._push_prompt_manifest = Mock(return_value="new-commit-hash-123")
        prompt_migrator._verify_prompt_commit = Mock(return_value=(True, "new-commit-hash-123"))

        result = prompt_migrator.migrate_prompt('user/test-prompt')

        assert result == 'user/test-prompt'
        # Verify we used the manifest-based approach (not SDK's pull_prompt)
        prompt_migrator._pull_prompt_manifest.assert_called_once_with('user/test-prompt', 'latest')
        prompt_migrator._push_prompt_manifest.assert_called_once_with(
            'user/test-prompt',
            mock_manifest
        )

    def test_migrate_prompt_with_all_commits(self, prompt_migrator, sample_config):
        """Test migrating prompt with all commit history using manifest-based approach."""
        sample_config.migration.dry_run = False

        mock_commit1 = Mock()
        mock_commit1.commit_hash = 'hash1'
        mock_commit1.parent_commit_hash = None

        mock_commit2 = Mock()
        mock_commit2.commit_hash = 'hash2'
        mock_commit2.parent_commit_hash = 'hash1'

        prompt_migrator.source_ls_client.list_prompt_commits.return_value = [
            mock_commit1,
            mock_commit2
        ]

        # Mock the manifest-based methods
        mock_manifest = {"id": ["langchain", "schema", "runnable", "RunnableSequence"], "kwargs": {}}
        prompt_migrator._pull_prompt_manifest = Mock(return_value={
            "commit_hash": "abc123",
            "manifest": mock_manifest
        })
        prompt_migrator._push_prompt_manifest = Mock(return_value="new-commit-hash")
        prompt_migrator._verify_prompt_commit = Mock(return_value=(True, "new-commit-hash"))

        result = prompt_migrator.migrate_prompt('user/test-prompt', include_all_commits=True)

        assert result == 'user/test-prompt'
        # Should call _pull_prompt_manifest 2 times (once per commit)
        assert prompt_migrator._pull_prompt_manifest.call_count == 2
        # And _push_prompt_manifest 2 times
        assert prompt_migrator._push_prompt_manifest.call_count == 2

    def test_migrate_prompt_error_handling(self, prompt_migrator, sample_config):
        """Test error handling in prompt migration."""
        sample_config.migration.dry_run = False

        # Mock manifest pull to return None (failure)
        prompt_migrator._pull_prompt_manifest = Mock(return_value=None)

        result = prompt_migrator.migrate_prompt('user/test-prompt')

        assert result is None

    def test_prompts_api_available_via_commit_lookup_when_listing_is_disabled(self, prompt_migrator):
        """Capability probing should allow migration when commit lookups work but prompt listing does not."""

        prompt_migrator.probe_capabilities = Mock(
            return_value={
                "list_read": {"supported": False, "detail": "405_not_allowed"},
                "repo_lookup": {"supported": True, "detail": "ok"},
            }
        )

        assert prompt_migrator.check_prompts_api_available() == (True, "")

    def test_get_prompt_commits(self, prompt_migrator):
        """Test getting prompt commits."""
        mock_commit = Mock()
        mock_commit.commit_hash = 'hash1'
        mock_commit.manifest = {'key': 'value'}
        mock_commit.parent_commit_hash = None
        
        prompt_migrator.source_ls_client.list_prompt_commits.return_value = [mock_commit]

        result = prompt_migrator.get_prompt_commits('user/test-prompt')

        assert len(result) == 1
        assert result[0]['commit_hash'] == 'hash1'
        # assert result[0]['manifest'] == {'key': 'value'} # Manifest is not included in list
