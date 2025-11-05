"""Unit tests for RulesMigrator."""

import pytest
from unittest.mock import Mock, patch
from langsmith_migrator.core.migrators import RulesMigrator
from langsmith_migrator.core.api_client import NotFoundError


class TestRulesMigrator:
    """Test cases for RulesMigrator."""

    @pytest.fixture
    def rules_migrator(self, mock_api_client, sample_config, migration_state):
        """Create a RulesMigrator instance."""
        return RulesMigrator(
            mock_api_client,
            mock_api_client,
            migration_state,
            sample_config
        )

    @pytest.fixture
    def sample_rule(self):
        """Sample rule data."""
        return {
            'id': 'rule-123',
            'name': 'Test Rule',
            'description': 'A test rule',
            'enabled': True,
            'rule_type': 'auto_add_to_dataset',
            'filters': {'status': 'success'},
            'actions': [{'type': 'add_to_dataset', 'dataset_id': 'dataset-123'}],
            'sampling_rate': 1.0,
            'dataset_id': 'dataset-123',  # Required by API
        }

    def test_list_rules(self, rules_migrator, mock_api_client, sample_rule):
        """Test listing rules."""
        mock_api_client.get_paginated.return_value = [sample_rule]

        result = rules_migrator.list_rules()

        assert len(result) == 1
        assert result[0] == sample_rule
        mock_api_client.get_paginated.assert_called_once()

    def test_list_rules_not_found(self, rules_migrator, mock_api_client):
        """Test listing rules when endpoint not found."""
        def raise_not_found(*args, **kwargs):
            raise NotFoundError("Not found", status_code=404, request_info={})
        
        mock_api_client.get_paginated.side_effect = raise_not_found

        result = rules_migrator.list_rules()

        assert len(result) == 0

    def test_list_rules_error(self, rules_migrator, mock_api_client):
        """Test listing rules with general error."""
        mock_api_client.get_paginated.side_effect = Exception("API Error")

        result = rules_migrator.list_rules()

        assert len(result) == 0

    def test_get_rule(self, rules_migrator, mock_api_client, sample_rule):
        """Test getting a specific rule."""
        rule_id = "rule-123"
        mock_api_client.get.return_value = sample_rule

        result = rules_migrator.get_rule(rule_id)

        assert result == sample_rule
        mock_api_client.get.assert_called_once_with(f"/runs/rules/{rule_id}")

    def test_get_rule_not_found(self, rules_migrator, mock_api_client):
        """Test getting a rule that doesn't exist."""
        def raise_not_found(*args, **kwargs):
            raise NotFoundError("Not found", status_code=404, request_info={})
        
        mock_api_client.get.side_effect = raise_not_found

        result = rules_migrator.get_rule("rule-123")

        assert result is None

    def test_list_project_rules(self, rules_migrator, mock_api_client, sample_rule):
        """Test listing rules for a specific project."""
        project_id = "project-123"
        mock_api_client.get_paginated.return_value = [sample_rule]

        result = rules_migrator.list_project_rules(project_id)

        assert len(result) == 1
        assert result[0] == sample_rule
        mock_api_client.get_paginated.assert_called_once()

    def test_create_rule_dry_run(self, rules_migrator, sample_config, sample_rule):
        """Test creating rule in dry-run mode."""
        sample_config.migration.dry_run = True

        result = rules_migrator.create_rule(sample_rule)

        assert result is not None
        assert 'dry-run' in result

    def test_create_rule_success(self, rules_migrator, mock_api_client, sample_config, sample_rule):
        """Test successful rule creation."""
        sample_config.migration.dry_run = False
        mock_api_client.post.return_value = {'id': 'new-rule-123'}
        
        # Mock the ID mapping methods to return a mapping for the dataset
        rules_migrator._dataset_id_map = {'dataset-123': 'dest-dataset-123'}
        rules_migrator._project_id_map = {}

        result = rules_migrator.create_rule(sample_rule)

        assert result == 'new-rule-123'
        mock_api_client.post.assert_called_once()

    def test_create_rule_with_project(self, rules_migrator, mock_api_client, sample_config, sample_rule):
        """Test creating rule with project context."""
        sample_config.migration.dry_run = False
        project_id = "project-123"
        mock_api_client.post.return_value = {'id': 'new-rule-123'}

        result = rules_migrator.create_rule(sample_rule, target_project_id=project_id)

        assert result == 'new-rule-123'
        call_args = mock_api_client.post.call_args
        # Now using /runs/rules endpoint for all rules
        assert '/runs/rules' in call_args[0][0]
        assert call_args[0][1]['session_id'] == project_id

    def test_create_rule_error(self, rules_migrator, mock_api_client, sample_config, sample_rule):
        """Test error handling in rule creation."""
        sample_config.migration.dry_run = False
        mock_api_client.post.side_effect = Exception("API Error")

        result = rules_migrator.create_rule(sample_rule)

        assert result is None

    def test_migrate_rule(self, rules_migrator, mock_api_client, sample_config, sample_rule):
        """Test migrating a single rule."""
        sample_config.migration.dry_run = False
        mock_api_client.get.return_value = sample_rule
        mock_api_client.post.return_value = {'id': 'new-rule-123'}
        
        # Mock the ID mapping methods
        rules_migrator._dataset_id_map = {'dataset-123': 'dest-dataset-123'}
        rules_migrator._project_id_map = {}

        result = rules_migrator.migrate_rule('rule-123')

        assert result == 'new-rule-123'

    def test_migrate_project_rules(self, rules_migrator, mock_api_client, sample_config, sample_rule):
        """Test migrating all rules from one project to another."""
        sample_config.migration.dry_run = False
        
        rule1 = {**sample_rule, 'id': 'rule-1', 'name': 'Rule 1'}
        rule2 = {**sample_rule, 'id': 'rule-2', 'name': 'Rule 2'}
        
        mock_api_client.get_paginated.return_value = [rule1, rule2]
        mock_api_client.post.side_effect = [
            {'id': 'new-rule-1'},
            {'id': 'new-rule-2'}
        ]

        result = rules_migrator.migrate_project_rules('source-project', 'dest-project')

        assert len(result) == 2
        assert result['rule-1'] == 'new-rule-1'
        assert result['rule-2'] == 'new-rule-2'
        assert mock_api_client.post.call_count == 2

    def test_migrate_project_rules_empty(self, rules_migrator, mock_api_client):
        """Test migrating rules when project has none."""
        mock_api_client.get_paginated.return_value = []

        result = rules_migrator.migrate_project_rules('source-project', 'dest-project')

        assert len(result) == 0
        mock_api_client.post.assert_not_called()
