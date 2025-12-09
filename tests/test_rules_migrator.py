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
            'display_name': 'Test Rule',
            'is_enabled': True,
            'sampling_rate': 1.0,
            'filter': 'eq(is_root, true)',
            'dataset_id': 'dataset-123',  # Required by API
            'evaluator_version': 3,
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
        
        # Mock finding rules in source
        mock_api_client.get_paginated.return_value = [rule1, rule2]
        
        # Mock create rule calls (check existence fails/returns None, then create works)
        # We need to mock find_existing_rule to return None to simulate non-existence
        with patch.object(rules_migrator, 'find_existing_rule', return_value=None):
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

    def test_create_rule_includes_evaluators_and_code_evaluators(self, rules_migrator, mock_api_client, sample_config, sample_rule):
        """Test that evaluators and code_evaluators are included in the payload."""
        sample_config.migration.dry_run = False
        mock_api_client.post.return_value = {'id': 'new-rule-123'}

        # Mock mappings
        rules_migrator._dataset_id_map = {'dataset-123': 'dest-dataset-123'}
        rules_migrator._project_id_map = {}

        # Enhanced sample rule with LLM evaluators
        enhanced_rule = {
            **sample_rule,
            'evaluators': [
                {
                    'structured': {
                        'hub_ref': 'owner/prompt:latest',
                        'variable_mapping': {'input': 'input', 'output': 'output'}
                    }
                }
            ],
            'code_evaluators': [
                {
                    'code': 'def perform_eval(run, example): return {"key": "test", "score": 1}',
                    'language': 'python'
                }
            ]
        }

        result = rules_migrator.create_rule(enhanced_rule)

        assert result == 'new-rule-123'

        call_args = mock_api_client.post.call_args
        payload = call_args[0][1]

        assert 'evaluators' in payload
        assert len(payload['evaluators']) == 1
        assert payload['evaluators'][0]['structured']['hub_ref'] == 'owner/prompt:latest'
        assert 'code_evaluators' in payload
        assert len(payload['code_evaluators']) == 1
        assert payload['code_evaluators'][0]['language'] == 'python'

    def test_update_existing_rule(self, rules_migrator, mock_api_client, sample_config, sample_rule):
        """Test updating an existing rule when skip_existing is False."""
        sample_config.migration.dry_run = False
        sample_config.migration.skip_existing = False
        
        # Mock mappings
        rules_migrator._dataset_id_map = {'dataset-123': 'dest-dataset-123'}
        rules_migrator._project_id_map = {}
        
        # Mock find_existing_rule to return an existing ID
        with patch.object(rules_migrator, 'find_existing_rule', return_value='existing-rule-123'):
            mock_api_client.patch.return_value = None  # PATCH typically returns None or the updated object
            
            result = rules_migrator.create_rule(sample_rule)
            
            # Should return the existing rule ID after update
            assert result == 'existing-rule-123'
            # Should have called PATCH, not POST
            mock_api_client.patch.assert_called_once()
            mock_api_client.post.assert_not_called()
            
            # Verify PATCH was called with correct endpoint
            call_args = mock_api_client.patch.call_args
            assert '/runs/rules/existing-rule-123' in call_args[0][0]

    def test_skip_existing_rule(self, rules_migrator, mock_api_client, sample_config, sample_rule):
        """Test skipping an existing rule when skip_existing is True."""
        sample_config.migration.dry_run = False
        sample_config.migration.skip_existing = True
        
        # Mock mappings
        rules_migrator._dataset_id_map = {'dataset-123': 'dest-dataset-123'}
        rules_migrator._project_id_map = {}
        
        # Mock find_existing_rule to return an existing ID
        with patch.object(rules_migrator, 'find_existing_rule', return_value='existing-rule-123'):
            result = rules_migrator.create_rule(sample_rule)
            
            # Should return the existing rule ID without updating
            assert result == 'existing-rule-123'
            # Should not have called PATCH or POST
            mock_api_client.patch.assert_not_called()
            mock_api_client.post.assert_not_called()

    def test_update_rule_failure(self, rules_migrator, mock_api_client, sample_config, sample_rule):
        """Test handling update failure."""
        sample_config.migration.dry_run = False
        sample_config.migration.skip_existing = False
        
        # Mock mappings
        rules_migrator._dataset_id_map = {'dataset-123': 'dest-dataset-123'}
        rules_migrator._project_id_map = {}
        
        # Mock find_existing_rule to return an existing ID
        with patch.object(rules_migrator, 'find_existing_rule', return_value='existing-rule-123'):
            # Make PATCH fail
            mock_api_client.patch.side_effect = Exception("Update failed")
            
            result = rules_migrator.create_rule(sample_rule)
            
            # Should return None on failure
            assert result is None

    def test_create_rule_with_project_mapping(self, rules_migrator, mock_api_client, sample_config):
        """Test creating a rule with project-specific mapping."""
        sample_config.migration.dry_run = False
        
        project_rule = {
            'id': 'rule-with-project',
            'display_name': 'Project Rule',
            'is_enabled': True,
            'sampling_rate': 1.0,
            'session_id': 'source-project-123',  # Project-specific rule
        }
        
        # Mock project mapping
        rules_migrator._project_id_map = {'source-project-123': 'dest-project-456'}
        rules_migrator._dataset_id_map = {}
        
        mock_api_client.post.return_value = {'id': 'new-rule-123'}
        
        result = rules_migrator.create_rule(project_rule)
        
        assert result == 'new-rule-123'
        
        # Verify the payload uses the mapped project ID
        call_args = mock_api_client.post.call_args
        payload = call_args[0][1]
        assert payload.get('session_id') == 'dest-project-456'

    def test_create_rule_with_both_project_and_dataset(self, rules_migrator, mock_api_client, sample_config):
        """Test creating a rule with both project and dataset mapping."""
        sample_config.migration.dry_run = False
        
        rule_with_both = {
            'id': 'rule-with-both',
            'display_name': 'Rule With Both',
            'is_enabled': True,
            'sampling_rate': 1.0,
            'session_id': 'source-project-123',
            'dataset_id': 'source-dataset-789',
        }
        
        # Mock both mappings
        rules_migrator._project_id_map = {'source-project-123': 'dest-project-456'}
        rules_migrator._dataset_id_map = {'source-dataset-789': 'dest-dataset-abc'}
        
        mock_api_client.post.return_value = {'id': 'new-rule-123'}
        
        result = rules_migrator.create_rule(rule_with_both)
        
        assert result == 'new-rule-123'
        
        # Verify the payload uses both mapped IDs
        call_args = mock_api_client.post.call_args
        payload = call_args[0][1]
        assert payload.get('session_id') == 'dest-project-456'
        assert payload.get('dataset_id') == 'dest-dataset-abc'

    def test_update_rule_filters_create_only_fields(self, rules_migrator, mock_api_client, sample_config):
        """Test that update_rule filters out CREATE-only fields like group_by."""
        sample_config.migration.dry_run = False
        
        # Payload with group_by (which is CREATE-only, not allowed in PATCH)
        payload_with_group_by = {
            'display_name': 'Test Rule',
            'is_enabled': True,
            'sampling_rate': 1.0,
            'session_id': 'project-123',
            'group_by': 'thread_id',  # CREATE-only field
            'evaluators': [{'structured': {'hub_ref': 'test:latest'}}],
        }
        
        mock_api_client.patch.return_value = {}
        
        result = rules_migrator.update_rule('existing-rule-123', payload_with_group_by)
        
        assert result == 'existing-rule-123'
        
        # Verify the PATCH payload does NOT include group_by
        call_args = mock_api_client.patch.call_args
        patch_payload = call_args[0][1]
        assert 'group_by' not in patch_payload
        # But other fields should be present
        assert patch_payload.get('display_name') == 'Test Rule'
        assert patch_payload.get('session_id') == 'project-123'
        assert patch_payload.get('evaluators') == [{'structured': {'hub_ref': 'test:latest'}}]

    def test_create_rule_includes_group_by(self, rules_migrator, mock_api_client, sample_config):
        """Test that create_rule includes group_by field for thread evaluators."""
        sample_config.migration.dry_run = False
        
        rule_with_group_by = {
            'id': 'thread-rule',
            'display_name': 'Thread Evaluator Rule',
            'is_enabled': True,
            'sampling_rate': 1.0,
            'dataset_id': 'source-dataset-123',
            'group_by': 'thread_id',  # For thread evaluators
        }
        
        # Mock dataset mapping
        rules_migrator._dataset_id_map = {'source-dataset-123': 'dest-dataset-456'}
        rules_migrator._project_id_map = {}
        
        mock_api_client.post.return_value = {'id': 'new-rule-123'}
        
        result = rules_migrator.create_rule(rule_with_group_by)
        
        assert result == 'new-rule-123'
        
        # Verify the POST payload INCLUDES group_by
        call_args = mock_api_client.post.call_args
        payload = call_args[0][1]
        assert payload.get('group_by') == 'thread_id'
        assert payload.get('dataset_id') == 'dest-dataset-456'

    def test_clean_none_values(self, rules_migrator):
        """Test that _clean_none_values removes None values from nested structures."""
        from langsmith_migrator.core.migrators.rules import RulesMigrator
        
        # Test with evaluator-like structure that has None values
        evaluators_with_nones = [
            {
                'structured': {
                    'hub_ref': 'eval_test:latest',
                    'prompt': None,
                    'template_format': None,
                    'schema': None,
                    'variable_mapping': {'inputs': 'input', 'outputs': 'output'},
                    'model': None
                }
            }
        ]
        
        cleaned = RulesMigrator._clean_none_values(evaluators_with_nones)
        
        # Should only have hub_ref and variable_mapping
        assert cleaned == [
            {
                'structured': {
                    'hub_ref': 'eval_test:latest',
                    'variable_mapping': {'inputs': 'input', 'outputs': 'output'}
                }
            }
        ]

    def test_create_rule_cleans_evaluator_none_values(self, rules_migrator, mock_api_client, sample_config):
        """Test that create_rule cleans None values from evaluators before sending."""
        sample_config.migration.dry_run = False
        
        rule_with_none_evaluators = {
            'id': 'rule-with-nones',
            'display_name': 'Rule With Nones',
            'is_enabled': True,
            'sampling_rate': 1.0,
            'dataset_id': 'source-dataset-123',
            'evaluators': [
                {
                    'structured': {
                        'hub_ref': 'eval_test:latest',
                        'prompt': None,
                        'template_format': None,
                        'schema': None,
                        'variable_mapping': {'inputs': 'input'},
                        'model': None
                    }
                }
            ]
        }
        
        # Mock dataset mapping
        rules_migrator._dataset_id_map = {'source-dataset-123': 'dest-dataset-456'}
        rules_migrator._project_id_map = {}
        
        mock_api_client.post.return_value = {'id': 'new-rule-123'}
        
        result = rules_migrator.create_rule(rule_with_none_evaluators)
        
        assert result == 'new-rule-123'
        
        # Verify the POST payload has cleaned evaluators (no None values)
        call_args = mock_api_client.post.call_args
        payload = call_args[0][1]
        evaluators = payload.get('evaluators')
        assert evaluators is not None
        assert len(evaluators) == 1
        structured = evaluators[0]['structured']
        # Should NOT have keys with None values
        assert 'prompt' not in structured
        assert 'template_format' not in structured
        assert 'schema' not in structured
        assert 'model' not in structured
        # Should have the non-None values
        assert structured['hub_ref'] == 'eval_test:latest'
        assert structured['variable_mapping'] == {'inputs': 'input'}

