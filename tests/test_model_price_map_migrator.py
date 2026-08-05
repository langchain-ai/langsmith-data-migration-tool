"""Unit tests for ModelPriceMapMigrator."""

import pytest

from langsmith_migrator.core.api_client import ConflictError
from langsmith_migrator.core.migrators import ModelPriceMapMigrator


class TestModelPriceMapMigrator:
    """Test cases for ModelPriceMapMigrator."""

    @pytest.fixture
    def pricing_migrator(self, mock_api_client, sample_config, migration_state):
        """Create a ModelPriceMapMigrator instance (same mock for source and dest)."""
        return ModelPriceMapMigrator(
            mock_api_client, mock_api_client, migration_state, sample_config
        )

    @pytest.fixture
    def custom_entry(self):
        """A workspace-custom pricing entry (tenant_id set)."""
        return {
            "id": "price-src-1",
            "tenant_id": "ws-source",
            "name": "amazon_bedrock_claude-sonnet-4-5 (fuel50)",
            "start_time": None,
            "match_path": ["model", "model_name"],
            "match_pattern": "claude-sonnet-4-5",
            "prompt_cost": "0.000003",
            "completion_cost": "0.000015",
            "prompt_cost_details": None,
            "completion_cost_details": None,
            "provider": "amazon_bedrock",
        }

    @pytest.fixture
    def global_entry(self):
        """A global/built-in pricing entry (tenant_id is null) - should be skipped."""
        return {
            "id": "price-global-1",
            "tenant_id": None,
            "name": "gpt-4o",
            "match_path": ["model"],
            "match_pattern": "gpt-4o",
            "prompt_cost": "0.0000025",
            "completion_cost": "0.00001",
            "provider": "openai",
        }

    def test_list_price_maps_filters_global_entries(
        self, pricing_migrator, mock_api_client, custom_entry, global_entry
    ):
        """Only workspace-custom entries (tenant_id set) are returned."""
        mock_api_client.get.return_value = [custom_entry, global_entry]

        result = pricing_migrator.list_price_maps()

        assert len(result) == 1
        assert result[0]["id"] == "price-src-1"
        mock_api_client.get.assert_called_once_with("/model-price-map")

    def test_list_price_maps_non_list_response(self, pricing_migrator, mock_api_client):
        """A non-list response degrades gracefully to an empty list."""
        mock_api_client.get.return_value = {"detail": "unexpected"}

        assert pricing_migrator.list_price_maps() == []

    def test_create_price_map_strips_server_assigned_fields(
        self, pricing_migrator, mock_api_client, sample_config, custom_entry
    ):
        """Payload must omit id/tenant_id/priority_order (server-assigned)."""
        sample_config.migration.dry_run = False
        # find_existing -> none; create -> success
        mock_api_client.get.return_value = []
        mock_api_client.post.return_value = {"id": "price-dest-1"}

        result = pricing_migrator.create_price_map(custom_entry)

        assert result == "price-dest-1"
        endpoint, payload = mock_api_client.post.call_args[0]
        assert endpoint == "/model-price-map"
        assert "id" not in payload
        assert "tenant_id" not in payload
        assert "priority_order" not in payload
        assert payload["name"] == custom_entry["name"]
        assert payload["match_pattern"] == "claude-sonnet-4-5"
        assert payload["prompt_cost"] == "0.000003"

    def test_create_price_map_dry_run(
        self, pricing_migrator, mock_api_client, sample_config, custom_entry
    ):
        """Dry-run makes no POST and returns a dry-run sentinel id."""
        sample_config.migration.dry_run = True
        mock_api_client.get.return_value = []

        result = pricing_migrator.create_price_map(custom_entry)

        assert result.startswith("dry-run-")
        mock_api_client.post.assert_not_called()

    def test_create_price_map_skip_existing(
        self, pricing_migrator, mock_api_client, sample_config, custom_entry
    ):
        """When an equivalent entry exists and skip_existing is on, skip the POST."""
        sample_config.migration.dry_run = False
        sample_config.migration.skip_existing = True
        existing = {**custom_entry, "id": "price-dest-existing", "tenant_id": "ws-dest"}
        mock_api_client.get.return_value = [existing]

        result = pricing_migrator.create_price_map(custom_entry)

        assert result == "price-dest-existing"
        mock_api_client.post.assert_not_called()

    def test_create_price_map_updates_when_not_skipping(
        self, pricing_migrator, mock_api_client, sample_config, custom_entry
    ):
        """When an equivalent entry exists and skip_existing is off, PUT-update it."""
        sample_config.migration.dry_run = False
        sample_config.migration.skip_existing = False
        existing = {**custom_entry, "id": "price-dest-existing", "tenant_id": "ws-dest"}
        mock_api_client.get.return_value = [existing]
        mock_api_client.put.return_value = {"id": "price-dest-existing"}

        result = pricing_migrator.create_price_map(custom_entry)

        assert result == "price-dest-existing"
        endpoint, _ = mock_api_client.put.call_args[0]
        assert endpoint == "/model-price-map/price-dest-existing"
        mock_api_client.post.assert_not_called()

    def test_find_existing_ignores_global_builtin_entries(
        self, pricing_migrator, mock_api_client, custom_entry
    ):
        """A global built-in (tenant_id null) with matching conditions must NOT
        be treated as an existing destination entry. Otherwise the migrator would
        PUT against a row that doesn't exist in the tenant and get a 500."""
        global_match = {
            "id": "global-builtin-id",
            "tenant_id": None,
            "match_path": custom_entry["match_path"],
            "match_pattern": custom_entry["match_pattern"],
            "provider": custom_entry["provider"],
        }
        mock_api_client.get.return_value = [global_match]

        assert pricing_migrator.find_existing(custom_entry) is None

    def test_create_price_map_creates_when_only_global_matches(
        self, pricing_migrator, mock_api_client, sample_config, custom_entry
    ):
        """When the destination has only a matching global built-in (no custom
        entry), the migrator should CREATE a new custom entry, not update."""
        sample_config.migration.dry_run = False
        global_match = {
            "id": "global-builtin-id",
            "tenant_id": None,
            "match_path": custom_entry["match_path"],
            "match_pattern": custom_entry["match_pattern"],
            "provider": custom_entry["provider"],
        }
        mock_api_client.get.return_value = [global_match]
        mock_api_client.post.return_value = {"id": "price-dest-new"}

        result = pricing_migrator.create_price_map(custom_entry)

        assert result == "price-dest-new"
        mock_api_client.post.assert_called_once()
        mock_api_client.put.assert_not_called()

    def test_create_price_map_conflict_is_idempotent(
        self, pricing_migrator, mock_api_client, sample_config, custom_entry
    ):
        """A 409 from POST is swallowed and treated as already-exists."""
        sample_config.migration.dry_run = False
        sample_config.migration.skip_existing = False
        existing = {**custom_entry, "id": "price-dest-existing", "tenant_id": "ws-dest"}
        # First find_existing (pre-POST) returns nothing; post 409s; second
        # find_existing (post-conflict) resolves the id.
        mock_api_client.get.side_effect = [[], [existing]]
        mock_api_client.post.side_effect = ConflictError(
            "Resource conflict", request_info={}
        )

        result = pricing_migrator.create_price_map(custom_entry)

        assert result == "price-dest-existing"
