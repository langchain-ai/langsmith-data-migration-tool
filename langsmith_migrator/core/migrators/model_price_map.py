"""Custom model pricing (model-price-map) migration logic."""

from typing import Any, Dict, List, Optional

from .base import BaseMigrator

# Fields accepted by the destination create endpoint (ModelPriceMapCreateSchema).
# id / tenant_id / priority_order are assigned server-side and must not be sent.
_CREATE_FIELDS = (
    "name",
    "start_time",
    "match_path",
    "match_pattern",
    "prompt_cost",
    "completion_cost",
    "prompt_cost_details",
    "completion_cost_details",
    "provider",
)


def _match_identity(entry: Dict[str, Any]) -> tuple:
    """Identity used to detect an equivalent entry already on the destination.

    Mirrors the server's uniqueness on match conditions (match_pattern +
    match_path + provider).
    """
    match_path = entry.get("match_path") or []
    return (
        entry.get("match_pattern"),
        tuple(match_path),
        entry.get("provider"),
    )


class ModelPriceMapMigrator(BaseMigrator):
    """Handles custom model pricing (model-price-map) migration."""

    def list_price_maps(self) -> List[Dict[str, Any]]:
        """List workspace-custom model price entries from the source.

        ``GET /model-price-map`` returns both the workspace's custom entries
        (``tenant_id`` set) and the global built-ins (``tenant_id`` is null).
        Only the custom entries are migratable, so the built-ins are dropped -
        recreating them would be redundant and would 409 against the
        destination's own built-ins.
        """
        try:
            response = self.source.get("/model-price-map")
        except Exception as e:
            self.log(f"Failed to list model price maps: {e}", "warning")
            return []

        if not isinstance(response, list):
            self.log(
                f"Unexpected model-price-map response type: {type(response)}", "warning"
            )
            return []

        return [
            entry
            for entry in response
            if isinstance(entry, dict) and entry.get("tenant_id") is not None
        ]

    def find_existing(self, entry: Dict[str, Any]) -> Optional[str]:
        """Return the id of an equivalent entry on the destination, if any."""
        try:
            response = self.dest.get("/model-price-map")
        except Exception as e:
            self.log(f"Failed to check for existing model price map: {e}", "warning")
            return None

        if not isinstance(response, list):
            return None

        target = _match_identity(entry)
        for existing in response:
            if not isinstance(existing, dict):
                continue
            # Only match the destination's own custom entries. Global built-ins
            # (tenant_id is null) share match conditions with custom entries but
            # are not updatable per-tenant - matching one would yield its id and a
            # PUT against a row that doesn't exist in this tenant (500).
            if existing.get("tenant_id") is None:
                continue
            if _match_identity(existing) == target:
                return existing.get("id")
        return None

    def _build_payload(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Build the create/update payload, copying only server-accepted fields."""
        return {field: entry[field] for field in _CREATE_FIELDS if field in entry}

    def update_price_map(self, price_map_id: str, entry: Dict[str, Any]) -> None:
        """Update an existing model price entry on the destination."""
        if self.config.migration.dry_run:
            self.log(
                f"[DRY RUN] Would update model price: {entry.get('name')} ({price_map_id})"
            )
            return

        self.dest.put(f"/model-price-map/{price_map_id}", self._build_payload(entry))
        self.log(
            f"Updated model price: {entry.get('name')} ({price_map_id})", "success"
        )

    def create_price_map(self, entry: Dict[str, Any]) -> str:
        """Create (or update) a model price entry on the destination."""
        name = entry.get("name") or "unnamed"

        existing_id = self.find_existing(entry)
        if existing_id:
            if self.config.migration.skip_existing:
                self.log(f"Model price '{name}' already exists, skipping", "warning")
                return existing_id
            self.log(f"Model price '{name}' exists, updating...", "info")
            self.update_price_map(existing_id, entry)
            return existing_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create model price: {name}")
            return f"dry-run-{entry.get('id', name)}"

        from ..api_client import APIError, ConflictError

        try:
            response = self.dest.post("/model-price-map", self._build_payload(entry))
        except ConflictError:
            # Server enforces uniqueness on match conditions. A conflict means an
            # equivalent entry already exists on the destination, so the migration
            # goal is already satisfied - treat as idempotent success.
            self.log(
                f"Model price '{name}' already exists on destination, skipping",
                "warning",
            )
            return self.find_existing(entry) or ""

        if not isinstance(response, dict) or "id" not in response:
            raise APIError(
                f"Invalid response creating model price '{name}': {response}"
            )

        return response["id"]
