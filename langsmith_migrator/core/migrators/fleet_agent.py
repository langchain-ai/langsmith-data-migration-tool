"""Fleet agent migration logic."""

from typing import Dict, List, Any, Optional
import copy

from .base import BaseMigrator
from ..api_client import NotFoundError


class FleetAgentMigrator(BaseMigrator):
    """Handles migration of Fleet agents with cross-reference remapping."""

    def list_agents(self) -> List[Dict[str, Any]]:
        """List all agent summaries from the source workspace.

        The Fleet API splits agent listing into two audiences: ``user``
        (owned + directly-shared, the default) and ``tenant``
        (workspace-shared). There is no single call that returns both,
        so we issue both and merge with dedup by agent ID.
        """
        agents: List[Dict[str, Any]] = []
        seen_ids: set = set()

        for audience in ("user", "tenant"):
            try:
                for agent in self.source.get_cursor_paginated(
                    "/v1/fleet/agents", params={"audience": audience}
                ):
                    if isinstance(agent, dict):
                        agent_id = agent.get("id")
                        if agent_id and agent_id in seen_ids:
                            continue
                        if agent_id:
                            seen_ids.add(agent_id)
                        agents.append(agent)
            except NotFoundError:
                self.log("Fleet agents endpoint not found", "warning")
            except Exception as e:
                self.log(f"Failed to list Fleet agents (audience={audience}): {e}", "warning")

        return agents

    def list_dest_models(self) -> List[str]:
        """List available model IDs on the destination instance."""
        try:
            resp = self.dest.get("/v1/fleet/models")
            if isinstance(resp, dict):
                items = resp.get("items", [])
                return [m.get("id", "") for m in items if isinstance(m, dict)]
            elif isinstance(resp, list):
                return [m.get("id", "") for m in resp if isinstance(m, dict)]
        except Exception as e:
            self.log(f"Failed to list destination models: {e}", "warning")
        return []

    def get_agent(self, agent_id: str) -> Dict[str, Any]:
        """Get full agent detail with files and system prompt."""
        return self.source.get(f"/v1/fleet/agents/{agent_id}")

    def find_existing_agent(self, name: str) -> Optional[str]:
        """Check if an agent with the same name exists in destination.

        Searches both user and tenant audiences on the destination.
        """
        agent = self.dest_index(
            "_dest_agents",
            "/v1/fleet/agents",
            "name",
            error_label="agent",
            audiences=("user", "tenant"),
        ).get(name)
        return agent.get("id") if agent else None

    def create_agent(
        self,
        agent: Dict[str, Any],
        id_mappings: Dict[str, Dict[str, str]],
        skip_skills: bool = False,
        dest_model_ids: Optional[List[str]] = None,
        dest_user_ids: Optional[set] = None,
    ) -> str:
        """Create or update an agent in the destination workspace.

        Args:
            agent: Full agent record from source (must include files/system_prompt).
            id_mappings: Dictionary of ID mappings from prior migration phases.
                Expected keys: "fleet_skills", "fleet_sandbox_policies",
                "fleet_auth_providers".
            skip_skills: If True, strip skill file entries from the agent payload.
            dest_model_ids: Available model IDs on the destination. If provided,
                the agent's model is validated and stripped if not recognized.
            dest_user_ids: Set of ls_user_id values that exist on the destination
                workspace. If provided, shared_users are filtered to keep only
                valid users. If None, shared_users are stripped entirely.

        Returns the destination agent ID.
        """
        name = agent.get("name", "")
        existing_id = self.find_existing_agent(name)

        if existing_id:
            self.log(f"Agent '{name}' already exists, skipping", "warning")
            return existing_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create agent: {name}")
            return f"dry-run-{agent.get('id', name)}"

        payload = self._build_create_payload(
            agent, id_mappings, skip_skills, dest_model_ids, dest_user_ids
        )

        response = self.dest.post("/v1/fleet/agents", payload)

        if not isinstance(response, dict):
            from ..api_client import APIError
            raise APIError(
                f"Invalid response creating agent: expected dict, got {type(response)}"
            )
        if "id" not in response:
            from ..api_client import APIError
            raise APIError(
                f"Invalid response creating agent: missing 'id'. Response: {response}"
            )

        self.register_dest_item("_dest_agents", name, response)
        return response["id"]

    def _build_create_payload(
        self,
        agent: Dict[str, Any],
        id_mappings: Dict[str, Dict[str, str]],
        skip_skills: bool,
        dest_model_ids: Optional[List[str]] = None,
        dest_user_ids: Optional[set] = None,
    ) -> Dict[str, Any]:
        """Build the POST /fleet/agents payload with remapped references."""
        payload: Dict[str, Any] = {
            "name": agent.get("name", ""),
        }

        description = agent.get("description")
        if description:
            payload["description"] = description

        system_prompt = agent.get("system_prompt")
        if system_prompt:
            payload["system_prompt"] = system_prompt

        permissions = agent.get("permissions")
        if permissions:
            payload["permissions"] = self._remap_permissions(permissions, dest_user_ids)

        model = agent.get("model")
        if model:
            if dest_model_ids is not None:
                model_id = model.get("id", "") if isinstance(model, dict) else str(model)
                if model_id and model_id not in dest_model_ids:
                    replacement = self._find_replacement_model(model_id, dest_model_ids)
                    if replacement:
                        self.log(
                            f"Model '{model_id}' not available on destination, "
                            f"substituting '{replacement}'.",
                            "warning",
                        )
                        payload["model"] = {"id": replacement}
                    else:
                        self.log(
                            f"Model '{model_id}' not available on destination, "
                            f"using first available model '{dest_model_ids[0]}'.",
                            "warning",
                        )
                        payload["model"] = {"id": dest_model_ids[0]}
                else:
                    payload["model"] = model
            else:
                payload["model"] = model
        elif dest_model_ids:
            payload["model"] = {"id": dest_model_ids[0]}

        backend = agent.get("backend")
        if backend:
            payload["backend"] = self._remap_backend(backend, id_mappings)

        display = agent.get("display")
        if display:
            payload["display"] = display

        options = agent.get("options")
        if options:
            payload["options"] = self._remap_options(options, id_mappings)

        tools = agent.get("tools")
        if tools:
            # MCP server URLs are stable external identities. Registering the
            # same URL on the destination makes the copied tool config valid.
            payload["tools"] = copy.deepcopy(tools)

        subagents = agent.get("subagents")
        if subagents:
            payload["subagents"] = copy.deepcopy(subagents)

        files = agent.get("files")
        if files:
            payload["files"] = self._remap_files(
                files, id_mappings, skip_skills
            )

        return payload

    def _remap_files(
        self,
        files: Dict[str, Any],
        id_mappings: Dict[str, Dict[str, str]],
        skip_skills: bool,
    ) -> Dict[str, Any]:
        """Remap skill repo handles in file entries, or strip them if skipped."""
        remapped = {}
        skill_map = id_mappings.get("fleet_skills", {})

        for path, entry in files.items():
            if not isinstance(entry, dict):
                remapped[path] = entry
                continue

            entry_type = entry.get("type", "file")
            if entry_type == "skill" and path.startswith("skills/"):
                if skip_skills:
                    self.log(
                        f"Stripping skill reference '{path}' (skills skipped)",
                        "warning",
                    )
                    continue
                repo_handle = entry.get("repo_handle", "")
                dest_handle = skill_map.get(repo_handle, repo_handle)
                new_entry = copy.deepcopy(entry)
                new_entry["repo_handle"] = dest_handle
                remapped[path] = new_entry
            else:
                remapped[path] = copy.deepcopy(entry)

        return remapped

    def _remap_backend(
        self,
        backend: Dict[str, Any],
        id_mappings: Dict[str, Dict[str, str]],
    ) -> Dict[str, Any]:
        """Remap sandbox policy IDs in backend config."""
        remapped = copy.deepcopy(backend)
        sandbox = remapped.get("sandbox_config")
        if not isinstance(sandbox, dict):
            return remapped

        policy_ids = sandbox.get("policy_ids")
        if not policy_ids:
            return remapped

        policy_map = id_mappings.get("fleet_sandbox_policies", {})
        sandbox["policy_ids"] = [
            policy_map.get(pid, pid) for pid in policy_ids
        ]
        return remapped

    def _remap_options(
        self,
        options: Dict[str, Any],
        id_mappings: Dict[str, Dict[str, str]],
    ) -> Dict[str, Any]:
        """Remap auth provider IDs in agent options."""
        remapped = copy.deepcopy(options)

        slack_provider_id = remapped.get("slack_oauth_provider_id")
        if slack_provider_id:
            provider_map = id_mappings.get("fleet_auth_providers", {})
            mapped = provider_map.get(slack_provider_id)
            if mapped:
                remapped["slack_oauth_provider_id"] = mapped

        return remapped

    def _remap_permissions(
        self,
        permissions: Dict[str, Any],
        dest_user_ids: Optional[set] = None,
    ) -> Dict[str, Any]:
        """Remap permissions for migration.

        Filters ``shared_users`` to keep only user IDs that exist on the
        destination workspace. If ``dest_user_ids`` is None (destination
        members couldn't be fetched), all shared_users are stripped as a
        safety measure. Preserves ``identity``, ``visibility``, and
        ``tenant_access_level``.
        """
        remapped = copy.deepcopy(permissions)

        if "shared_users" not in remapped:
            return remapped

        shared = remapped["shared_users"]
        if not shared or not isinstance(shared, dict):
            return remapped

        if dest_user_ids is None:
            total = sum(
                len(v) for v in shared.values()
                if isinstance(v, list)
            )
            if total > 0:
                self.log(
                    f"Stripping shared_users ({total} user IDs) from agent "
                    f"permissions, could not verify destination users. "
                    f"Re-share manually after migration.",
                    "warning",
                )
            del remapped["shared_users"]
            return remapped

        filtered: Dict[str, Any] = {}
        stripped_count = 0
        for level, user_ids in shared.items():
            if not isinstance(user_ids, list):
                filtered[level] = user_ids
                continue
            kept = [uid for uid in user_ids if uid in dest_user_ids]
            stripped_count += len(user_ids) - len(kept)
            if kept:
                filtered[level] = kept

        if stripped_count > 0:
            self.log(
                f"Filtered shared_users: removed {stripped_count} user ID(s) "
                f"not found on destination workspace.",
                "warning",
            )

        if filtered:
            remapped["shared_users"] = filtered
        else:
            del remapped["shared_users"]

        return remapped

    @staticmethod
    def _find_replacement_model(
        source_model_id: str, dest_model_ids: List[str]
    ) -> Optional[str]:
        """Find a replacement model from the destination catalog.

        Tries to match by provider prefix first (e.g. 'anthropic:' -> any
        anthropic model on the destination), then falls back to None.
        """
        if not dest_model_ids:
            return None

        source_prefix = source_model_id.split(":")[0] if ":" in source_model_id else ""

        if source_prefix:
            for dest_id in dest_model_ids:
                if dest_id.startswith(source_prefix + ":"):
                    return dest_id

        return None
