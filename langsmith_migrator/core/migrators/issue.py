"""LangSmith Engine issues and issues-agent migration logic.

Migrates two LangSmith Engine resource types served by the Go backend
under ``/v1/platform/*``:

- **Issues agent** (per-project engine configuration) at
  ``/v1/platform/sessions/{session_id}/issues-agent``.
- **Issues** (the detected/clustered issues) at ``/v1/platform/issues``.

Trace/run links are intentionally NOT migrated. Issues anchor to runs via
``run_id``/``trace_id`` (see ``tracer_session_issue_runs`` + mirrored feedback
rows). Trace data is not portable across instances and the destination's
``RunResolver`` validates every ``run_id`` against its own run store, so
re-linking runs from the source would be rejected. Callers should re-run the
engine on the destination to re-detect run links. See ``README.md``.
"""

from typing import Any, Dict, List, Optional

from .base import BaseMigrator
from ..api_client import APIError, NotFoundError
from ...utils.matching import unique_name_map


class IssueMigrator(BaseMigrator):
    """Handles Engine issues and issues-agent migration."""

    # Fields on an issues-agent config that reference source-instance-only
    # resources (LSD threads/runs, tenant, server-managed counters/timestamps)
    # and must be stripped before recreating on the destination.
    _AGENT_STRIP_FIELDS = frozenset(
        {
            "id",
            "tenant_id",
            "tenant_name",
            "session_id",
            "session_name",
            "issue_count",
            "latest_thread_id",
            "latest_run_id",
            "created_at",
            "updated_at",
        }
    )

    def __init__(self, source_client, dest_client, state, config):
        super().__init__(source_client, dest_client, state, config)
        # Maps source_project_id -> dest_project_id. May be injected by the CLI
        # (from workspace/TUI mapping) or built lazily via build_project_mapping.
        self._project_id_map: Optional[Dict[str, str]] = None

    # ------------------------------------------------------------------
    # Project ID mapping (mirrors RulesMigrator so behavior matches `rules`)
    # ------------------------------------------------------------------
    def _create_project(self, project: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a project (tracer_session) in the destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create project: {project['name']}", "info")
            return {"id": f"dry-run-{project['id']}", "name": project["name"]}

        try:
            payload = {
                "name": project["name"],
                "description": project.get("description"),
                "metadata": project.get("metadata"),
                "start_time": project.get("start_time"),
                "end_time": project.get("end_time"),
                "extra": project.get("extra"),
            }
            response = self.dest.post("/sessions", payload)
            self.log(f"Created project '{project['name']}' in destination", "success")
            return response
        except Exception as e:
            self.log(f"Failed to create project '{project['name']}': {e}", "error")
            return None

    def build_project_mapping(self, create_missing: bool = True) -> Dict[str, str]:
        """Build a source->destination project ID mapping by matching names.

        Matches the ``rules`` command behavior: reuses any state mapping, maps
        exact name matches, and (when ``create_missing``) auto-creates missing
        projects on the destination. Duplicate names on either side are skipped
        with a warning.

        Args:
            create_missing: If True (default), create projects that exist on the
                source but not the destination.

        Returns:
            Dict mapping source_project_id -> dest_project_id.
        """
        if self._project_id_map is not None:
            return self._project_id_map

        self.log("Building project ID mapping...", "info")
        self._project_id_map = {}

        try:
            # Only map real tracing projects, not experiment/test-run sessions.
            # ``reference_free=true`` mirrors the UI's default "Exclude
            # Experiments" filter (WHERE reference_dataset_id IS NULL). The
            # client-side guard defends against a backend that ignores the param.
            source_records: List[Dict[str, Any]] = []
            for project in self.source.get_paginated(
                "/sessions", params={"reference_free": "true"}, page_size=100
            ):
                if isinstance(project, dict) and not project.get("reference_dataset_id"):
                    source_records.append(project)

            dest_records: List[Dict[str, Any]] = []
            for project in self.dest.get_paginated(
                "/sessions", params={"reference_free": "true"}, page_size=100
            ):
                if isinstance(project, dict) and not project.get("reference_dataset_id"):
                    dest_records.append(project)

            _, source_duplicates = unique_name_map(source_records)
            dest_unique, dest_duplicates = unique_name_map(dest_records)
            existing_count = 0
            created_count = 0

            for source_project in source_records:
                source_id = source_project["id"]
                source_name = source_project["name"]

                if self.state and self.state.get_mapped_id("project", source_id):
                    mapped_id = self.state.get_mapped_id("project", source_id)
                    self._project_id_map[source_id] = mapped_id
                    self.record_provenance(f"project:{source_id}", "state_mapping")
                    continue

                if source_name in source_duplicates:
                    self.log(
                        f"Project '{source_name}' is duplicated on source; skipping automatic mapping",
                        "warning",
                    )
                    continue
                if source_name in dest_duplicates:
                    self.log(
                        f"Project '{source_name}' is duplicated on destination; skipping automatic mapping",
                        "warning",
                    )
                    continue

                if source_name in dest_unique:
                    self._project_id_map[source_id] = dest_unique[source_name]
                    self.record_provenance(f"project:{source_id}", "exact_name_match")
                    self.log(
                        f"Mapped project '{source_name}': {source_id} -> {dest_unique[source_name]}",
                        "info",
                    )
                    existing_count += 1
                elif create_missing:
                    self.log(
                        f"Project '{source_name}' not found in destination, creating...", "info"
                    )
                    new_project = self._create_project(source_project)
                    if new_project:
                        self._project_id_map[source_id] = new_project["id"]
                        self.record_provenance(f"project:{source_id}", "created_on_destination")
                        self.log(
                            f"Mapped project '{source_name}': {source_id} -> {new_project['id']}",
                            "info",
                        )
                        created_count += 1
                    else:
                        self.log(
                            f"Failed to create project '{source_name}' in destination", "error"
                        )

            total_mapped = len(self._project_id_map)
            if created_count > 0:
                self.log(
                    f"Built project mapping: {existing_count} existing, "
                    f"{created_count} created, {total_mapped} total",
                    "success",
                )
            else:
                self.log(f"Built project mapping: {total_mapped} projects mapped", "success")

        except Exception as e:
            self.log(f"Failed to build project mapping: {e}", "error")
            self._project_id_map = {}

        return self._project_id_map

    def map_single_project(
        self, source_project: Dict[str, Any], create_missing: bool = True
    ) -> Optional[str]:
        """Map (and optionally create) a single source project on the destination.

        Used to scope a run to one tracing project so the whole workspace is
        not created. Adds the resolved ID to ``self._project_id_map`` and
        returns the destination project ID (or None if it could not be mapped).
        """
        if self._project_id_map is None:
            self._project_id_map = {}

        source_id = source_project["id"]
        source_name = source_project.get("name")
        if source_id in self._project_id_map:
            return self._project_id_map[source_id]

        # Prefer a persisted state mapping if one exists.
        if self.state and self.state.get_mapped_id("project", source_id):
            mapped_id = self.state.get_mapped_id("project", source_id)
            self._project_id_map[source_id] = mapped_id
            self.record_provenance(f"project:{source_id}", "state_mapping")
            return mapped_id

        # Match by name on the destination, restricted to real tracing
        # projects (not experiment sessions), mirroring build_project_mapping.
        try:
            dest_records = [
                p
                for p in self.dest.get_paginated(
                    "/sessions", params={"reference_free": "true"}, page_size=100
                )
                if isinstance(p, dict) and not p.get("reference_dataset_id")
            ]
        except Exception as e:
            self.log(f"Failed to list destination projects: {e}", "error")
            dest_records = []

        for p in dest_records:
            if p.get("name") == source_name:
                self._project_id_map[source_id] = p["id"]
                self.record_provenance(f"project:{source_id}", "exact_name_match")
                return p["id"]

        if not create_missing:
            return None

        self.log(f"Project '{source_name}' not found in destination, creating...", "info")
        new_project = self._create_project(source_project)
        if new_project:
            self._project_id_map[source_id] = new_project["id"]
            self.record_provenance(f"project:{source_id}", "created_on_destination")
            return new_project["id"]
        return None

    def _map_session_id(self, source_session_id: Optional[str]) -> Optional[str]:
        """Resolve a source session (project) ID to its destination ID."""
        if not source_session_id:
            return None
        project_map = self._project_id_map or {}
        return project_map.get(source_session_id)

    # ------------------------------------------------------------------
    # Issues agent (per-project engine config)
    # ------------------------------------------------------------------
    def list_issue_agents(
        self, source_session_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List issues-agent configs from the source workspace.

        When ``source_session_id`` is given, only that project's config is
        returned (via the per-session endpoint); otherwise all configs in the
        workspace are listed.
        """
        agents: List[Dict[str, Any]] = []
        try:
            if source_session_id:
                agent = self.source.get(
                    f"/v1/platform/sessions/{source_session_id}/issues-agent"
                )
                if isinstance(agent, dict) and agent.get("id"):
                    agents.append(agent)
            else:
                for agent in self.source.get_paginated("/v1/platform/issues-agent"):
                    if isinstance(agent, dict):
                        agents.append(agent)
        except NotFoundError:
            # No config for this session, or engine not enabled -- not an error.
            self.log("No issues-agent config found for the requested scope", "info")
        except Exception as e:
            self.log(f"Failed to list issues agents: {e}", "warning")
        return agents

    def find_existing_issue_agent(self, dest_session_id: str) -> Optional[str]:
        """Check if an issues-agent config already exists for a destination project."""
        agent = self.dest_index(
            "_dest_issue_agents",
            "/v1/platform/issues-agent",
            "session_id",
            error_label="issues agent",
        ).get(dest_session_id)
        return agent.get("id") if agent else None

    def create_issue_agent(self, agent: Dict[str, Any]) -> Optional[str]:
        """Create an issues-agent config on the destination.

        The destination project (session) must be mapped. Source-instance-only
        fields (LSD thread/run ids, tenant, counters, timestamps) are stripped.
        GitHub/Context-Hub linkage is carried over but depends on the
        destination having the corresponding integrations configured.

        Returns the destination issues-agent ID, or None if skipped.
        """
        source_session_id = agent.get("session_id")
        dest_session_id = self._map_session_id(source_session_id)
        if not dest_session_id:
            self.log(
                f"Project not found in destination for issues agent "
                f"(source session {source_session_id}); skipping",
                "warning",
            )
            return None

        existing_id = self.find_existing_issue_agent(dest_session_id)
        if existing_id:
            self.log(
                f"Issues agent for project '{agent.get('session_name', dest_session_id)}' "
                "already exists, skipping",
                "warning",
            )
            return existing_id

        if self.config.migration.dry_run:
            self.log(
                f"[DRY RUN] Would create issues agent for project "
                f"'{agent.get('session_name', dest_session_id)}'"
            )
            return f"dry-run-{agent.get('id', dest_session_id)}"

        payload = {
            k: v
            for k, v in agent.items()
            if k not in self._AGENT_STRIP_FIELDS and v is not None
        }

        response = self.dest.post(
            f"/v1/platform/sessions/{dest_session_id}/issues-agent", payload
        )

        if not isinstance(response, dict):
            raise APIError(
                f"Invalid response creating issues agent: expected dict, got {type(response)}"
            )

        self.register_dest_item("_dest_issue_agents", dest_session_id, response)
        self.log(
            f"Created issues agent for project "
            f"'{agent.get('session_name', dest_session_id)}'",
            "success",
        )
        return response.get("id")

    # ------------------------------------------------------------------
    # Issues (detected/clustered issues)
    # ------------------------------------------------------------------
    def list_issues(self, source_session_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List issues from the source, optionally scoped to one project.

        ``GET /v1/platform/issues`` returns a bare array using limit/offset
        paging (capped at 500 per page), which ``get_paginated`` handles.
        """
        issues: List[Dict[str, Any]] = []
        params = {"session_id": source_session_id} if source_session_id else None
        try:
            for issue in self.source.get_paginated(
                "/v1/platform/issues", params=params, page_size=500
            ):
                if isinstance(issue, dict):
                    issues.append(issue)
        except NotFoundError:
            self.log("Issues endpoint not found (engine may not be enabled)", "warning")
        except Exception as e:
            self.log(f"Failed to list issues: {e}", "warning")
        return issues

    def _dest_issue_index(self, dest_session_id: str) -> Dict[str, Dict[str, Any]]:
        """Return a cached ``{name: issue}`` index of destination issues for a project.

        Issue ``name`` is unique within a session, so ``(session_id, name)`` is
        used as the dedup key. Cached per destination session for the run.
        """
        cache = getattr(self, "_dest_issues_by_session", None)
        if cache is None:
            cache = {}
            self._dest_issues_by_session = cache
        if dest_session_id in cache:
            return cache[dest_session_id]

        index: Dict[str, Dict[str, Any]] = {}
        try:
            for issue in self.dest.get_paginated(
                "/v1/platform/issues",
                params={"session_id": dest_session_id},
                page_size=500,
            ):
                name = issue.get("name") if isinstance(issue, dict) else None
                if name is not None:
                    index.setdefault(name, issue)
        except Exception as e:
            self.log(f"Failed to check for existing issues: {e}", "warning")
            # Do not cache on failure so a later call can retry.
            return index
        cache[dest_session_id] = index
        return index

    def find_existing_issue(
        self, dest_session_id: str, name: Optional[str]
    ) -> Optional[str]:
        """Return the destination issue ID with this name in the project, if any."""
        if not name:
            return None
        existing = self._dest_issue_index(dest_session_id).get(name)
        return existing.get("id") if existing else None

    def create_issue(self, issue: Dict[str, Any]) -> Optional[str]:
        """Create a detected issue on the destination as metadata only.

        The ``traces`` array (linked runs) is intentionally dropped: those runs
        do not exist on the destination and the server would reject the links.
        Run the engine on the destination to re-detect run links.

        An issue with the same name already present in the destination project
        is treated as a duplicate and skipped (its existing ID is returned).

        Returns the destination issue ID, or None if skipped due to an
        unmapped project.
        """
        source_session_id = issue.get("session_id")
        dest_session_id = self._map_session_id(source_session_id)
        if not dest_session_id:
            self.log(
                f"Project not found in destination for issue "
                f"'{issue.get('name', issue.get('id'))}' "
                f"(source session {source_session_id}); skipping",
                "warning",
            )
            return None

        name = issue.get("name")
        existing_id = self.find_existing_issue(dest_session_id, name)
        if existing_id:
            self.log(
                f"Issue '{name}' already exists in destination project, skipping",
                "warning",
            )
            self.last_issue_skipped_existing = True
            return existing_id
        self.last_issue_skipped_existing = False

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create issue: {issue.get('name')}")
            return f"dry-run-{issue.get('id', issue.get('name'))}"

        payload: Dict[str, Any] = {
            "session_id": dest_session_id,
            "name": issue.get("name"),
            "description": issue.get("description"),
            "severity": issue.get("severity"),
        }
        # Optional metadata carried over when present.
        tags = issue.get("tags")
        if tags is not None:
            payload["tags"] = tags

        # NOTE: `traces` (run links) and `actions` are deliberately never
        # included. `traces` reference source runs that do not exist on the
        # destination. `actions` are Engine-generated advisory suggestions
        # (e.g. suggested evaluators) that the destination re-validates
        # strictly on create -- some require a `body` that is not present in
        # the source record -- and that Engine regenerates when it runs on the
        # destination. Sending them causes 400 validation errors.

        response = self.dest.post("/v1/platform/issues", payload)

        if not isinstance(response, dict):
            raise APIError(
                f"Invalid response creating issue: expected dict, got {type(response)}"
            )
        if "id" not in response:
            raise APIError(f"Invalid response creating issue: missing 'id'. Response: {response}")

        dest_issue_id = response["id"]

        # Record in the dedup index so a same-named issue later in this run is
        # also treated as a duplicate.
        cache = getattr(self, "_dest_issues_by_session", None)
        if cache is not None and dest_session_id in cache and name is not None:
            cache[dest_session_id].setdefault(name, response)

        # Issues are created `open`; restore the source status if it differs and
        # the destination supports patching it.
        source_status = issue.get("status")
        if source_status and source_status != response.get("status"):
            try:
                self.dest.patch(
                    f"/v1/platform/issues/{dest_issue_id}", {"status": source_status}
                )
            except Exception as e:
                self.log(
                    f"Created issue '{issue.get('name')}' but failed to set "
                    f"status '{source_status}': {e}",
                    "warning",
                )

        self.log(f"Created issue: {issue.get('name')}", "success")
        return dest_issue_id
