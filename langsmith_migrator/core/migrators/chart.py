"""Chart migration logic using global API endpoints."""

import datetime
from typing import Dict, List, Any, Optional
from .base import BaseMigrator
from ..api_client import NotFoundError, APIError
from ...utils.matching import unique_name_map


class ChartMigrator(BaseMigrator):
    """Handles chart migration using global /api/v1/charts endpoints."""

    def __init__(self, source_client, dest_client, state, config):
        """Initialize chart migrator with ID mapping caches."""
        super().__init__(source_client, dest_client, state, config)
        self._project_id_map = None  # Lazy-loaded cache
        self._project_mapping_complete = False
        self._dataset_id_map = None  # Lazy-loaded cache
        self._dest_section_map = None # Lazy-loaded cache for dest sections
        self._section_strategy = None

    def _chart_item_id(self, chart: Dict[str, Any]) -> str:
        chart_id = chart.get("id") or chart.get("title") or chart.get("name") or "unknown"
        return f"chart_{chart_id}"

    def probe_capabilities(self) -> Dict[str, Dict[str, Any]]:
        """Probe chart-related capabilities without mutating destination state."""
        capabilities: Dict[str, Dict[str, Any]] = {}

        def record(name: str, supported: Optional[bool], detail: str, probe: str) -> None:
            capabilities[name] = {"supported": supported, "detail": detail, "probe": probe}
            self.record_capability(
                "charts",
                name,
                supported=supported,
                detail=detail,
                probe=probe,
            )

        try:
            self._list_charts(self.source, side="source")
            record("source_chart_list", True, "ok", "POST /charts")
        except Exception as e:
            record("source_chart_list", False, str(e), "POST /charts")

        try:
            destination_response = self._list_charts(self.dest, side="destination")
            record("destination_chart_list", True, "ok", "POST /charts")
            if isinstance(destination_response, list):
                self._section_strategy = "sectioned"
        except Exception as e:
            record("destination_chart_list", False, str(e), "POST /charts")

        record("section_scoped_create", None, "deferred_until_first_write", "POST /charts/section")
        return capabilities

    def _normalize_chart(self, chart_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a chart payload for post-write verification."""
        return {
            "title": chart_data.get("title") or chart_data.get("name"),
            "chart_type": chart_data.get("chart_type"),
            "section_id": chart_data.get("section_id"),
            "common_filters": chart_data.get("common_filters"),
            "series": chart_data.get("series"),
        }

    def _build_chart_payload(self, chart_data: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        """Build a destination chart payload and report whether fidelity was downgraded."""
        chart_title = chart_data.get("title") or chart_data.get("name") or "Untitled Chart"
        payload = {
            "title": chart_title,
            "chart_type": chart_data.get("chart_type", "line"),
            "series": chart_data.get("series", []),
            "description": chart_data.get("description"),
            "index": chart_data.get("index"),
            "metadata": chart_data.get("metadata"),
            "section_id": chart_data.get("section_id"),
            "common_filters": chart_data.get("common_filters"),
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        downgraded = False
        source_section_title = chart_data.get("_source_section_title")
        source_section_desc = chart_data.get("_source_section_description")

        if source_section_title and self._section_strategy != "unsectioned":
            dest_section_id = self._ensure_dest_section(source_section_title, source_section_desc)
            if dest_section_id:
                payload["section_id"] = dest_section_id
            else:
                payload.pop("section_id", None)
                downgraded = True

        return payload, downgraded

    def _verify_chart(self, chart_id: str, payload: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """Verify a chart by refetching destination charts and comparing normalized fields."""
        mismatches = {}
        chart = None
        for existing in self._list_charts(self.dest, side="destination"):
            if existing.get("id") == chart_id:
                chart = existing
                break
        if chart is None:
            return False, {"error": "chart_not_found_after_write"}

        normalized_expected = self._normalize_chart(payload)
        normalized_actual = self._normalize_chart(chart)
        for key, expected in normalized_expected.items():
            if normalized_actual.get(key) != expected:
                mismatches[key] = {
                    "expected": expected,
                    "actual": normalized_actual.get(key),
                }
        return not mismatches, mismatches

    def _export_chart_manual_apply(
        self,
        chart: Dict[str, Any],
        *,
        reason: str,
        payload: Optional[Dict[str, Any]] = None,
        analysis: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Export chart payload and analysis for manual apply."""
        return self.export_payload(
            self._chart_item_id(chart),
            "manual_apply",
            {
                "chart_id": chart.get("id"),
                "title": chart.get("title") or chart.get("name"),
                "reason": reason,
                "payload": payload or chart,
                "analysis": analysis or {},
                "manual_steps": [
                    "Review the missing fields or destination capability mismatch.",
                    "Create or update the chart manually with the exported payload.",
                    "Re-run `langsmith-migrator resume` after the chart exists on destination.",
                ],
            },
        )

    def _enrich_chart(self, chart: Dict[str, Any]) -> Dict[str, Any]:
        """Attempt one richer source fetch when chart metadata is incomplete."""
        if chart.get("series"):
            return chart

        try:
            start_time = (
                datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)
            ).isoformat()
            payload = {
                "timezone": "UTC",
                "omit_data": False,
                "start_time": start_time,
                "end_time": None,
                "stride": {"days": 0, "hours": 0, "minutes": 15},
                "after_index": None,
                "tag_value_id": None,
            }
            response = self.source.post("/charts", payload)
            candidate_charts = []
            if isinstance(response, dict) and "charts" in response:
                candidate_charts = response["charts"]
            elif isinstance(response, list):
                candidate_charts = response
            for candidate in candidate_charts:
                if candidate.get("id") == chart.get("id"):
                    return candidate
        except Exception as e:
            self.log(f"Failed to enrich chart '{chart.get('title') or chart.get('name')}': {e}", "warning")

        return chart

    def list_sessions(self) -> List[Dict[str, Any]]:
        """
        List all sessions (projects) from source.

        Returns:
            List of session objects
        """
        sessions = []
        try:
            self.log("Listing sessions from source...", "info")
            for session in self.source.get_paginated("/sessions", page_size=100):
                sessions.append(session)
            self.log(f"Found {len(sessions)} session(s) in source ✓", "success")
            return sessions
        except Exception as e:
            self.log(f"Failed to list sessions: {e}", "error")
            return []

    def resolve_destination_session_id(
        self,
        source_session_id: Optional[str],
        *,
        same_instance: bool = False,
    ) -> Optional[str]:
        """Resolve a source session/project ID onto the destination instance."""
        if not source_session_id:
            return None

        if same_instance:
            return source_session_id

        if self._project_id_map and source_session_id in self._project_id_map:
            return self._project_id_map[source_session_id]

        if self.state:
            mapped_id = self.state.get_mapped_id("project", source_session_id)
            if mapped_id:
                if self._project_id_map is None:
                    self._project_id_map = {}
                self._project_id_map[source_session_id] = mapped_id
                self.record_provenance(f"project:{source_session_id}", "state_mapping")
                return mapped_id

        return self._build_project_mapping().get(source_session_id)

    @staticmethod
    def _normalize_unresolved_dependencies(
        unresolved: Dict[str, set[str]],
    ) -> Dict[str, List[str]]:
        """Convert unresolved dependency sets into stable JSON-serializable lists."""
        return {
            key: sorted(values)
            for key, values in unresolved.items()
            if values
        }

    def _list_charts(
        self,
        client,
        *,
        session_id: Optional[str] = None,
        side: str = "source",
    ) -> List[Dict[str, Any]]:
        """List charts from the requested instance using POST /api/v1/charts."""
        try:
            # Prepare request body for listing charts according to schema
            # Note: Some versions require start_time even if omit_data is True
            start_time = (
                datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)
            ).isoformat()

            payload = {
                "timezone": "UTC",
                "omit_data": True,  # Only fetch metadata, not full data
                "start_time": start_time,
                "end_time": None,
                "stride": {"days": 0, "hours": 0, "minutes": 15},
                "after_index": None,
                "tag_value_id": None
            }

            response = client.post("/charts", payload)
            charts = []

            # Handle different response formats
            if isinstance(response, list):
                charts = response
            elif isinstance(response, dict):
                # Check for nested sections (dashboard layout)
                if 'sections' in response and isinstance(response['sections'], list):
                    for section in response['sections']:
                        if isinstance(section, dict):
                            section_title = section.get('title')
                            section_desc = section.get('description')
                            # If charts are in the section, extract them
                            if 'charts' in section:
                                for c in section['charts']:
                                    if section_title:
                                        c['_source_section_title'] = section_title
                                    if section_desc:
                                        c['_source_section_description'] = section_desc
                                    charts.append(c)
                # Check for direct charts list
                elif 'charts' in response:
                    charts = response['charts']
                else:
                    # Sometimes the list is the response itself if it's not wrapped
                    # But if it has keys like "detail" it might be error, assumed handled by api_client
                    charts = [response]

            # Filter by session_id if provided
            if session_id and charts:
                filtered_charts = []
                for chart in charts:
                    # Check obvious fields
                    if chart.get('session_id') == session_id or chart.get('project_id') == session_id:
                        filtered_charts.append(chart)
                        continue

                    # Check inside series filters
                    # Series is usually a list of dicts
                    series = chart.get('series', [])
                    matched = False
                    for s in series:
                        if isinstance(s, dict):
                            filters = s.get('filters')
                            if not filters:
                                continue

                            if filters.get('project_id') == session_id or filters.get('session_id') == session_id:
                                filtered_charts.append(chart)
                                matched = True
                                break
                    if not matched:
                        # Some charts might be attached to a section that is attached to a project
                        # But we can't easily resolve that here without more queries.
                        if self.config.migration.verbose:
                            chart_title = chart.get('title') or chart.get('name', 'Untitled')
                            self.log(
                                f"Chart '{chart_title}' filtered out - no matching session_id/project_id "
                                f"in filters (looking for {session_id})",
                                "info"
                            )

                return filtered_charts

            return charts

        except NotFoundError:
            self.log(f"Charts API endpoint not found on {side} (404)", "error")
            return []
        except APIError as e:
            if e.status_code == 405:
                self.log(f"Charts API method not allowed on {side} (405)", "error")
            else:
                self.log(f"Charts API error on {side} ({e.status_code}): {e}", "error")
            return []
        except Exception as e:
            self.log(f"Unexpected error listing charts on {side}: {e}", "error")
            return []

    def list_charts(self, session_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List charts from source using POST /api/v1/charts.

        Args:
            session_id: Optional session ID to filter by

        Returns:
            List of chart configurations
        """
        return self._list_charts(self.source, session_id=session_id, side="source")

    def _ensure_dest_section(self, title: str, description: Optional[str] = None) -> Optional[str]:
        """
        Get existing section ID by title or create a new one.
        """
        if not title:
            return None

        if self._dest_section_map is None:
            self._build_dest_section_map()

        # Check if exists
        if title in self._dest_section_map:
            return self._dest_section_map[title]

        # Create new section
        self.log(f"Creating new dashboard section: '{title}'", "info")
        try:
            payload = {
                "title": title,
                "description": description or "",
                "index": 0 # Default index
            }
            # Use the endpoint identified by user
            response = self.dest.post("/charts/section", payload)

            if isinstance(response, dict) and 'id' in response:
                new_id = response['id']
                self._dest_section_map[title] = new_id # Update cache
                self.log(f"Created section '{title}' -> {new_id}", "success")
                return new_id

        except Exception as e:
            self.log(f"Failed to create section '{title}': {e}", "error")
            self._section_strategy = "unsectioned"
            self.record_capability(
                "charts",
                "section_scoped_create",
                supported=False,
                detail=str(e),
                probe="POST /charts/section",
            )

        return None

    def _get_dest_section_id(self, section_title: str) -> Optional[str]:
        """Look up a destination section ID by its title."""
        if not section_title:
            return None

        if self._dest_section_map is None:
            self._build_dest_section_map()

        return self._dest_section_map.get(section_title)

    def _build_dest_section_map(self):
        """Fetch destination charts/sections and build a name->id map."""
        self._dest_section_map = {}
        try:
            # We use the same payload as list_charts but against destination
            start_time = (
                datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)
            ).isoformat()
            payload = {
                "timezone": "UTC",
                "omit_data": True,
                "start_time": start_time,
                "stride": {"days": 0, "hours": 0, "minutes": 15}
            }

            response = self.dest.post("/charts", payload)

            if isinstance(response, dict) and 'sections' in response:
                for section in response['sections']:
                    if isinstance(section, dict):
                        title = section.get('title')
                        sec_id = section.get('id')
                        if title and sec_id:
                            self._dest_section_map[title] = sec_id

            self.log(f"Built destination section map: {list(self._dest_section_map.keys())}", "info")

        except Exception as e:
            self.log(f"Failed to build destination section map: {e}", "warning")

    def find_existing_chart(self, title: str, section_id: Optional[str] = None) -> Optional[str]:
        """
        Check if a chart with the same title already exists in destination.

        Args:
            title: Chart title
            section_id: Optional section ID to narrow the search

        Returns:
            The chart ID if found, None otherwise
        """
        try:
            charts = self._list_charts(self.dest, side="destination")

            for chart in charts:
                chart_title = chart.get("title") or chart.get("name")
                chart_section = chart.get("section_id")

                # Match by title, and optionally by section
                if chart_title == title:
                    if section_id is None or chart_section == section_id:
                        return chart.get("id")

            return None
        except Exception as e:
            self.log(f"Failed to check for existing chart: {e}", "warning")
            return None

    def update_chart(self, chart_id: str, chart_data: Dict[str, Any]) -> bool:
        """
        Update an existing chart in destination.

        Args:
            chart_id: The chart ID to update
            chart_data: Chart configuration dict

        Returns:
            True if successful, False otherwise
        """
        if self.config.migration.dry_run:
            chart_title = chart_data.get("title") or chart_data.get("name") or "Untitled Chart"
            self.log(f"[DRY RUN] Would update chart: {chart_title} ({chart_id})")
            return True

        payload, _ = self._build_chart_payload(chart_data)

        try:
            # Use PATCH to update the chart
            self.dest.patch(f"/charts/{chart_id}", payload)
            chart_title = chart_data.get("title") or chart_data.get("name") or "Untitled Chart"
            self.log(f"Updated chart: {chart_title} ({chart_id})", "success")
            return True
        except Exception as e:
            chart_title = chart_data.get("title") or chart_data.get("name") or "Untitled Chart"
            self.log(f"Failed to update chart '{chart_title}': {e}", "error")
            return False

    def create_chart(self, chart_data: Dict[str, Any]) -> Optional[str]:
        """
        Create or update a chart in destination using POST /api/v1/charts/create or PATCH.

        Args:
            chart_data: Chart configuration dict

        Returns:
            The created/updated chart ID or None if failed
        """
        chart_title = chart_data.get("title") or chart_data.get("name") or "Untitled Chart"
        item_id = self._chart_item_id(chart_data)
        self.ensure_item(
            item_id,
            "chart",
            chart_title,
            str(chart_data.get("id") or chart_title),
            stage="prepare_chart",
            strategy=self._section_strategy or "sectioned",
            metadata={"title": chart_title},
        )

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create chart: {chart_title}", "info")
            return f"dry-run-{chart_data.get('id', 'chart-id')}"

        chart_copy = self._enrich_chart(chart_data)
        if chart_data.get("_source_section_title") and not chart_copy.get("_source_section_title"):
            chart_copy["_source_section_title"] = chart_data.get("_source_section_title")
        if chart_data.get("_source_section_description") and not chart_copy.get("_source_section_description"):
            chart_copy["_source_section_description"] = chart_data.get("_source_section_description")

        payload, downgraded_to_unsectioned = self._build_chart_payload(chart_copy)
        source_section_title = chart_copy.get("_source_section_title")
        self.checkpoint_item(
            item_id,
            stage="ready_to_write",
            strategy=self._section_strategy or "sectioned",
            metadata={"source_chart_id": chart_data.get("id")},
        )

        # Validate required fields
        if not payload["series"]:
            export_path = self._export_chart_manual_apply(
                chart_copy,
                reason="missing_required_series",
                payload=payload,
                analysis={"missing_fields": ["series"]},
            )
            issue = self.record_issue(
                "source_data_gap",
                "missing_required_series",
                f"Chart '{chart_title}' is missing series data after enrichment",
                item_id=item_id,
                next_action="Review the exported chart payload and missing fields, then re-run `langsmith-migrator resume`.",
                evidence={"title": chart_title},
                export_path=export_path,
            )
            if issue:
                self.queue_remediation(
                    issue_id=issue.id,
                    item_id=item_id,
                    next_action=issue.next_action or "Review chart payload.",
                    export_path=export_path,
                    command="langsmith-migrator resume",
                )
            self.mark_exported(
                item_id,
                "missing_required_series",
                next_action="Review the exported chart payload and missing fields, then run `langsmith-migrator resume`.",
                export_path=export_path,
                evidence={"title": chart_title},
            )
            return None

        # Check if chart already exists
        existing_id = self.find_existing_chart(chart_title, payload.get("section_id"))

        if existing_id:
            if self.config.migration.skip_existing:
                self.log(f"Chart '{chart_title}' already exists, skipping", "warning")
                self.mark_migrated(
                    item_id,
                    outcome_code="chart_already_exists",
                    evidence={"chart_id": existing_id},
                )
                return existing_id
            else:
                self.log(f"Chart '{chart_title}' exists, updating...", "info")
                self.checkpoint_item(item_id, stage="update_chart")
                success = self.update_chart(existing_id, chart_copy)
                if not success:
                    issue = self.record_issue(
                        "transient",
                        "chart_update_failed",
                        f"Chart '{chart_title}' could not be updated on the destination instance",
                        item_id=item_id,
                        next_action="Review the chart payload and retry with `langsmith-migrator resume`.",
                        evidence={"chart_id": existing_id},
                    )
                    if issue:
                        self.queue_remediation(
                            issue_id=issue.id,
                            item_id=item_id,
                            next_action=issue.next_action or "Retry chart update.",
                            command="langsmith-migrator resume",
                        )
                    self.mark_blocked(
                        item_id,
                        "chart_update_failed",
                        next_action="Review the chart payload and retry with `langsmith-migrator resume`.",
                        evidence={"chart_id": existing_id},
                    )
                    return None
                verified, mismatches = self._verify_chart(existing_id, payload)
                if verified:
                    if downgraded_to_unsectioned:
                        self.mark_degraded(
                            item_id,
                            "chart_updated_without_section",
                            next_action="Review chart placement if section grouping is required.",
                            evidence={"chart_id": existing_id},
                        )
                    else:
                        self.mark_migrated(
                            item_id,
                            outcome_code="chart_updated",
                            evidence={"chart_id": existing_id},
                        )
                else:
                    issue = self.record_issue(
                        "post_write_verification",
                        "chart_update_verification_failed",
                        f"Chart '{chart_title}' did not match the requested payload after update",
                        item_id=item_id,
                        next_action="Review the chart mismatches in the remediation bundle.",
                        evidence=mismatches,
                    )
                    if issue:
                        self.queue_remediation(
                            issue_id=issue.id,
                            item_id=item_id,
                            next_action=issue.next_action or "Review chart mismatches.",
                            command="langsmith-migrator resume",
                        )
                    self.mark_degraded(
                        item_id,
                        "chart_update_verification_mismatch",
                        next_action="Review chart field mismatches in the remediation bundle.",
                        evidence=mismatches,
                    )
                return existing_id

        try:
            self.checkpoint_item(item_id, stage="create_chart")
            # Try to create
            try:
                response = self.dest.post("/charts/create", payload)
                self.record_capability(
                    "charts",
                    "section_scoped_create",
                    supported="section_id" in payload,
                    detail="ok",
                    probe="POST /charts/create",
                )
            except APIError as e:
                # If failed and we have section_id, try removing it
                if "section_id" in payload:
                    self.log(f"First attempt failed ({e}), retrying without section_id", "info")
                    self.record_capability(
                        "charts",
                        "section_scoped_create",
                        supported=False,
                        detail=str(e),
                        probe="POST /charts/create",
                    )
                    self._section_strategy = "unsectioned"
                    del payload["section_id"]
                    downgraded_to_unsectioned = True
                    response = self.dest.post("/charts/create", payload)
                else:
                    raise e

            # Response might be the chart object or just ID
            if isinstance(response, dict):
                chart_id = response.get("id")
            else:
                chart_id = None

            if chart_id:
                verified, mismatches = self._verify_chart(str(chart_id), payload)
                if verified:
                    if downgraded_to_unsectioned or (source_section_title and "section_id" not in payload):
                        self.mark_degraded(
                            item_id,
                            "chart_created_without_section",
                            next_action="Review the chart placement if section grouping is required.",
                            evidence={"chart_id": chart_id},
                        )
                    else:
                        self.mark_migrated(
                            item_id,
                            outcome_code="chart_migrated",
                            evidence={"chart_id": chart_id},
                        )
                else:
                    issue = self.record_issue(
                        "post_write_verification",
                        "chart_create_verification_failed",
                        f"Chart '{chart_title}' did not match the requested payload after creation",
                        item_id=item_id,
                        next_action="Review the chart mismatches in the remediation bundle.",
                        evidence=mismatches,
                    )
                    if issue:
                        self.queue_remediation(
                            issue_id=issue.id,
                            item_id=item_id,
                            next_action=issue.next_action or "Review chart mismatches.",
                            command="langsmith-migrator resume",
                        )
                    self.mark_degraded(
                        item_id,
                        "chart_create_verification_mismatch",
                        next_action="Review chart field mismatches in the remediation bundle.",
                        evidence=mismatches,
                    )
                return str(chart_id)

            issue = self.record_issue(
                "post_write_verification",
                "chart_create_missing_id",
                f"Chart '{chart_title}' create response did not include a destination ID",
                item_id=item_id,
                next_action="Review the destination chart response and retry with `langsmith-migrator resume`.",
                evidence={"response_type": type(response).__name__},
            )
            if issue:
                self.queue_remediation(
                    issue_id=issue.id,
                    item_id=item_id,
                    next_action=issue.next_action or "Review chart create response.",
                    command="langsmith-migrator resume",
                )
            self.mark_blocked(
                item_id,
                "chart_create_missing_id",
                next_action="Review the destination chart response and retry with `langsmith-migrator resume`.",
                evidence={"response_type": type(response).__name__},
            )
            return None

        except APIError as e:
            self.log(f"Failed to create chart '{chart_title}': {e}", "error")
            export_path = self._export_chart_manual_apply(
                chart_copy,
                reason="chart_create_failed",
                payload=payload,
                analysis={"error": str(e)},
            )
            issue = self.record_issue(
                "capability",
                "chart_create_failed",
                f"Chart '{chart_title}' could not be created on the destination instance",
                item_id=item_id,
                next_action="Review the exported chart payload and destination chart capabilities, then re-run `langsmith-migrator resume`.",
                evidence={"error": str(e)},
                export_path=export_path,
            )
            if issue:
                self.queue_remediation(
                    issue_id=issue.id,
                    item_id=item_id,
                    next_action=issue.next_action or "Review exported chart payload.",
                    export_path=export_path,
                    command="langsmith-migrator resume",
                )
            self.mark_exported(
                item_id,
                "chart_create_failed",
                next_action="Review the exported chart payload, then run `langsmith-migrator resume`.",
                export_path=export_path,
                evidence={"error": str(e)},
            )
            return None
        except Exception as e:
            self.log(f"Unexpected error creating chart '{chart_title}': {e}", "error")
            issue = self.record_issue(
                "transient",
                "chart_migration_failed",
                f"Chart '{chart_title}' failed during migration",
                item_id=item_id,
                next_action="Re-run `langsmith-migrator resume` after reviewing the error.",
                evidence={"error": str(e)},
            )
            if issue:
                self.queue_remediation(
                    issue_id=issue.id,
                    item_id=item_id,
                    next_action=issue.next_action or "Retry chart migration.",
                    command="langsmith-migrator resume",
                )
            self.mark_blocked(
                item_id,
                "chart_migration_failed",
                next_action="Re-run `langsmith-migrator resume` after reviewing the error.",
                evidence={"error": str(e)},
            )
            return None

    def migrate_chart(
        self,
        chart: Dict[str, Any],
        dest_session_id: Optional[str] = None,
        *,
        same_instance: bool = False,
    ) -> Optional[str]:
        """
        Migrate a single chart.

        Args:
            chart: Source chart configuration
            dest_session_id: Optional destination session ID to enforce in filters

        Returns:
            New chart ID in destination or None if failed
        """
        chart_title = chart.get("title") or chart.get("name") or "Untitled"
        chart_id = chart.get("id")
        item_id = self._chart_item_id(chart)
        self.ensure_item(
            item_id,
            "chart",
            chart_title,
            str(chart_id or chart_title),
            stage="map_dependencies",
            strategy=self._section_strategy or "sectioned",
            metadata={"dest_session_id": dest_session_id},
        )

        if not chart_id:
            self.log(f"Chart '{chart_title}' missing ID, skipping", "warning")
            export_path = self._export_chart_manual_apply(
                chart,
                reason="missing_chart_id",
                analysis={"missing_fields": ["id"]},
            )
            issue = self.record_issue(
                "source_data_gap",
                "missing_chart_id",
                f"Chart '{chart_title}' is missing its source identifier",
                item_id=item_id,
                next_action="Review the exported chart payload and migrate it manually before resuming.",
                evidence={"title": chart_title},
                export_path=export_path,
            )
            if issue:
                self.queue_remediation(
                    issue_id=issue.id,
                    item_id=item_id,
                    next_action=issue.next_action or "Review exported chart payload.",
                    export_path=export_path,
                    command="langsmith-migrator resume",
                )
            self.mark_exported(
                item_id,
                "missing_chart_id",
                next_action="Review the exported chart payload and migrate it manually before resuming.",
                export_path=export_path,
                evidence={"title": chart_title},
            )
            return None

        # Deep copy
        import copy
        chart_copy = copy.deepcopy(chart)

        # Map IDs within chart config
        source_session_id = self._extract_session_id(chart)
        unresolved_dependencies = self._map_ids_in_chart(
            chart_copy,
            dest_session_id,
            same_instance=same_instance,
            source_session_id=source_session_id,
        )
        unresolved_dependencies = self._normalize_unresolved_dependencies(
            unresolved_dependencies
        )
        if unresolved_dependencies:
            self.log(
                f"Chart '{chart_title}' has unresolved dependencies: "
                f"{unresolved_dependencies}",
                "warning",
            )
            export_path = self._export_chart_manual_apply(
                chart_copy,
                reason="unresolved_chart_dependencies",
                analysis={
                    "unresolved_dependencies": unresolved_dependencies,
                    "dest_session_id": dest_session_id,
                },
            )
            issue = self.record_issue(
                "dependency",
                "unresolved_chart_dependencies",
                f"Chart '{chart_title}' could not resolve project/session/dataset dependencies",
                item_id=item_id,
                next_action=(
                    "Provide the missing mappings or migrate the missing dependencies, "
                    "then run `langsmith-migrator resume`."
                ),
                evidence=unresolved_dependencies,
                export_path=export_path,
            )
            if issue:
                self.queue_remediation(
                    issue_id=issue.id,
                    item_id=item_id,
                    next_action=issue.next_action or "Resolve chart dependencies.",
                    export_path=export_path,
                    command="langsmith-migrator resume",
                )
            self.mark_exported(
                item_id,
                "unresolved_chart_dependencies",
                next_action=(
                    "Resolve the chart dependency mappings, then run "
                    "`langsmith-migrator resume`."
                ),
                export_path=export_path,
                evidence=unresolved_dependencies,
            )
            return None
        self.checkpoint_item(
            item_id,
            stage="mapped_dependencies",
            metadata={"dest_session_id": dest_session_id},
        )

        # Create in destination
        try:
            new_id = self.create_chart(chart_copy)
            return new_id
        except Exception as e:
            self.log(f"Failed to migrate chart '{chart_title}': {e}", "error")
            return None

    def migrate_session_charts(
        self,
        source_session_id: str,
        dest_session_id: str,
        *,
        same_instance: bool = False,
    ) -> Dict[str, str]:
        """
        Migrate all charts for a specific session.

        Args:
            source_session_id: Source session ID
            dest_session_id: Destination session ID

        Returns:
            Dictionary mapping source chart IDs to destination chart IDs
        """
        id_mapping = {}

        # List charts filtered by source session
        charts = self.list_charts(source_session_id)

        if not charts:
            self.log("  - No charts found for session", "info")
            return id_mapping

        self.log(f"  Processing {len(charts)} chart(s)...", "info")

        success_count = 0
        failed_count = 0

        for chart in charts:
            chart_id = chart.get("id")
            if not chart_id:
                continue

            new_id = self.migrate_chart(
                chart,
                dest_session_id,
                same_instance=same_instance,
            )

            if new_id:
                id_mapping[chart_id] = new_id
                success_count += 1
            else:
                failed_count += 1

        if success_count > 0:
            self.log(f"  ✓ Migrated {success_count} chart(s)", "success")
        if failed_count > 0:
            self.log(f"  ✗ Failed to migrate {failed_count} chart(s)", "warning")

        return id_mapping

    def migrate_all_charts(self, same_instance: bool = False) -> Dict[str, Dict[str, str]]:
        """
        Migrate all charts from all sessions.

        Args:
            same_instance: If True, assumes source and dest have same session IDs.

        Returns:
            Dict mapping session_id -> {source_chart_id -> dest_chart_id}
        """
        # Note: The structure of the return value assumes we can group by session_id.
        # But charts list is flat. We'll group them ourselves.

        all_mappings = {}  # session_id -> map
        global_map = {} # chart_id -> new_id (for fallback)

        self.log("Fetching all charts from source...", "info")
        charts = self.list_charts()

        if not charts:
            self.log("No charts found in source", "info")
            return {}

        self.log(f"Found {len(charts)} total charts to migrate", "info")

        success_count = 0
        failed_count = 0

        for chart in charts:
            chart_id = chart.get("id")
            if not chart_id:
                continue

            # Determine source session/project ID for this chart
            source_session_id = self._extract_session_id(chart)
            dest_session_id = self.resolve_destination_session_id(
                source_session_id,
                same_instance=same_instance,
            )

            # Migrate
            new_id = self.migrate_chart(
                chart,
                dest_session_id,
                same_instance=same_instance,
            )

            if new_id:
                success_count += 1
                global_map[chart_id] = new_id

                # Add to grouped result if possible
                if source_session_id:
                    if source_session_id not in all_mappings:
                        all_mappings[source_session_id] = {}
                    all_mappings[source_session_id][chart_id] = new_id
            else:
                failed_count += 1

        self.log("\nChart Migration Summary:", "info")
        self.log(f"  Charts migrated: {success_count}", "success" if success_count > 0 else "info")
        if failed_count > 0:
            self.log(f"  Charts failed: {failed_count}", "warning")

        # Return grouped mappings, or just a generic group if none found
        if not all_mappings and global_map:
            all_mappings['unknown_session'] = global_map

        return all_mappings

    def _extract_session_id(self, chart: Dict[str, Any]) -> Optional[str]:
        """Extract session/project ID from chart config."""
        found = self._find_session_dependency(chart)
        if found:
            return found
        return None

    def _find_session_dependency(self, obj: Any) -> Optional[str]:
        """Find the first project/session dependency in a chart filter tree."""
        if isinstance(obj, dict):
            for key in ("session_id", "project_id"):
                value = obj.get(key)
                if isinstance(value, str) and value:
                    return value

            sessions = obj.get("session")
            if isinstance(sessions, list):
                for value in sessions:
                    if isinstance(value, str) and value:
                        return value

            for value in obj.values():
                found = self._find_session_dependency(value)
                if found:
                    return found

        elif isinstance(obj, list):
            for item in obj:
                found = self._find_session_dependency(item)
                if found:
                    return found

        return None

    def _map_ids_in_chart(
        self,
        obj: Any,
        dest_session_id: Optional[str] = None,
        unresolved: Optional[Dict[str, set[str]]] = None,
        *,
        same_instance: bool = False,
        source_session_id: Optional[str] = None,
    ) -> Dict[str, set[str]]:
        """
        Recursively map project and dataset IDs within a chart object.
        Modifies the object in-place.

        Args:
            obj: Chart object or sub-structure
            dest_session_id: If provided, used as a fallback for the chart's source
                             project/session dependency when no mapping is available.
        """
        if unresolved is None:
            unresolved = {
                "project_id": set(),
                "session_id": set(),
                "dataset_id": set(),
            }

        if isinstance(obj, dict):
            # Check for specific keys to map
            if "project_id" in obj:
                self._map_project_session_field(
                    obj,
                    "project_id",
                    dest_session_id,
                    unresolved,
                    same_instance=same_instance,
                    source_session_id=source_session_id,
                )

            if "session_id" in obj:
                self._map_project_session_field(
                    obj,
                    "session_id",
                    dest_session_id,
                    unresolved,
                    same_instance=same_instance,
                    source_session_id=source_session_id,
                )

            if "dataset_id" in obj:
                self._map_id_field(
                    obj,
                    "dataset_id",
                    self._build_dataset_mapping(),
                    unresolved,
                    preserve_unmapped=same_instance,
                )

            if "session" in obj and isinstance(obj["session"], list):
                # Map list of session IDs (used in common_filters)
                new_ids = []
                mapping = {} if same_instance else self._build_project_mapping()
                for old_id in obj["session"]:
                    if same_instance:
                        new_ids.append(old_id)
                    elif isinstance(old_id, str) and old_id in mapping:
                        new_ids.append(mapping[old_id])
                    elif (
                        isinstance(old_id, str)
                        and dest_session_id
                        and (not source_session_id or old_id == source_session_id)
                    ):
                        new_ids.append(dest_session_id)
                    else:
                        if isinstance(old_id, str) and old_id:
                            unresolved["session_id"].add(old_id)
                        new_ids.append(old_id)
                obj["session"] = new_ids

            # Handle special 'tag_value_id' which might be project ID in some contexts
            # But skipping for now as it's ambiguous

            # Recurse into values
            for key, value in obj.items():
                self._map_ids_in_chart(
                    value,
                    dest_session_id,
                    unresolved,
                    same_instance=same_instance,
                    source_session_id=source_session_id,
                )

        elif isinstance(obj, list):
            for item in obj:
                self._map_ids_in_chart(
                    item,
                    dest_session_id,
                    unresolved,
                    same_instance=same_instance,
                    source_session_id=source_session_id,
                )

        return unresolved

    def _map_project_session_field(
        self,
        obj: Dict,
        field: str,
        dest_session_id: Optional[str],
        unresolved: Dict[str, set[str]],
        *,
        same_instance: bool = False,
        source_session_id: Optional[str] = None,
    ) -> None:
        """Map one project/session field without collapsing unrelated dependencies."""
        old_id = obj.get(field)
        if not isinstance(old_id, str) or not old_id:
            return

        if same_instance:
            return

        mapping = self._build_project_mapping()
        if old_id in mapping:
            obj[field] = mapping[old_id]
        elif dest_session_id and (not source_session_id or old_id == source_session_id):
            obj[field] = dest_session_id
        else:
            unresolved[field].add(old_id)

    def _map_id_field(
        self,
        obj: Dict,
        field: str,
        mapping: Dict[str, str],
        unresolved: Dict[str, set[str]],
        *,
        preserve_unmapped: bool = False,
    ) -> None:
        """Map a single ID field if mapping exists."""
        old_id = obj.get(field)
        if old_id and old_id in mapping:
            new_id = mapping[old_id]
            obj[field] = new_id
        elif preserve_unmapped:
            return
        elif isinstance(old_id, str) and old_id:
            unresolved[field].add(old_id)

    def _build_project_mapping(self) -> Dict[str, str]:
        """
        Build a mapping of project IDs from source to destination by matching project names.
        """
        if self._project_mapping_complete and self._project_id_map is not None:
            return self._project_id_map

        existing_mapping = dict(self._project_id_map or {})
        try:
            source_records: List[Dict[str, Any]] = []
            for project in self.source.get_paginated("/sessions", page_size=100):
                if isinstance(project, dict):
                    source_records.append(project)

            dest_records: List[Dict[str, Any]] = []
            for project in self.dest.get_paginated("/sessions", page_size=100):
                if isinstance(project, dict):
                    dest_records.append(project)

            _, source_duplicates = unique_name_map(source_records)
            dest_unique, dest_duplicates = unique_name_map(dest_records)

            for project in source_records:
                source_id = project["id"]
                project_name = project["name"]

                # Respect explicit mappings already loaded onto the migrator.
                if source_id in existing_mapping:
                    continue

                if self.state and self.state.get_mapped_id("project", source_id):
                    mapped_id = self.state.get_mapped_id("project", source_id)
                    existing_mapping[source_id] = mapped_id
                    self.record_provenance(f"project:{source_id}", "state_mapping")
                    continue

                if project_name in source_duplicates or project_name in dest_duplicates:
                    self.log(
                        f"Project '{project_name}' is duplicated; skipping automatic exact-name mapping",
                        "warning",
                    )
                    continue

                if project_name in dest_unique:
                    existing_mapping[source_id] = dest_unique[project_name]
                    self.record_provenance(f"project:{source_id}", "exact_name_match")
        except Exception as e:
            self.log(f"Failed to build project mapping: {e}", "error")
        self._project_id_map = existing_mapping
        self._project_mapping_complete = True

        return self._project_id_map

    def _build_dataset_mapping(self) -> Dict[str, str]:
        """
        Build a mapping of dataset IDs from source to destination by matching dataset names.
        """
        if self._dataset_id_map is not None:
            return self._dataset_id_map

        self._dataset_id_map = {}
        try:
            source_records: List[Dict[str, Any]] = []
            for dataset in self.source.get_paginated("/datasets", page_size=100):
                if isinstance(dataset, dict):
                    source_records.append(dataset)

            dest_records: List[Dict[str, Any]] = []
            for dataset in self.dest.get_paginated("/datasets", page_size=100):
                if isinstance(dataset, dict):
                    dest_records.append(dataset)

            _, source_duplicates = unique_name_map(source_records)
            dest_unique, dest_duplicates = unique_name_map(dest_records)

            for dataset in source_records:
                source_id = dataset["id"]
                dataset_name = dataset["name"]

                if self.state and self.state.get_mapped_id("dataset", source_id):
                    mapped_id = self.state.get_mapped_id("dataset", source_id)
                    self._dataset_id_map[source_id] = mapped_id
                    self.record_provenance(f"dataset:{source_id}", "state_mapping")
                    continue

                if dataset_name in source_duplicates or dataset_name in dest_duplicates:
                    self.log(
                        f"Dataset '{dataset_name}' is duplicated; skipping automatic exact-name mapping",
                        "warning",
                    )
                    continue

                if dataset_name in dest_unique:
                    self._dataset_id_map[source_id] = dest_unique[dataset_name]
                    self.record_provenance(f"dataset:{source_id}", "exact_name_match")
        except Exception as e:
            self.log(f"Failed to build dataset mapping: {e}", "error")
            self._dataset_id_map = {}

        return self._dataset_id_map
