"""Chart migration logic using global API endpoints."""

import datetime
from typing import Dict, List, Any, Optional
from .base import BaseMigrator
from ..api_client import NotFoundError, APIError


class ChartMigrator(BaseMigrator):
    """Handles chart migration using global /api/v1/charts endpoints."""

    def __init__(self, source_client, dest_client, state, config):
        """Initialize chart migrator with ID mapping caches."""
        super().__init__(source_client, dest_client, state, config)
        self._project_id_map = None  # Lazy-loaded cache
        self._dataset_id_map = None  # Lazy-loaded cache
        self._dest_section_map = None # Lazy-loaded cache for dest sections

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

    def list_charts(self, session_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List charts from source using POST /api/v1/charts.
        
        Args:
            session_id: Optional session ID to filter by
            
        Returns:
            List of chart configurations
        """
        try:
            # Prepare request body for listing charts according to schema
            # Note: Some versions require start_time even if omit_data is True
            start_time = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).isoformat()

            payload = {
                "timezone": "UTC",
                "omit_data": True,  # Only fetch metadata, not full data
                "start_time": start_time,
                "end_time": None,
                "stride": {"days": 0, "hours": 0, "minutes": 15},
                "after_index": None,
                "tag_value_id": None
            }

            response = self.source.post("/charts", payload)
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
                        # For now, strict filtering.
                        pass

                return filtered_charts

            return charts

        except NotFoundError:
            self.log("Charts API endpoint not found (404)", "error")
            return []
        except APIError as e:
            if e.status_code == 405:
                self.log("Charts API method not allowed (405)", "error")
            else:
                self.log(f"Charts API error ({e.status_code}): {e}", "error")
            return []
        except Exception as e:
            self.log(f"Unexpected error listing charts: {e}", "error")
            return []

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
            start_time = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).isoformat()
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
            # List all charts from destination
            charts = self.list_charts()

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

        # Build update payload
        payload = {
            "title": chart_data.get("title") or chart_data.get("name"),
            "chart_type": chart_data.get("chart_type"),
            "series": chart_data.get("series"),
            "description": chart_data.get("description"),
            "index": chart_data.get("index"),
            "metadata": chart_data.get("metadata"),
            "section_id": chart_data.get("section_id"),
            "common_filters": chart_data.get("common_filters")
        }

        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}

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

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create chart: {chart_title}", "info")
            return f"dry-run-{chart_data.get('id', 'chart-id')}"

        # Build payload based on schema
        payload = {
            "title": chart_title,
            "chart_type": chart_data.get("chart_type", "line"),
            "series": chart_data.get("series", []),
            "description": chart_data.get("description"),
            "index": chart_data.get("index"),
            "metadata": chart_data.get("metadata"),
            "section_id": chart_data.get("section_id"),
            "common_filters": chart_data.get("common_filters")
        }

        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}

        # If we have a source section title, ensure it exists in destination
        source_section_title = chart_data.get("_source_section_title")
        source_section_desc = chart_data.get("_source_section_description")

        if source_section_title:
            dest_section_id = self._ensure_dest_section(source_section_title, source_section_desc)
            if dest_section_id:
                # payload["section_id"] = dest_section_id
                # NOTE: We previously tried setting section_id directly, but if we have a valid
                # section ID from the ensure step, we should use it.
                payload["section_id"] = dest_section_id

        # Validate required fields
        if not payload["series"]:
            self.log(f"Chart '{chart_title}' has no series, skipping", "warning")
            return None

        # Check if chart already exists
        existing_id = self.find_existing_chart(chart_title, payload.get("section_id"))

        if existing_id:
            if self.config.migration.skip_existing:
                self.log(f"Chart '{chart_title}' already exists, skipping", "warning")
                return existing_id
            else:
                self.log(f"Chart '{chart_title}' exists, updating...", "info")
                success = self.update_chart(existing_id, chart_data)
                return existing_id if success else None

        try:
            # Try to create
            try:
                response = self.dest.post("/charts/create", payload)
            except APIError as e:
                # If failed and we have section_id, try removing it
                if "section_id" in payload:
                    self.log(f"First attempt failed ({e}), retrying without section_id", "info")
                    del payload["section_id"]
                    response = self.dest.post("/charts/create", payload)
                else:
                    raise e

            # Response might be the chart object or just ID
            if isinstance(response, dict):
                chart_id = response.get("id")
            else:
                chart_id = None

            if chart_id:
                return str(chart_id)

            return None

        except APIError as e:
            self.log(f"Failed to create chart '{chart_title}': {e}", "error")
            return None
        except Exception as e:
            self.log(f"Unexpected error creating chart '{chart_title}': {e}", "error")
            return None

    def migrate_chart(self, chart: Dict[str, Any], dest_session_id: Optional[str] = None) -> Optional[str]:
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

        if not chart_id:
            self.log(f"Chart '{chart_title}' missing ID, skipping", "warning")
            return None

        # Deep copy
        import copy
        chart_copy = copy.deepcopy(chart)

        # Map IDs within chart config
        self._map_ids_in_chart(chart_copy, dest_session_id)

        # Create in destination
        try:
            new_id = self.create_chart(chart_copy)
            return new_id
        except Exception as e:
            self.log(f"Failed to migrate chart '{chart_title}': {e}", "error")
            return None

    def migrate_session_charts(self, source_session_id: str, dest_session_id: str) -> Dict[str, str]:
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

            new_id = self.migrate_chart(chart, dest_session_id)

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

        # Pre-load project mapping if needed
        if not same_instance:
            self._build_project_mapping()

        for chart in charts:
            chart_id = chart.get("id")
            if not chart_id:
                continue

            # Determine source session/project ID for this chart
            source_session_id = self._extract_session_id(chart)

            dest_session_id = None
            if source_session_id:
                if same_instance:
                    dest_session_id = source_session_id
                elif self._project_id_map:
                    dest_session_id = self._project_id_map.get(source_session_id)

            # Migrate
            new_id = self.migrate_chart(chart, dest_session_id)

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
        # Top level
        if chart.get('session_id'): return chart['session_id']
        if chart.get('project_id'): return chart['project_id']

        # In series
        for s in chart.get('series', []):
            if isinstance(s, dict):
                filters = s.get('filters')
                if not filters:
                    continue

                if filters.get('project_id'): return filters['project_id']
                if filters.get('session_id'): return filters['session_id']

        # In common_filters
        common_filters = chart.get('common_filters')
        if isinstance(common_filters, dict):
            sessions = common_filters.get('session')
            if isinstance(sessions, list) and sessions:
                return sessions[0]  # Return first session ID found

        return None

    def _map_ids_in_chart(self, obj: Any, dest_session_id: Optional[str] = None):
        """
        Recursively map project and dataset IDs within a chart object.
        Modifies the object in-place.
        
        Args:
            obj: Chart object or sub-structure
            dest_session_id: If provided, forcibly sets project_id/session_id to this value
                             instead of using the mapping (useful when we know the target)
        """
        if isinstance(obj, dict):
            # Check for specific keys to map
            if "project_id" in obj:
                if dest_session_id:
                    obj["project_id"] = dest_session_id
                else:
                    self._map_id_field(obj, "project_id", self._build_project_mapping())

            if "session_id" in obj:
                if dest_session_id:
                    obj["session_id"] = dest_session_id
                else:
                    self._map_id_field(obj, "session_id", self._build_project_mapping())

            if "dataset_id" in obj:
                self._map_id_field(obj, "dataset_id", self._build_dataset_mapping())

            if "session" in obj and isinstance(obj["session"], list):
                # Map list of session IDs (used in common_filters)
                new_ids = []
                # Only build mapping if we don't have a forced destination ID
                mapping = {}
                if not dest_session_id:
                    mapping = self._build_project_mapping()

                # If forced ID, just use that
                if dest_session_id:
                    new_ids = [dest_session_id]
                else:
                    # Map each ID
                    for old_id in obj["session"]:
                        if isinstance(old_id, str) and old_id in mapping:
                            new_ids.append(mapping[old_id])
                        else:
                            new_ids.append(old_id)
                obj["session"] = new_ids

            # Handle special 'tag_value_id' which might be project ID in some contexts
            # But skipping for now as it's ambiguous

            # Recurse into values
            for key, value in obj.items():
                self._map_ids_in_chart(value, dest_session_id)

        elif isinstance(obj, list):
            for item in obj:
                self._map_ids_in_chart(item, dest_session_id)

    def _map_id_field(self, obj: Dict, field: str, mapping: Dict[str, str]):
        """Map a single ID field if mapping exists."""
        old_id = obj.get(field)
        if old_id and old_id in mapping:
            new_id = mapping[old_id]
            obj[field] = new_id

    def _build_project_mapping(self) -> Dict[str, str]:
        """
        Build a mapping of project IDs from source to destination by matching project names.
        """
        if self._project_id_map is not None:
            return self._project_id_map

        self._project_id_map = {}
        try:
            # Get all projects from source
            source_projects = {}  # name -> id
            for project in self.source.get_paginated("/sessions", page_size=100):
                if isinstance(project, dict) and 'name' in project and 'id' in project:
                    source_projects[project['name']] = project['id']

            # Get all projects from destination
            dest_projects = {}  # name -> id
            for project in self.dest.get_paginated("/sessions", page_size=100):
                if isinstance(project, dict) and 'name' in project and 'id' in project:
                    dest_projects[project['name']] = project['id']

            # Build mapping by matching names
            for name, source_id in source_projects.items():
                if name in dest_projects:
                    self._project_id_map[source_id] = dest_projects[name]
        except Exception as e:
            self.log(f"Failed to build project mapping: {e}", "error")
            self._project_id_map = {}

        return self._project_id_map

    def _build_dataset_mapping(self) -> Dict[str, str]:
        """
        Build a mapping of dataset IDs from source to destination by matching dataset names.
        """
        if self._dataset_id_map is not None:
            return self._dataset_id_map

        self._dataset_id_map = {}
        try:
            # Get all datasets from source
            source_datasets = {}  # name -> id
            for dataset in self.source.get_paginated("/datasets", page_size=100):
                if isinstance(dataset, dict) and 'name' in dataset and 'id' in dataset:
                    source_datasets[dataset['name']] = dataset['id']

            # Get all datasets from destination
            dest_datasets = {}  # name -> id
            for dataset in self.dest.get_paginated("/datasets", page_size=100):
                if isinstance(dataset, dict) and 'name' in dataset and 'id' in dataset:
                    dest_datasets[dataset['name']] = dataset['id']

            # Build mapping by matching names
            for name, source_id in source_datasets.items():
                if name in dest_datasets:
                    self._dataset_id_map[source_id] = dest_datasets[name]
        except Exception as e:
            self.log(f"Failed to build dataset mapping: {e}", "error")
            self._dataset_id_map = {}

        return self._dataset_id_map
