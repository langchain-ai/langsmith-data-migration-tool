"""Project rules migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator
from ..api_client import NotFoundError


class RulesMigrator(BaseMigrator):
    """Handles project rules (automation rules) migration."""

    def __init__(self, source_client, dest_client, state, config):
        super().__init__(source_client, dest_client, state, config)
        # Cache the working endpoint to avoid re-testing for each rule
        self._dest_rules_endpoint = None
        self._dest_endpoint_tested = False
        # ID mappings for projects and datasets
        self._project_id_map = None  # Maps old_project_id -> new_project_id
        self._dataset_id_map = None  # Maps old_dataset_id -> new_dataset_id

    def _get_project_details(self, project_id: str) -> Optional[Dict[str, Any]]:
        """
        Get full project details from source.

        Args:
            project_id: The project ID to fetch

        Returns:
            Project details dict, or None if failed
        """
        try:
            return self.source.get(f"/sessions/{project_id}")
        except Exception as e:
            self.log(f"Failed to get project {project_id}: {e}", "error")
            return None

    def _create_project(self, project: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a project in the destination.

        Args:
            project: Project details from source

        Returns:
            Created project details, or None if failed
        """
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create project: {project['name']}", "info")
            return {"id": f"dry-run-{project['id']}", "name": project['name']}

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
        """
        Build a mapping of project IDs from source to destination by matching project names.
        Automatically creates missing projects in destination by default.

        Args:
            create_missing: If True (default), creates projects that exist in source but not in destination

        Returns:
            Dict mapping source_project_id -> dest_project_id
        """
        if self._project_id_map is not None:
            return self._project_id_map

        self.log("Building project ID mapping...", "info")
        self._project_id_map = {}

        try:
            # Get all projects from source (store full objects for potential creation)
            source_projects = {}  # name -> full project dict
            source_projects_by_id = {}  # id -> full project dict
            for project in self.source.get_paginated("/sessions", page_size=100):
                if isinstance(project, dict):
                    source_projects[project['name']] = project
                    source_projects_by_id[project['id']] = project

            # Get all projects from destination
            dest_projects = {}  # name -> project id
            for project in self.dest.get_paginated("/sessions", page_size=100):
                if isinstance(project, dict):
                    dest_projects[project['name']] = project['id']

            # Build mapping by matching names
            existing_count = 0
            created_count = 0

            for name, source_project in source_projects.items():
                source_id = source_project['id']

                if name in dest_projects:
                    # Project exists in both - create mapping
                    self._project_id_map[source_id] = dest_projects[name]
                    self.log(f"Mapped project '{name}': {source_id} -> {dest_projects[name]}", "info")
                    existing_count += 1
                elif create_missing:
                    # Project missing in destination - create it
                    self.log(f"Project '{name}' not found in destination, creating...", "info")
                    new_project = self._create_project(source_project)
                    if new_project:
                        self._project_id_map[source_id] = new_project['id']
                        self.log(f"Mapped project '{name}': {source_id} -> {new_project['id']}", "info")
                        created_count += 1
                    else:
                        self.log(f"Failed to create project '{name}' in destination", "error")

            total_mapped = len(self._project_id_map)
            if created_count > 0:
                self.log(f"Built project mapping: {existing_count} existing, {created_count} created, {total_mapped} total", "success")
            else:
                self.log(f"Built project mapping: {total_mapped} projects mapped", "success")

        except Exception as e:
            self.log(f"Failed to build project mapping: {e}", "error")
            self._project_id_map = {}

        return self._project_id_map
    
    def build_dataset_mapping(self) -> Dict[str, str]:
        """
        Build a mapping of dataset IDs from source to destination by matching dataset names.
        
        Returns:
            Dict mapping source_dataset_id -> dest_dataset_id
        """
        if self._dataset_id_map is not None:
            return self._dataset_id_map
            
        self.log("Building dataset ID mapping...", "info")
        self._dataset_id_map = {}
        
        try:
            # Get all datasets from source
            source_datasets = {}
            for dataset in self.source.get_paginated("/datasets", page_size=100):
                if isinstance(dataset, dict):
                    source_datasets[dataset['name']] = dataset['id']
            
            # Get all datasets from destination
            dest_datasets = {}
            for dataset in self.dest.get_paginated("/datasets", page_size=100):
                if isinstance(dataset, dict):
                    dest_datasets[dataset['name']] = dataset['id']
            
            # Build mapping by matching names
            for name, source_id in source_datasets.items():
                if name in dest_datasets:
                    self._dataset_id_map[source_id] = dest_datasets[name]
                    self.log(f"Mapped dataset '{name}': {source_id} -> {dest_datasets[name]}", "info")
            
            self.log(f"Built dataset mapping: {len(self._dataset_id_map)} datasets matched", "success")
            
        except Exception as e:
            self.log(f"Failed to build dataset mapping: {e}", "error")
            self._dataset_id_map = {}
        
        return self._dataset_id_map

    def _get_rules_endpoint(self) -> str:
        """Get the rules API endpoint (always /runs/rules for LangSmith)."""
        return "/runs/rules"

    def list_rules(self) -> List[Dict[str, Any]]:
        """
        List all automation rules from source instance.
        
        Rules can be project-specific or global. This method lists all accessible rules.
        """
        rules = []
        endpoint = self._get_rules_endpoint()
        
        try:
            for rule in self.source.get_paginated(endpoint, page_size=100):
                if isinstance(rule, dict):
                    rules.append(rule)
            return rules
        except NotFoundError:
            self.log(f"Rules endpoint {endpoint} not found", "warning")
            return []
        except Exception as e:
            self.log(f"Failed to list rules: {e}", "error")
            return []

    def get_rule(self, rule_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific rule by ID."""
        endpoint = self._get_rules_endpoint()
        try:
            return self.source.get(f"{endpoint}/{rule_id}")
        except NotFoundError:
            self.log(f"Rule {rule_id} not found", "warning")
            return None
        except Exception as e:
            self.log(f"Failed to get rule {rule_id}: {e}", "error")
            return None

    def list_project_rules(self, project_id: str) -> List[Dict[str, Any]]:
        """
        List rules for a specific project.
        
        Project rules are often stored at /sessions/{project_id}/rules or similar.
        """
        # Try different possible endpoint names for project-specific rules
        possible_endpoints = [
            f"/sessions/{project_id}/rules",
            f"/sessions/{project_id}/automations",
            f"/sessions/{project_id}/automation-rules",
            f"/sessions/{project_id}/monitors",
            f"/sessions/{project_id}/online-evaluations",
        ]
        
        for endpoint in possible_endpoints:
            try:
                self.log(f"Trying to list project rules from {endpoint}...", "info")
                rules = []
                
                for rule in self.source.get_paginated(endpoint, page_size=100):
                    if isinstance(rule, dict):
                        rules.append(rule)
                
                if rules:
                    self.log(f"Found {len(rules)} rule(s) for project at {endpoint}", "success")
                    return rules
                else:
                    self.log(f"No rules found at {endpoint}, trying next...", "info")
                    
            except NotFoundError:
                self.log(f"Endpoint {endpoint} not found, trying next...", "info")
                continue
            except Exception as e:
                self.log(f"Error accessing {endpoint}: {e}", "warning")
                continue
        
        self.log(f"No rules endpoints found for project {project_id}", "warning")
        return []

    def create_rule(
        self,
        rule: Dict[str, Any],
        target_project_id: Optional[str] = None,
        strip_project_reference: bool = False,
        ensure_project: bool = False
    ) -> Optional[str]:
        """
        Create a rule in the destination instance.

        Args:
            rule: Rule configuration from source
            target_project_id: Project ID in destination (if project-specific rule)
            strip_project_reference: If True, creates as global rule even if source was project-specific
            ensure_project: If True, creates the project if it doesn't exist

        Returns:
            The new rule ID, or None if failed
        """
        if self.config.migration.dry_run:
            rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
            self.log(f"[DRY RUN] Would create rule: {rule_name}")
            return f"dry-run-{rule.get('id', 'rule')}"

        try:
            display_name = rule.get('display_name') or rule.get('name')
            
            if not display_name:
                rule_id = rule.get('id', 'unknown')
                display_name = f"Rule {rule_id}"
                self.log(f"Warning: Rule {rule_id} missing display_name/name, using: {display_name}", "warning")
                self.log(f"Available fields in rule: {list(rule.keys())}", "info")
            
            payload = {
                'display_name': display_name,
                'description': rule.get('description'),
                'enabled': rule.get('enabled', True),
                'rule_type': rule.get('rule_type'),
                'filters': rule.get('filters', {}),
                'actions': rule.get('actions', []),
                'sampling_rate': rule.get('sampling_rate', 1.0),
            }

            # Use the standard rules endpoint
            base_endpoint = self._get_rules_endpoint()

            # Determine what to do with session_id and dataset_id
            source_session_id = rule.get('session_id')
            source_dataset_id = rule.get('dataset_id')
            
            # Build ID mappings if not already done
            project_map = self.build_project_mapping()
            dataset_map = self.build_dataset_mapping()
            
            # Map IDs from source to destination
            dest_session_id = None
            dest_dataset_id = None
            
            if source_session_id:
                dest_session_id = project_map.get(source_session_id)
                if not dest_session_id:
                    self.log(f"Warning: Project {source_session_id} not found in destination", "warning")
            
            if source_dataset_id:
                dest_dataset_id = dataset_map.get(source_dataset_id)
                if not dest_dataset_id:
                    self.log(f"Warning: Dataset {source_dataset_id} not found in destination", "warning")
            
            # Determine endpoint and payload based on project/dataset context
            if strip_project_reference:
                # User wants to strip project references
                # But we still need EITHER session_id OR dataset_id (API requirement)
                if dest_dataset_id:
                    # Use mapped dataset_id even when stripping project
                    payload['dataset_id'] = dest_dataset_id
                    self.log(f"Using mapped dataset_id (stripping project)", "info")
                elif source_dataset_id:
                    self.log(f"Warning: Dataset {source_dataset_id} not found in destination", "warning")
                    self.log(f"Cannot migrate rule without valid dataset or project", "warning")
                    return None
                else:
                    self.log(f"Warning: Cannot strip project from rule '{display_name}' - no dataset_id to use instead", "warning")
                    return None
                endpoint = base_endpoint
            elif target_project_id:
                # User provided a target project ID - use it directly
                payload['session_id'] = target_project_id
                # Also use mapped dataset_id if present
                if dest_dataset_id:
                    payload['dataset_id'] = dest_dataset_id
                endpoint = base_endpoint
            else:
                # Use mapped IDs from source to destination
                if dest_session_id:
                    payload['session_id'] = dest_session_id
                    self.log(f"Using mapped project ID", "info")
                if dest_dataset_id:
                    payload['dataset_id'] = dest_dataset_id
                    self.log(f"Using mapped dataset ID", "info")
                    
                # API requires at least one of these
                if not dest_session_id and not dest_dataset_id:
                    self.log(f"Error: Rule '{display_name}' cannot be mapped", "error")
                    if source_session_id and not dest_session_id:
                        self.log(f"  Project {source_session_id} not found in destination", "error")
                    if source_dataset_id and not dest_dataset_id:
                        self.log(f"  Dataset {source_dataset_id} not found in destination", "error")
                    if not source_session_id and not source_dataset_id:
                        self.log(f"  Rule has neither session_id nor dataset_id in source", "error")
                    return None
                    
                endpoint = base_endpoint

            self.log(f"Creating rule at {endpoint}", "info")
            response = self.dest.post(endpoint, payload)
            rule_id = response.get('id')

            self.log(f"Created rule: {display_name} -> {rule_id}", "success")
            return rule_id

        except Exception as e:
            rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
            self.log(f"Failed to create rule {rule_name}: {e}", "error")
            return None

    def migrate_rule(
        self,
        rule_id: str,
        target_project_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Migrate a single rule.
        
        Args:
            rule_id: Source rule ID
            target_project_id: Destination project ID (for project-specific rules)
            
        Returns:
            The new rule ID, or None if failed
        """
        rule = self.get_rule(rule_id)
        if not rule:
            return None
            
        return self.create_rule(rule, target_project_id)

    def migrate_project_rules(
        self,
        source_project_id: str,
        dest_project_id: str
    ) -> Dict[str, str]:
        """
        Migrate all rules from one project to another.
        
        Args:
            source_project_id: Source project ID
            dest_project_id: Destination project ID
            
        Returns:
            Mapping of source rule IDs to destination rule IDs
        """
        rules = self.list_project_rules(source_project_id)
        
        if not rules:
            self.log(f"No rules found for project {source_project_id}")
            return {}
        
        self.log(f"Found {len(rules)} rules for project {source_project_id}")
        
        id_mapping = {}
        for rule in rules:
            rule_id = rule.get('id')
            new_rule_id = self.create_rule(rule, dest_project_id)
            
            if new_rule_id:
                id_mapping[rule_id] = new_rule_id
        
        self.log(
            f"Migrated {len(id_mapping)}/{len(rules)} rules for project",
            "success" if len(id_mapping) == len(rules) else "warning"
        )
        
        return id_mapping
