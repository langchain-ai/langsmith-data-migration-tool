"""Project rules migration logic."""

from typing import Dict, List, Any, Optional
import requests
from langsmith import Client

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
        
        # Initialize LangSmith client for checking prompts
        self.dest_ls_client = None
        try:
            self._init_ls_client()
        except Exception as e:
            self.log(f"Failed to initialize LangSmith client: {e}", "warning")

    @staticmethod
    def _clean_none_values(obj: Any) -> Any:
        """Recursively remove None values from dicts and lists.
        
        This is important for evaluators because the API returns fields like
        'prompt': None, 'schema': None which cause validation errors when sent back.
        """
        if isinstance(obj, dict):
            return {k: RulesMigrator._clean_none_values(v) for k, v in obj.items() if v is not None}
        elif isinstance(obj, list):
            return [RulesMigrator._clean_none_values(item) for item in obj if item is not None]
        else:
            return obj

    def _get_api_url(self, base_url: str) -> str:
        """Prepare API URL for LangSmith Client."""
        clean_url = base_url.rstrip('/')
        if not clean_url.endswith('/api/v1'):
            clean_url = f"{clean_url}/api/v1"
        return clean_url

    def _init_ls_client(self):
        """Initialize LangSmith client for prompt checks."""
        dest_kwargs = {
            "api_key": self.config.destination.api_key,
            "api_url": self._get_api_url(self.config.destination.base_url),
            "info": {}
        }
        
        if not self.config.destination.verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            session = requests.Session()
            session.verify = False
            dest_kwargs["session"] = session
            
        self.dest_ls_client = Client(**dest_kwargs)

    def _find_existing_prompt(self, prompt_handle: str) -> bool:
        """Check if a prompt exists in destination."""
        if not self.dest_ls_client:
            return False
            
        try:
            # Try to list prompts and find a match by repo_handle
            # We iterate because accessing directly might fail or require different permissions
            response = self.dest_ls_client.list_prompts(limit=100)
            if response and hasattr(response, 'repos'):
                for prompt in response.repos:
                    if prompt.repo_handle == prompt_handle:
                        return True
            return False
        except Exception as e:
            self.log(f"Failed to check prompt existence for {prompt_handle}: {e}", "warning")
            return False

    def _fetch_prompt_manifest(self, prompt_handle: str, commit: str = "latest", from_source: bool = True) -> Optional[Dict[str, Any]]:
        """
        Fetch a prompt manifest from source or destination.
        
        Args:
            prompt_handle: The prompt repo handle
            commit: Commit hash or "latest"
            from_source: If True, fetch from source; otherwise from destination
            
        Returns:
            The manifest dict, or None if failed
        """
        source_name = "source" if from_source else "destination"
        
        try:
            if from_source:
                base_url = self.config.source.base_url.rstrip('/')
                api_key = self.config.source.api_key
                verify_ssl = self.config.source.verify_ssl
            else:
                base_url = self.config.destination.base_url.rstrip('/')
                api_key = self.config.destination.api_key
                verify_ssl = self.config.destination.verify_ssl
            
            if not base_url.endswith('/api/v1'):
                base_url = f"{base_url}/api/v1"
            
            url = f"{base_url}/commits/-/{prompt_handle}/{commit}"
            # include_model=true returns the full manifest with model config
            # This does NOT invoke the LLM - it just includes the model serialization
            params = {"include_model": "true"}
            headers = {"x-api-key": api_key}
            
            session = requests.Session()
            if not verify_ssl:
                session.verify = False
            
            if self.config.migration.verbose:
                self.log(f"  Fetching prompt manifest from {source_name}: {url}", "info")
            
            response = session.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 404:
                self.log(f"  Prompt '{prompt_handle}' not found on {source_name} (404)", "warning")
                return None
            
            response.raise_for_status()
            data = response.json()
            manifest = data.get('manifest')
            
            if self.config.migration.verbose:
                if manifest:
                    manifest_id = manifest.get('id', [])
                    type_name = manifest_id[-1] if isinstance(manifest_id, list) and manifest_id else 'unknown'
                    has_last = 'last' in manifest.get('kwargs', {})
                    self.log(f"  Manifest type: {type_name}, has 'last' (model): {has_last}", "info")
                else:
                    self.log(f"  Response has no 'manifest' field. Keys: {list(data.keys())}", "warning")
            
            return manifest
            
        except Exception as e:
            self.log(f"  Failed to fetch prompt manifest for {prompt_handle} from {source_name}: {e}", "warning")
            if self.config.migration.verbose:
                import traceback
                self.log(f"  Traceback: {traceback.format_exc()}", "error")
            return None

    def _extract_model_from_manifest(self, manifest: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract the model configuration from a prompt manifest.
        
        For RunnableSequence/PromptPlayground manifests, the model is in kwargs.last
        
        Args:
            manifest: The prompt manifest dict
            
        Returns:
            The model configuration dict, or None if not found
        """
        if not manifest:
            if self.config.migration.verbose:
                self.log(f"  _extract_model_from_manifest: manifest is None/empty", "warning")
            return None
        
        manifest_id = manifest.get('id', [])
        type_name = manifest_id[-1] if isinstance(manifest_id, list) and manifest_id else ''
        
        if self.config.migration.verbose:
            self.log(f"  Manifest type: '{type_name}'", "info")
        
        if type_name in ('RunnableSequence', 'PromptPlayground'):
            kwargs = manifest.get('kwargs', {})
            model = kwargs.get('last')
            if model:
                if self.config.migration.verbose:
                    model_id = model.get('id', [])
                    model_type = model_id[-1] if isinstance(model_id, list) and model_id else 'unknown'
                    self.log(f"  Extracted model type: '{model_type}'", "info")
                return model
            else:
                if self.config.migration.verbose:
                    self.log(f"  Manifest kwargs keys: {list(kwargs.keys())}", "warning")
                    self.log(f"  No 'last' key found in kwargs", "warning")
                return None
        else:
            if self.config.migration.verbose:
                self.log(f"  Manifest type '{type_name}' is not RunnableSequence/PromptPlayground", "warning")
                self.log(f"  Full manifest id: {manifest_id}", "info")
            return None

    def _check_prompt_has_model(self, prompt_handle: str) -> tuple[bool, str]:
        """
        Check if a prompt on the destination includes a model configuration.
        
        For v3+ evaluators, the prompt must be a RunnableSequence or PromptPlayground
        that includes a model as part of its manifest.
        
        Args:
            prompt_handle: The prompt repo handle
            
        Returns:
            Tuple of (has_model, message)
        """
        manifest = self._fetch_prompt_manifest(prompt_handle, from_source=False)
        
        if manifest is None:
            return False, "Prompt not found on destination"
        
        model = self._extract_model_from_manifest(manifest)
        if model:
            return True, "Prompt has model configuration"
        
        manifest_id = manifest.get('id', [])
        type_name = manifest_id[-1] if isinstance(manifest_id, list) and manifest_id else 'unknown'
        return False, f"Prompt type is '{type_name}', missing model in kwargs.last"

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
                    # Log evaluator info for debugging
                    rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
                    has_evaluators = bool(rule.get('evaluators'))
                    has_code_evaluators = bool(rule.get('code_evaluators'))
                    evaluator_version = rule.get('evaluator_version')
                    # v3+ evaluator fields (stored in separate tables, returned as flat fields)
                    evaluator_prompt_handle = rule.get('evaluator_prompt_handle')
                    evaluator_variable_mapping = rule.get('evaluator_variable_mapping')
                    evaluator_commit_hash_or_tag = rule.get('evaluator_commit_hash_or_tag')

                    if self.config.migration.verbose:
                        self.log(f"Rule '{rule_name}':", "info")
                        self.log(f"  - evaluator_version: {evaluator_version}", "info")
                        self.log(f"  - has evaluators array: {has_evaluators}", "info")
                        self.log(f"  - has code_evaluators: {has_code_evaluators}", "info")
                        # v3+ evaluator info from separate fields
                        if evaluator_prompt_handle:
                            self.log(f"  - evaluator_prompt_handle: {evaluator_prompt_handle}", "info")
                            self.log(f"  - evaluator_commit_hash_or_tag: {evaluator_commit_hash_or_tag}", "info")
                            self.log(f"  - evaluator_variable_mapping: {evaluator_variable_mapping}", "info")
                        if has_evaluators:
                            self.log(f"  - evaluators: {rule.get('evaluators')}", "info")
                        if has_code_evaluators:
                            self.log(f"  - code_evaluators: {rule.get('code_evaluators')}", "info")
                        # Log all available fields
                        self.log(f"  - all fields: {list(rule.keys())}", "info")

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

    def find_existing_rule(self, name: str, session_id: Optional[str] = None, dataset_id: Optional[str] = None) -> Optional[str]:
        """Find existing rule in destination by name and scope (dataset_id or session_id)."""
        try:
            endpoint = self._get_rules_endpoint()
            params = {"limit": 100}

            for rule in self.dest.get_paginated(endpoint, params=params):
                if isinstance(rule, dict):
                    r_name = rule.get('display_name') or rule.get('name')
                    r_dataset_id = rule.get('dataset_id')
                    r_session_id = rule.get('session_id')

                    # Match by name AND scope (dataset_id or session_id)
                    # This prevents matching unrelated rules with the same name
                    if r_name == name:
                        if self.config.migration.verbose:
                            self.log(f"  Checking rule '{r_name}': dataset={r_dataset_id}, session={r_session_id}", "info")

                        # If we're looking for a dataset-specific rule, match dataset_id
                        if dataset_id and r_dataset_id == dataset_id:
                            self.log(f"  Found matching rule by name+dataset: {rule.get('id')}", "info")
                            return rule.get('id')
                        # If we're looking for a project-specific rule, match session_id
                        elif session_id and r_session_id == session_id:
                            self.log(f"  Found matching rule by name+session: {rule.get('id')}", "info")
                            return rule.get('id')
                        # If no scope specified, match by name alone (global rules)
                        elif not dataset_id and not session_id and not r_dataset_id and not r_session_id:
                            self.log(f"  Found matching global rule: {rule.get('id')}", "info")
                            return rule.get('id')
                        # Name matches but scope doesn't - not a match
                        elif self.config.migration.verbose:
                            self.log(f"  Name matches but scope differs (looking for dataset={dataset_id}, session={session_id})", "info")

        except Exception as e:
            self.log(f"Failed to check for existing rule: {e}", "warning")
        return None

    def update_rule(self, rule_id: str, payload: Dict[str, Any]) -> Optional[str]:
        """Update existing rule in destination.
        
        Args:
            rule_id: The ID of the rule to update
            payload: The full rule payload with updated values
            
        Returns:
            The rule ID if successful, None if failed
        """
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update rule: {payload.get('display_name')} ({rule_id})")
            return rule_id

        try:
            # PATCH endpoint only accepts specific fields (group_by is CREATE-only)
            # See: https://api.smith.langchain.com/api/v1/runs/rules/{rule_id}
            valid_patch_fields = {
                'display_name', 'session_id', 'is_enabled', 'dataset_id', 
                'sampling_rate', 'filter', 'trace_filter', 'tree_filter',
                'backfill_from', 'use_corrections_dataset', 'num_few_shot_examples',
                'extend_only', 'transient', 'add_to_annotation_queue_id',
                'add_to_dataset_id', 'add_to_dataset_prefer_correction',
                'evaluators', 'code_evaluators', 'alerts', 'webhooks',
                'evaluator_version', 'create_alignment_queue', 'include_extended_stats'
            }
            
            # Filter payload to only include valid PATCH fields
            patch_payload = {k: v for k, v in payload.items() if k in valid_patch_fields}
            
            # Log if any fields were filtered out
            filtered_fields = set(payload.keys()) - valid_patch_fields
            if filtered_fields:
                self.log(f"Note: Excluded CREATE-only fields from PATCH: {filtered_fields}", "info")
            
            endpoint = f"{self._get_rules_endpoint()}/{rule_id}"
            self.dest.patch(endpoint, patch_payload)
            self.log(f"Updated rule: {payload.get('display_name')} ({rule_id})", "success")
            return rule_id
        except Exception as e:
            self.log(f"Failed to update rule {rule_id}: {e}", "error")
            return None

    def create_rule(
        self,
        rule: Dict[str, Any],
        target_project_id: Optional[str] = None,
        strip_project_reference: bool = False,
        ensure_project: bool = False,
        create_disabled: bool = False
    ) -> Optional[str]:
        """
        Create or update a rule in the destination instance.

        Args:
            rule: Rule configuration from source
            target_project_id: Project ID in destination (if project-specific rule)
            strip_project_reference: If True, creates as global rule even if source was project-specific
            ensure_project: If True, creates the project if it doesn't exist
            create_disabled: If True, creates rule with is_enabled=False to bypass secrets validation.
                           This is useful when destination doesn't have the required API keys/secrets.
                           Rules can be enabled later after secrets are configured.

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
            
            # Build dataset mapping to map add_to_dataset_id
            dataset_map = self.build_dataset_mapping()
            source_add_to_dataset_id = rule.get('add_to_dataset_id')
            dest_add_to_dataset_id = None
            
            if source_add_to_dataset_id:
                dest_add_to_dataset_id = dataset_map.get(source_add_to_dataset_id)
                if dest_add_to_dataset_id:
                    self.log(f"Mapped add_to_dataset_id: {source_add_to_dataset_id} -> {dest_add_to_dataset_id}", "info")
                else:
                    self.log(f"Warning: add_to_dataset_id {source_add_to_dataset_id} not found in destination mapping", "warning")
            
            # Build initial payload - we'll filter out None values later
            # If create_disabled is True, force is_enabled=False to bypass secrets validation
            # The backend adds placeholder secrets when is_enabled=False
            source_is_enabled = rule.get('is_enabled', rule.get('enabled', True))
            is_enabled = False if create_disabled else source_is_enabled
            
            if create_disabled and source_is_enabled:
                self.log(f"Creating rule as disabled (to bypass secrets validation)", "info")
            
            payload = {
                'display_name': display_name,
                'is_enabled': is_enabled,
                'sampling_rate': rule.get('sampling_rate', 1.0),
                'filter': rule.get('filter'),
                'trace_filter': rule.get('trace_filter'),
                'tree_filter': rule.get('tree_filter'),
                'backfill_from': rule.get('backfill_from'),
                'use_corrections_dataset': rule.get('use_corrections_dataset', False),
                'num_few_shot_examples': rule.get('num_few_shot_examples'),
                'extend_only': rule.get('extend_only', False),
                'transient': rule.get('transient', False),
                'add_to_annotation_queue_id': rule.get('add_to_annotation_queue_id'),
                'add_to_dataset_id': dest_add_to_dataset_id or source_add_to_dataset_id,
                'add_to_dataset_prefer_correction': rule.get('add_to_dataset_prefer_correction', False),
                'evaluator_version': rule.get('evaluator_version'),
                'include_extended_stats': rule.get('include_extended_stats', False),
            }
            
            # Remove None values from payload to avoid API validation errors
            payload = {k: v for k, v in payload.items() if v is not None}

            # Handle evaluators - for v3+ evaluators, the data might be in separate fields
            # that need to be reconstructed into the evaluators array
            evaluators = rule.get('evaluators')

            # Check if we need to reconstruct evaluators from separate fields (v3+ evaluators)
            # The API returns: evaluator_prompt_handle, evaluator_variable_mapping, evaluator_commit_hash_or_tag
            if not evaluators and rule.get('evaluator_prompt_handle'):
                prompt_handle = rule.get('evaluator_prompt_handle')
                commit_or_tag = rule.get('evaluator_commit_hash_or_tag') or 'latest'
                variable_mapping = rule.get('evaluator_variable_mapping')

                hub_ref = f"{prompt_handle}:{commit_or_tag}"
                self.log(f"Reconstructing v3+ evaluator: hub_ref={hub_ref}", "info")
                
                # For v3+ evaluators, we need to ensure the model is available.
                # The model can come from:
                # 1. The destination prompt (if it's a RunnableSequence/PromptPlayground with model)
                # 2. The source prompt (we can fetch it and include it explicitly)
                # 
                # We'll try to get the model from the source prompt and include it explicitly
                # to ensure validation succeeds even if the destination prompt doesn't have it.
                
                model_config = None
                
                # First, try to get the model from the SOURCE prompt
                source_manifest = self._fetch_prompt_manifest(prompt_handle, commit_or_tag, from_source=True)
                if source_manifest:
                    # Debug: log the manifest structure
                    if self.config.migration.verbose:
                        import json
                        manifest_id = source_manifest.get('id', [])
                        manifest_kwargs_keys = list(source_manifest.get('kwargs', {}).keys())
                        self.log(f"  Source manifest id: {manifest_id}", "info")
                        self.log(f"  Source manifest kwargs keys: {manifest_kwargs_keys}", "info")
                    
                    model_config = self._extract_model_from_manifest(source_manifest)
                    if model_config:
                        self.log(f"  Extracted model config from source prompt", "info")
                    else:
                        self.log(f"  Source prompt doesn't have model config (not a RunnableSequence/PromptPlayground)", "warning")
                        # Log more details for debugging
                        if self.config.migration.verbose:
                            self.log(f"  Full manifest structure (first 500 chars): {str(source_manifest)[:500]}", "info")
                else:
                    self.log(f"  Could not fetch source prompt manifest", "warning")
                
                # Build the evaluator structure
                evaluator_structured = {
                    'hub_ref': hub_ref,
                    'variable_mapping': variable_mapping,
                }
                
                # Include the model if we found it
                if model_config:
                    evaluator_structured['model'] = model_config
                    self.log(f"  Including model config in evaluator (ensures validation passes)", "info")
                
                evaluators = [{'structured': evaluator_structured}]

                # If we still don't have a model, try to get it from destination prompt
                if not model_config:
                    self.log(f"  Trying to get model from destination prompt...", "info")
                    dest_manifest = self._fetch_prompt_manifest(prompt_handle, commit_or_tag, from_source=False)
                    if dest_manifest:
                        model_config = self._extract_model_from_manifest(dest_manifest)
                        if model_config:
                            self.log(f"  Got model config from destination prompt", "info")
                            evaluator_structured['model'] = model_config

                # Final check - do we have a model?
                if not model_config:
                    self.log(f"[ERROR] Could not find model config for evaluator prompt '{prompt_handle}'", "error")
                    self.log(f"  The prompt must be a RunnableSequence/PromptPlayground with a model", "error")
                    self.log(f"  This typically means:", "error")
                    self.log(f"    1. The prompt on source doesn't have a model (simple prompt, not RunnableSequence)", "error")
                    self.log(f"    2. Or the prompt migration didn't include the model", "error")
                    self.log(f"  Skipping this rule - it will fail validation without a model", "error")
                    return None
                
                # Check if prompt exists on destination (for informational purposes)
                if not self._find_existing_prompt(prompt_handle):
                    self.log(f"[WARNING] Prompt '{prompt_handle}' does NOT exist on destination", "warning")
                    self.log(f"  Run 'langsmith-migrator prompts' first to migrate prompts", "warning")
                else:
                    self.log(f"  Prompt '{prompt_handle}' exists on destination", "info")

            if evaluators:
                # Clean None values from evaluators - the API returns fields like
                # 'prompt': None, 'schema': None which cause validation errors when sent back
                evaluators = self._clean_none_values(evaluators)
                payload['evaluators'] = evaluators
                self.log(f"Copying {len(evaluators)} LLM evaluator(s)", "info")

                # Log details about each evaluator and warn about prompt dependencies
                missing_prompts = []
                for i, ev in enumerate(evaluators):
                    structured = ev.get('structured', {})
                    hub_ref = structured.get('hub_ref')
                    has_model = 'model' in structured
                    has_prompt = 'prompt' in structured
                    
                    if hub_ref:
                        self.log(f"  Evaluator {i+1}: hub_ref={hub_ref}, has_model={has_model}", "info")
                        # Extract prompt name from hub_ref (format: "owner/name:tag" or "name:tag")
                        prompt_name = hub_ref.split(':')[0] if ':' in hub_ref else hub_ref
                        missing_prompts.append(prompt_name)
                        
                        if not has_model:
                            self.log(f"  [WARNING] Evaluator {i+1} has no model - validation may fail!", "warning")
                    elif has_prompt:
                        self.log(f"  Evaluator {i+1}: inline prompt, has_model={has_model}", "info")

                # Check for prompt dependencies
                actually_missing = []
                for prompt in missing_prompts:
                    if not self._find_existing_prompt(prompt):
                        actually_missing.append(prompt)
                    else:
                        self.log(f"Confirmed prompt '{prompt}' exists on destination", "info")

                if actually_missing:
                    self.log(f"[WARNING] Rule references {len(actually_missing)} prompt(s) that must exist on destination:", "warning")
                    for prompt in actually_missing:
                        self.log(f"  - {prompt}", "warning")
                    self.log(f"Run 'langsmith-migrator prompts' first to migrate prompts", "warning")

            # Copy code_evaluators array directly (contains code evaluator configs)
            # Each code evaluator has: { code: str, language?: 'python' | 'javascript' }
            if rule.get('code_evaluators'):
                code_evaluators = self._clean_none_values(rule.get('code_evaluators'))
                payload['code_evaluators'] = code_evaluators
                self.log(f"Copying {len(code_evaluators)} code evaluator(s)", "info")

            # Copy alerts and webhooks if present
            if rule.get('alerts'):
                payload['alerts'] = rule.get('alerts')
            if rule.get('webhooks'):
                payload['webhooks'] = rule.get('webhooks')

            # Copy group_by for thread evaluators
            if rule.get('group_by'):
                payload['group_by'] = rule.get('group_by')

            # Use the standard rules endpoint
            base_endpoint = self._get_rules_endpoint()

            # Determine what to do with session_id and dataset_id
            source_session_id = rule.get('session_id')
            source_dataset_id = rule.get('dataset_id')
            
            # Build ID mappings if not already done
            # If ensure_project is True, create missing projects in destination
            project_map = self.build_project_mapping(create_missing=ensure_project)
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
                dest_session_id = target_project_id # For existence check
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

            # Check for existence before creating
            # We use dest_session_id or dest_dataset_id to narrow search
            # If payload has both, we search by both? Usually rules are one or other or both.
            existing_id = self.find_existing_rule(display_name, session_id=payload.get('session_id'), dataset_id=payload.get('dataset_id'))
            
            if existing_id:
                if self.config.migration.skip_existing:
                    self.log(f"Rule '{display_name}' already exists, skipping", "warning")
                    return existing_id
                else:
                    self.log(f"Rule '{display_name}' exists, updating...", "info")
                    result = self.update_rule(existing_id, payload)
                    return result  # Will be existing_id on success, None on failure

            # Filter payload to only include valid CREATE fields per API spec
            # See: https://api.smith.langchain.com/api/v1/runs/rules
            valid_create_fields = {
                'display_name', 'session_id', 'is_enabled', 'dataset_id',
                'sampling_rate', 'filter', 'trace_filter', 'tree_filter',
                'backfill_from', 'use_corrections_dataset', 'num_few_shot_examples',
                'extend_only', 'transient', 'add_to_annotation_queue_id',
                'add_to_dataset_id', 'add_to_dataset_prefer_correction',
                'evaluators', 'code_evaluators', 'alerts', 'webhooks',
                'evaluator_version', 'create_alignment_queue', 'include_extended_stats',
                'group_by'  # CREATE-only field for thread evaluators
            }
            
            # Filter payload to only include valid fields
            create_payload = {k: v for k, v in payload.items() if k in valid_create_fields}
            
            # Log if any fields were filtered out
            filtered_fields = set(payload.keys()) - valid_create_fields
            if filtered_fields:
                self.log(f"Note: Excluded invalid fields from CREATE: {filtered_fields}", "info")
            
            self.log(f"Creating rule at {endpoint}", "info")
            if self.config.migration.verbose:
                self.log(f"POST payload fields: {list(create_payload.keys())}", "info")
            response = self.dest.post(endpoint, create_payload)
            rule_id = response.get('id')

            self.log(f"Created rule: {display_name} -> {rule_id}", "success")
            return rule_id

        except Exception as e:
            rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
            error_str = str(e)
            self.log(f"Failed to create rule {rule_name}: {e}", "error")
            
            # Provide specific guidance for common errors
            if "RunnableSequence must have at least 2 steps" in error_str:
                self.log("", "error")
                self.log("This error indicates the evaluator prompt is missing a model configuration.", "error")
                self.log("For v3+ evaluators, the prompt in the hub must be a RunnableSequence or", "error")
                self.log("PromptPlayground that includes both the prompt AND the model.", "error")
                self.log("", "error")
                self.log("To fix this:", "error")
                self.log("1. Run 'langsmith-migrator prompts' to migrate prompts from source", "error")
                self.log("2. Ensure the prompt was migrated with include_model=true", "error")
                self.log("3. Or manually add the model to the prompt on the destination", "error")
                
                # Log which prompt is problematic
                if payload.get('evaluators'):
                    for ev in payload['evaluators']:
                        hub_ref = ev.get('structured', {}).get('hub_ref')
                        if hub_ref:
                            self.log(f"   Problematic prompt: {hub_ref}", "error")
            
            elif "Evaluator failed validation" in error_str:
                self.log("", "error")
                self.log("Evaluator validation failed. Common causes:", "error")
                self.log("- Missing or invalid prompt in destination hub", "error")
                self.log("- Prompt exists but doesn't include model configuration", "error") 
                self.log("- Missing secrets required by the model (e.g., API keys)", "error")
                self.log("", "error")
                self.log("Run 'langsmith-migrator prompts' first to ensure prompts are migrated", "error")
            
            if self.config.migration.verbose:
                self.log(f"Payload that failed: {list(payload.keys())}", "info")
                if payload.get('evaluators'):
                    self.log(f"Evaluators in payload: {payload['evaluators']}", "info")
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
