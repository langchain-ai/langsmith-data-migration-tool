"""Prompt migration logic."""

from typing import Dict, List, Any, Optional
from langsmith import Client
import requests

from .base import BaseMigrator


class PromptMigrator(BaseMigrator):
    """Handles prompt migration."""

    def __init__(self, source_client, dest_client, state, config):
        super().__init__(source_client, dest_client, state, config)

        # Create managed sessions so we can update workspace headers dynamically.
        # The parent EnhancedAPIClient (self.source / self.dest) may have
        # X-Tenant-Id set after init via orchestrator.set_workspace_context().
        # We sync that header into these sessions before each operation.
        self._source_session = requests.Session()
        self._dest_session = requests.Session()

        if not config.source.verify_ssl or not config.destination.verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        if not config.source.verify_ssl:
            self._source_session.verify = False
        if not config.destination.verify_ssl:
            self._dest_session.verify = False

        # Create LangSmith SDK clients with our managed sessions
        self.source_ls_client = Client(
            api_key=config.source.api_key,
            api_url=self._get_api_url(config.source.base_url),
            session=self._source_session,
            info={},
        )
        self.dest_ls_client = Client(
            api_key=config.destination.api_key,
            api_url=self._get_api_url(config.destination.base_url),
            session=self._dest_session,
            info={},
        )

    def _sync_workspace_headers(self):
        """Sync X-Tenant-Id from parent API clients to SDK sessions."""
        for session, client in [
            (self._source_session, self.source),
            (self._dest_session, self.dest),
        ]:
            ws_id = client.session.headers.get("X-Tenant-Id")
            if ws_id:
                session.headers["X-Tenant-Id"] = ws_id
            else:
                session.headers.pop("X-Tenant-Id", None)

    def _iter_prompt_repos(
        self,
        client: Client,
        *,
        is_archived: bool = False,
        is_public: Optional[bool] = None,
        limit: int = 100,
    ):
        """Yield prompt repos from the SDK with pagination support."""
        offset = 0

        while True:
            kwargs: Dict[str, Any] = {
                "limit": limit,
                "offset": offset,
                "is_archived": is_archived,
            }
            if is_public is not None:
                kwargs["is_public"] = is_public

            response = client.list_prompts(**kwargs)
            repos = list(getattr(response, "repos", []) or [])
            if not repos:
                break

            yield from repos

            if len(repos) < limit:
                break
            offset += len(repos)

    def _prompt_item_id(self, prompt_identifier: str) -> str:
        return f"prompt_{prompt_identifier.replace('/', '_').replace(':', '_')}"

    def _probe_commit_endpoint(
        self,
        prompt_identifier: str,
        *,
        from_source: bool,
        commit: str = "latest",
    ) -> tuple[Optional[bool], str]:
        """Probe whether commit read endpoints exist without mutating data."""
        owner, repo, _ = self._parse_prompt_identifier(prompt_identifier)
        owner_path = owner if owner else "-"
        base_url = self.config.source.base_url if from_source else self.config.destination.base_url
        api_key = self.config.source.api_key if from_source else self.config.destination.api_key
        session = self._source_session if from_source else self._dest_session
        url = f"{self._get_api_url(base_url)}/commits/{owner_path}/{repo}/{commit}"
        headers = {"x-api-key": api_key}

        try:
            response = session.get(
                url,
                headers=headers,
                params={"include_model": "true"},
                timeout=30,
            )
            if response.status_code == 404:
                return True, "endpoint_supported_resource_missing"
            response.raise_for_status()
            return True, "ok"
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in {404, 405}:
                if e.response.status_code == 404:
                    return True, "endpoint_supported_resource_missing"
                return False, "405_not_allowed"
            return False, str(e)
        except Exception as e:
            return False, str(e)

    def probe_capabilities(
        self,
        prompt_identifier: str = "codex-capability-probe",
    ) -> Dict[str, Dict[str, Any]]:
        """Probe prompt-related capabilities without mutating destination data."""
        self._sync_workspace_headers()
        capabilities: Dict[str, Dict[str, Any]] = {}

        def record(name: str, supported: Optional[bool], detail: str, probe: str) -> None:
            capabilities[name] = {
                "supported": supported,
                "detail": detail,
                "probe": probe,
            }
            self.record_capability(
                "prompts",
                name,
                supported=supported,
                detail=detail,
                probe=probe,
                evidence={"prompt_identifier": prompt_identifier},
            )

        try:
            self.dest_ls_client.list_prompts(limit=1)
            record("list_read", True, "ok", "sdk.list_prompts")
        except Exception as e:
            error_msg = str(e)
            if "405" in error_msg or "Not Allowed" in error_msg:
                record("list_read", False, "405_not_allowed", "sdk.list_prompts")
            elif "404" in error_msg:
                record("list_read", False, "404_not_found", "sdk.list_prompts")
            else:
                record("list_read", False, error_msg, "sdk.list_prompts")

        repo_lookup_supported, repo_lookup_detail = self._probe_commit_endpoint(
            prompt_identifier,
            from_source=False,
        )
        record("repo_lookup", repo_lookup_supported, repo_lookup_detail, "GET /commits/.../latest")
        record(
            "latest_commit_lookup",
            repo_lookup_supported,
            repo_lookup_detail,
            "GET /commits/.../latest",
        )
        record(
            "repo_create",
            None,
            "deferred_until_first_write",
            "POST create_prompt",
        )
        record(
            "commit_write",
            None,
            "deferred_until_first_write",
            "POST /commits/{owner}/{repo}",
        )

        return capabilities

    def check_prompts_api_available(self) -> tuple[bool, str]:
        """
        Check if the prompts API is available on the destination instance.
        Uses capability probes instead of relying only on list_prompts().

        Returns:
            Tuple of (is_available, error_message)
        """
        capabilities = self.probe_capabilities()
        if capabilities["list_read"]["supported"] or capabilities["repo_lookup"]["supported"]:
            return True, ""

        detail = capabilities["list_read"]["detail"]
        if detail == "405_not_allowed":
            return False, "Prompts API returned 405 Not Allowed - prompt listing is disabled on this instance"
        if detail == "404_not_found":
            return False, "Prompts API endpoints not found - this instance may not support prompts"
        return False, f"Prompts API capability probe failed: {detail}"

    def _get_api_url(self, base_url: str) -> str:
        """
        Prepare API URL for LangSmith Client.

        The LangSmith Client expects the full API URL including /api/v1.
        The config.base_url already has /api/v1 appended by the orchestrator.
        """
        clean_url = base_url.rstrip('/')
        # Ensure /api/v1 is present (should already be there from orchestrator)
        if not clean_url.endswith('/api/v1'):
            clean_url = f"{clean_url}/api/v1"
        return clean_url

    def _parse_prompt_identifier(self, prompt_identifier: str) -> tuple[Optional[str], str, Optional[str]]:
        """
        Parse a prompt identifier into owner, repo, and commit.

        Format: "owner/repo:commit" or "repo:commit" or "owner/repo" or "repo"

        Returns:
            Tuple of (owner, repo, commit)
        """
        commit = None
        if ":" in prompt_identifier:
            owner_repo, commit = prompt_identifier.split(":", 1)
        else:
            owner_repo = prompt_identifier

        if "/" in owner_repo:
            owner, repo = owner_repo.split("/", 1)
            return owner, repo, commit
        else:
            return None, owner_repo, commit

    def _pull_prompt_manifest(self, prompt_identifier: str, commit: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Pull a prompt manifest directly from the API without deserializing the model.

        This uses the /commits/{owner}/{repo}/{commit} endpoint to get the raw manifest,
        which includes the model configuration without trying to instantiate it.

        Args:
            prompt_identifier: Prompt repo handle (e.g., "owner/repo" or "repo")
            commit: Specific commit hash or "latest" (default: "latest")

        Returns:
            Dict containing 'commit_hash' and 'manifest', or None if failed
        """
        owner, repo, id_commit = self._parse_prompt_identifier(prompt_identifier)
        commit = commit or id_commit or "latest"

        # Build the API URL - use "-" for owner if not specified (means current user's workspace)
        owner_path = owner if owner else "-"
        endpoint = f"/commits/{owner_path}/{repo}/{commit}"

        # Add include_model=true to get the full manifest with model config
        params = {"include_model": "true"}

        try:
            url = f"{self._get_api_url(self.config.source.base_url)}{endpoint}"
            headers = {"x-api-key": self.config.source.api_key}

            response = self._source_session.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            return {
                "commit_hash": data.get("commit_hash"),
                "manifest": data.get("manifest"),
            }
        except Exception as e:
            self.log(f"Failed to pull prompt manifest for {prompt_identifier}: {e}", "error")
            return None

    def _get_latest_commit_hash(self, prompt_identifier: str) -> Optional[str]:
        """
        Get the latest commit hash for a prompt from destination.

        Args:
            prompt_identifier: Prompt repo handle

        Returns:
            The latest commit hash, or None if not found
        """
        owner, repo, _ = self._parse_prompt_identifier(prompt_identifier)
        owner_path = owner if owner else "-"

        try:
            url = f"{self._get_api_url(self.config.destination.base_url)}/commits/{owner_path}/{repo}/latest"
            headers = {"x-api-key": self.config.destination.api_key}

            response = self._dest_session.get(url, headers=headers, timeout=30)
            if response.status_code == 404:
                return None
            response.raise_for_status()

            data = response.json()
            return data.get("commit_hash")
        except Exception as e:
            self.log(f"Could not get latest commit for {prompt_identifier}: {e}", "warning")
            return None

    def _order_commits_for_replay(self, commits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Replay commit history from oldest to newest."""
        if not commits:
            return []

        children: Dict[Optional[str], List[Dict[str, Any]]] = {}
        for commit in commits:
            children.setdefault(commit.get("parent_commit_hash"), []).append(commit)

        ordered: List[Dict[str, Any]] = []
        roots = children.get(None, [])
        if not roots:
            roots = [commits[-1]]

        stack = list(reversed(roots))
        seen = set()
        while stack:
            commit = stack.pop()
            commit_hash = commit["commit_hash"]
            if commit_hash in seen:
                continue
            seen.add(commit_hash)
            ordered.append(commit)
            next_children = children.get(commit_hash, [])
            for child in reversed(next_children):
                stack.append(child)

        for commit in commits:
            if commit["commit_hash"] not in seen:
                ordered.append(commit)
        return ordered

    def _verify_prompt_commit(
        self,
        prompt_identifier: str,
        commit_hash: Optional[str] = None,
    ) -> tuple[bool, Optional[str]]:
        """Verify that a prompt commit can be fetched from destination."""
        owner, repo, _ = self._parse_prompt_identifier(prompt_identifier)
        owner_path = owner if owner else "-"
        target_commit = commit_hash or "latest"
        url = f"{self._get_api_url(self.config.destination.base_url)}/commits/{owner_path}/{repo}/{target_commit}"
        headers = {"x-api-key": self.config.destination.api_key}

        try:
            response = self._dest_session.get(
                url,
                headers=headers,
                params={"include_model": "true"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            actual_commit_hash = data.get("commit_hash") or target_commit
            return True, actual_commit_hash
        except Exception as e:
            self.log(f"Failed to verify prompt commit for {prompt_identifier}: {e}", "warning")
            return False, None

    def _export_prompt_manual_apply(
        self,
        prompt_identifier: str,
        *,
        include_all_commits: bool,
        reason: str,
        capabilities: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Export prompt data into the remediation bundle for manual apply."""
        item_id = self._prompt_item_id(prompt_identifier)
        commits = self.get_prompt_commits(prompt_identifier) if include_all_commits else []
        commit_payload = []
        if include_all_commits and commits:
            for commit in self._order_commits_for_replay(commits):
                manifest_data = self._pull_prompt_manifest(prompt_identifier, commit["commit_hash"])
                if manifest_data and manifest_data.get("manifest"):
                    commit_payload.append(
                        {
                            "source_commit_hash": commit["commit_hash"],
                            "parent_commit_hash": commit.get("parent_commit_hash"),
                            "manifest": manifest_data["manifest"],
                        }
                    )
        else:
            latest_manifest = self._pull_prompt_manifest(prompt_identifier, "latest")
            if latest_manifest and latest_manifest.get("manifest"):
                commit_payload.append(
                    {
                        "source_commit_hash": latest_manifest.get("commit_hash") or "latest",
                        "parent_commit_hash": None,
                        "manifest": latest_manifest["manifest"],
                    }
                )

        export_payload = {
            "prompt_identifier": prompt_identifier,
            "include_all_commits": include_all_commits,
            "reason": reason,
            "capabilities": capabilities or {},
            "commits": commit_payload,
            "manual_steps": [
                "Confirm prompt write support on the destination instance.",
                "Apply the exported manifests in order using the destination prompt API.",
                "Re-run `langsmith-migrator resume` after the prompt exists on destination.",
            ],
        }
        return self.export_payload(item_id, "manual_apply", export_payload)

    def _push_prompt_manifest(
        self,
        prompt_identifier: str,
        manifest: Dict[str, Any],
        parent_commit: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        auto_parent: bool = True
    ) -> Optional[str]:
        """
        Push a prompt manifest directly to the API.

        This uses the /commits/{owner}/{repo} endpoint to create a commit with the raw manifest,
        preserving the model configuration without trying to instantiate it.

        Args:
            prompt_identifier: Prompt repo handle (e.g., "owner/repo" or "repo")
            manifest: The prompt manifest dict (includes model config)
            parent_commit: Parent commit hash (optional)
            metadata: Optional dict with description, readme, tags, is_public for repo creation
            auto_parent: If True, automatically get latest commit as parent if not provided

        Returns:
            The commit hash of the created commit, or None if failed
        """
        owner, repo, _ = self._parse_prompt_identifier(prompt_identifier)
        metadata = metadata or {}

        # Step 1: Ensure the prompt repo exists
        try:
            self.log(f"Ensuring prompt repo exists: {prompt_identifier}", "info")
            self.dest_ls_client.create_prompt(
                prompt_identifier,
                description=metadata.get('description', ''),
                readme=metadata.get('readme', ''),
                tags=metadata.get('tags', []),
                is_public=metadata.get('is_public', False)
            )
            self.log(f"Created prompt repo: {prompt_identifier}", "success")
        except Exception as e:
            error_str = str(e).lower()
            if "already exists" in error_str or "409" in error_str or "conflict" in error_str:
                self.log(f"Prompt repo already exists: {prompt_identifier}", "info")
            else:
                self.log(f"Could not create prompt repo (may already exist): {e}", "warning")

        # Step 2: Get the latest commit hash from destination to use as parent
        # This is required to create a new commit on an existing repo
        if parent_commit is None and auto_parent:
            parent_commit = self._get_latest_commit_hash(prompt_identifier)
            if parent_commit:
                self.log(f"Using destination's latest commit as parent: {parent_commit[:16]}...", "info")

        # Step 3: Create a commit with the manifest
        owner_path = owner if owner else "-"
        endpoint = f"/commits/{owner_path}/{repo}"

        payload = {
            "manifest": manifest,
        }
        if parent_commit:
            payload["parent_commit"] = parent_commit

        try:
            url = f"{self._get_api_url(self.config.destination.base_url)}{endpoint}"
            headers = {
                "x-api-key": self.config.destination.api_key,
                "Content-Type": "application/json"
            }

            response = self._dest_session.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()

            data = response.json()
            commit_hash = data.get("commit", {}).get("commit_hash")
            self.log(f"Created commit: {commit_hash}", "success")
            return commit_hash
        except requests.exceptions.HTTPError as e:
            error_detail = ""
            error_dict = {}
            try:
                error_dict = e.response.json()
                error_detail = error_dict.get("detail", error_dict.get("error", str(error_dict)))
            except (ValueError, AttributeError):
                error_detail = e.response.text[:500] if e.response else ""

            # Check for "nothing to commit" (already up to date) - various phrasings
            if e.response and e.response.status_code == 409:
                error_lower = str(error_detail).lower()
                # Check for explicit "nothing to commit" messages
                if any(phrase in error_lower for phrase in ["nothing to commit", "no changes", "already up to date", "identical"]):
                    self.log(f"Prompt already up to date: {prompt_identifier}", "info")
                    return "already_up_to_date"
                # If we got a 409 with parent commit set and empty/minimal error detail,
                # it likely means the manifest is identical (nothing to commit)
                if parent_commit and (not error_detail or error_detail == "{}" or len(error_detail) < 10):
                    self.log(f"Prompt already up to date (409 with matching parent): {prompt_identifier}", "info")
                    return "already_up_to_date"

            self.log(f"Failed to push prompt manifest: {e}", "error")
            self.log(f"  Error detail: {error_detail}", "error")
            return None
        except Exception as e:
            self.log(f"Failed to push prompt manifest: {e}", "error")
            return None

    def _push_prompt_direct(self, prompt_identifier: str, prompt_obj: Any, parent_commit_hash: Optional[str] = None, prompt_metadata: Optional[Dict] = None) -> str:
        """
        Push a prompt by creating the repo first, then adding a commit.
        This bypasses SDK's _prompt_exists check which fails with 405/JSONDecodeError.

        Instead of checking if prompt exists, we:
        1. Try to create the prompt repo (may fail if exists, that's OK)
        2. Create a commit with the content

        Args:
            prompt_identifier: The prompt repo handle (e.g., "username/prompt-name")
            prompt_obj: The prompt object to push
            parent_commit_hash: Parent commit hash (optional, defaults to "latest")
            prompt_metadata: Optional dict with description, readme, tags, is_public

        Returns:
            The commit hash of the created commit
        """
        metadata = prompt_metadata or {}

        # Step 1: Try to create the prompt repo
        # This may fail with 409 if it already exists, which is fine
        try:
            self.log(f"Attempting to create prompt repo: {prompt_identifier}", "info")
            self.dest_ls_client.create_prompt(
                prompt_identifier,
                description=metadata.get('description', ''),
                readme=metadata.get('readme', ''),
                tags=metadata.get('tags', []),
                is_public=metadata.get('is_public', False)
            )
            self.log(f"✓ Created prompt repo: {prompt_identifier}", "success")
        except Exception as e:
            error_str = str(e).lower()
            # These errors mean the repo already exists, which is fine
            if "already exists" in error_str or "409" in error_str or "conflict" in error_str:
                self.log(f"Prompt repo already exists (OK): {prompt_identifier}", "info")
            else:
                # Log warning but continue - maybe it exists and we just can't detect it
                self.log(f"Could not create prompt repo (may already exist): {e}", "warning")

        # Step 2: Create a commit with the prompt content
        try:
            self.log(f"Creating commit for prompt: {prompt_identifier}", "info")
            commit_url = self.dest_ls_client.create_commit(
                prompt_identifier,
                prompt_obj,
                parent_commit_hash=parent_commit_hash or "latest"
            )
            self.log(f"✓ Created commit: {commit_url}", "success")

            # Extract commit hash from URL (typically ends with /commits/{hash})
            if "/" in str(commit_url):
                commit_hash = str(commit_url).split("/")[-1]
                return commit_hash
            return str(commit_url)

        except Exception as e:
            raise ValueError(f"Failed to create commit for prompt {prompt_identifier}: {e}")

    def list_prompts(self, is_archived: bool = False) -> List[Dict[str, Any]]:
        """List all prompts from source instance."""
        self._sync_workspace_headers()
        prompts = []
        try:
            self.log(f"Fetching prompts (archived={is_archived})...", "info")
            seen_handles = set()

            try:
                # Do not force visibility filters here. With workspace scoping enabled
                # (X-Tenant-Id), this keeps discovery limited to the selected workspace
                # instead of unioning broad public catalogs.
                for prompt in self._iter_prompt_repos(
                    self.source_ls_client,
                    is_archived=is_archived,
                    is_public=None,
                ):
                    if prompt.repo_handle in seen_handles:
                        continue
                    seen_handles.add(prompt.repo_handle)
                    prompts.append({
                        'id': str(prompt.id),
                        'repo_handle': prompt.repo_handle,
                        'description': prompt.description,
                        'readme': prompt.readme,
                        'is_public': prompt.is_public,
                        'is_archived': prompt.is_archived,
                        'tags': prompt.tags or [],
                        'num_likes': prompt.num_likes,
                        'num_downloads': prompt.num_downloads,
                        'num_commits': prompt.num_commits,
                        'updated_at': str(prompt.updated_at) if prompt.updated_at else None,
                    })
            except Exception as api_error:
                self.log(f"API error while listing prompts: {api_error}", "error")
                if prompts:
                    self.log(
                        f"Returning {len(prompts)} prompts fetched before error",
                        "warning",
                    )
                    return prompts
                raise

            self.log(f"Total prompts fetched: {len(prompts)}", "success")
            return prompts
        except Exception as e:
            self.log(f"Failed to list prompts: {e}", "error")
            import traceback
            self.log(f"Traceback: {traceback.format_exc()}", "error")
            return []

    def get_prompt_commits(self, prompt_identifier: str) -> List[Dict[str, Any]]:
        """Get all commits for a prompt."""
        try:
            commits = []
            for commit in self.source_ls_client.list_prompt_commits(prompt_identifier):
                commits.append({
                    'commit_hash': commit.commit_hash,
                    'parent_commit_hash': commit.parent_commit_hash,
                })
            return commits
        except Exception as e:
            self.log(f"Failed to get commits for prompt {prompt_identifier}: {e}", "error")
            return []

    def find_existing_prompt(self, prompt_identifier: str) -> bool:
        """
        Check if a prompt already exists in the destination.

        Args:
            prompt_identifier: The prompt repo handle (e.g., "username/prompt-name")

        Returns:
            True if the prompt exists, False otherwise
        """
        self._sync_workspace_headers()
        try:
            for prompt in self._iter_prompt_repos(
                self.dest_ls_client,
                is_public=None,
            ):
                if prompt.repo_handle == prompt_identifier:
                    return True
            return False
        except Exception as e:
            self.log(f"Could not check if prompt exists: {e}", "warning")
            return self._get_latest_commit_hash(prompt_identifier) is not None

    def migrate_prompt(
        self,
        prompt_identifier: str,
        include_all_commits: bool = False
    ) -> Optional[str]:
        """
        Migrate a single prompt with all its versions.

        Args:
            prompt_identifier: Prompt repo handle (e.g., "username/prompt-name")
            include_all_commits: Whether to migrate all commits or just latest

        Returns:
            The prompt identifier in the destination instance, or None if failed
        """
        self._sync_workspace_headers()
        item_id = self._prompt_item_id(prompt_identifier)
        self.ensure_item(
            item_id,
            "prompt",
            prompt_identifier,
            prompt_identifier,
            stage="planning",
            strategy="full_history" if include_all_commits else "latest_only",
            metadata={"include_all_commits": include_all_commits},
        )
        capabilities = self.probe_capabilities(prompt_identifier)

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would migrate prompt: {prompt_identifier}")
            return prompt_identifier

        # Check if prompt already exists
        exists = self.find_existing_prompt(prompt_identifier)

        if exists:
            if self.config.migration.skip_existing:
                self.log(f"Prompt '{prompt_identifier}' already exists, skipping", "warning")
                self.mark_migrated(
                    item_id,
                    outcome_code="prompt_already_exists",
                    evidence={"prompt_identifier": prompt_identifier},
                )
                return prompt_identifier
            else:
                self.log(f"Prompt '{prompt_identifier}' exists, updating with new commits...", "info")

        try:
            self.log(f"Migrating prompt: {prompt_identifier}", "info")
            self.checkpoint_item(item_id, stage="migrating", destination_id=prompt_identifier)
            commits_migrated = 0
            degraded_history = False
            commit_map: Dict[str, str] = {}

            if include_all_commits:
                commits = self._order_commits_for_replay(
                    self.get_prompt_commits(prompt_identifier)
                )
                self.log(f"Found {len(commits)} commits for {prompt_identifier}", "info")
                existing_dest_parent = self._get_latest_commit_hash(prompt_identifier) if exists else None

                for i, commit in enumerate(commits):
                    try:
                        commit_hash = commit["commit_hash"]
                        self.log(f"Pulling commit {commit_hash[:16]} manifest...", "info")

                        manifest_data = self._pull_prompt_manifest(prompt_identifier, commit_hash)

                        if not manifest_data or not manifest_data.get("manifest"):
                            self.log(f"Pull returned empty manifest for commit {commit_hash[:16]}", "warning")
                            continue

                        source_parent_hash = commit.get("parent_commit_hash")
                        parent_hash = commit_map.get(source_parent_hash)
                        if parent_hash is None and i == 0 and existing_dest_parent:
                            parent_hash = existing_dest_parent
                            degraded_history = True

                        new_commit_hash = self._push_prompt_manifest(
                            prompt_identifier,
                            manifest_data["manifest"],
                            parent_commit=parent_hash,
                            auto_parent=False,
                        )

                        if new_commit_hash:
                            if new_commit_hash == "already_up_to_date":
                                verified, actual_commit_hash = self._verify_prompt_commit(
                                    prompt_identifier,
                                    None,
                                )
                                if not verified:
                                    raise ValueError(
                                        f"Post-write verification failed for commit {commit_hash[:16]}"
                                    )
                                mapped_commit_hash = actual_commit_hash or parent_hash or existing_dest_parent
                            else:
                                self.record_capability(
                                    "prompts",
                                    "commit_write",
                                    supported=True,
                                    detail="write_succeeded",
                                    probe="POST /commits/{owner}/{repo}",
                                )
                                verified, actual_commit_hash = self._verify_prompt_commit(
                                    prompt_identifier,
                                    new_commit_hash,
                                )
                                if not verified:
                                    raise ValueError(
                                        f"Post-write verification failed for commit {commit_hash[:16]}"
                                    )
                                mapped_commit_hash = actual_commit_hash or new_commit_hash

                            if mapped_commit_hash:
                                commit_map[commit_hash] = mapped_commit_hash
                                if self.state:
                                    self.state.set_mapped_id(
                                        "prompt_commit",
                                        f"{prompt_identifier}:{commit_hash}",
                                        mapped_commit_hash,
                                    )
                                    self.persist_state()

                            self.log(
                                f"Migrated commit {commit_hash[:16]} -> "
                                f"{mapped_commit_hash[:16] if mapped_commit_hash else 'verified latest'}",
                                "success",
                            )
                            commits_migrated += 1
                        else:
                            self.log(f"Failed to push commit {commit_hash[:16]}", "warning")

                    except Exception as e:
                        self.log(f"Failed to migrate commit {commit['commit_hash'][:16]}: {e}", "warning")
                        import traceback
                        if self.config.migration.verbose:
                            self.log(f"Traceback: {traceback.format_exc()}", "error")
                        continue

                if commits_migrated == 0:
                    self.log("No commits migrated, falling back to latest version", "warning")
                    manifest_data = self._pull_prompt_manifest(prompt_identifier, "latest")

                    if not manifest_data or not manifest_data.get("manifest"):
                        raise ValueError("Failed to pull prompt manifest")

                    new_commit_hash = self._push_prompt_manifest(
                        prompt_identifier,
                        manifest_data["manifest"],
                    )

                    if new_commit_hash:
                        verified, actual_commit_hash = self._verify_prompt_commit(
                            prompt_identifier,
                            None if new_commit_hash == "already_up_to_date" else new_commit_hash,
                        )
                        if not verified:
                            raise ValueError("Failed to verify latest prompt commit")
                        if self.state and manifest_data.get("commit_hash") and actual_commit_hash:
                            self.state.set_mapped_id(
                                "prompt_commit",
                                f"{prompt_identifier}:{manifest_data['commit_hash']}",
                                actual_commit_hash,
                            )
                            self.persist_state()
                        self.log(f"Migrated prompt (latest only): {prompt_identifier}", "success")
                    else:
                        raise ValueError("Failed to push prompt manifest")
                else:
                    self.log(f"Successfully migrated {commits_migrated}/{len(commits)} commits", "success")
            else:
                self.log(f"Pulling latest version of {prompt_identifier} (manifest)...", "info")

                manifest_data = self._pull_prompt_manifest(prompt_identifier, "latest")

                if not manifest_data or not manifest_data.get("manifest"):
                    raise ValueError("Failed to pull prompt manifest - prompt may not exist or be inaccessible")

                self.log("Pushing manifest to destination...", "info")
                new_commit_hash = self._push_prompt_manifest(
                    prompt_identifier,
                    manifest_data["manifest"],
                )

                if new_commit_hash:
                    self.record_capability(
                        "prompts",
                        "commit_write",
                        supported=True,
                        detail="write_succeeded" if new_commit_hash != "already_up_to_date" else "already_up_to_date",
                        probe="POST /commits/{owner}/{repo}",
                    )
                    verified, actual_commit_hash = self._verify_prompt_commit(
                        prompt_identifier,
                        None if new_commit_hash == "already_up_to_date" else new_commit_hash,
                    )
                    if not verified:
                        raise ValueError("Failed post-write verification for prompt commit")
                    if self.state and manifest_data.get("commit_hash") and actual_commit_hash:
                        self.state.set_mapped_id(
                            "prompt_commit",
                            f"{prompt_identifier}:{manifest_data['commit_hash']}",
                            actual_commit_hash,
                        )
                        self.persist_state()
                    if new_commit_hash == "already_up_to_date":
                        self.log(f"Prompt already up to date: {prompt_identifier}", "success")
                    else:
                        self.log(f"Migrated prompt: {prompt_identifier} (commit: {new_commit_hash[:16]})", "success")
                else:
                    raise ValueError("Failed to push prompt manifest")

            self.checkpoint_item(item_id, stage="completed")
            if degraded_history:
                self.mark_degraded(
                    item_id,
                    "prompt_history_rebased_on_existing_destination_head",
                    next_action="Review the prompt commit history if exact ancestry must be preserved.",
                    evidence={"prompt_identifier": prompt_identifier},
                )
            else:
                self.mark_migrated(
                    item_id,
                    outcome_code="prompt_migrated",
                    evidence={
                        "prompt_identifier": prompt_identifier,
                        "include_all_commits": include_all_commits,
                    },
                )
            return prompt_identifier

        except Exception as e:
            error_msg = str(e)

            if "405" in error_msg or "Not Allowed" in error_msg:
                self.log(f"Failed to migrate prompt {prompt_identifier}: {e}", "error")
                self.record_capability(
                    "prompts",
                    "commit_write",
                    supported=False,
                    detail="405_not_allowed",
                    probe="POST /commits/{owner}/{repo}",
                )
                export_path = self._export_prompt_manual_apply(
                    prompt_identifier,
                    include_all_commits=include_all_commits,
                    reason="destination_prompt_write_not_supported",
                    capabilities=capabilities,
                )
                issue = self.record_issue(
                    "capability",
                    "prompt_write_unsupported",
                    f"Destination prompt write API is not available for {prompt_identifier}",
                    item_id=item_id,
                    next_action="Enable prompt write support or apply the exported payload manually, then run `langsmith-migrator resume`.",
                    evidence={"error": error_msg, "capabilities": capabilities},
                    export_path=export_path,
                )
                if issue:
                    self.queue_remediation(
                        issue_id=issue.id,
                        item_id=item_id,
                        export_path=export_path,
                        next_action=issue.next_action or "Apply exported prompt payload manually.",
                        command="langsmith-migrator resume",
                    )
                self.mark_exported(
                    item_id,
                    "prompt_write_unsupported",
                    next_action="Apply the exported prompt payload manually, then run `langsmith-migrator resume`.",
                    export_path=export_path,
                    evidence={"error": error_msg},
                )
            else:
                self.log(f"Failed to migrate prompt {prompt_identifier}: {e}", "error")
                issue = self.record_issue(
                    "post_write_verification" if "verification" in error_msg.lower() else "transient",
                    "prompt_migration_failed",
                    f"Prompt migration failed for {prompt_identifier}",
                    item_id=item_id,
                    next_action="Re-run `langsmith-migrator resume` after reviewing the error.",
                    evidence={"error": error_msg},
                )
                if issue:
                    self.queue_remediation(
                        issue_id=issue.id,
                        item_id=item_id,
                        next_action=issue.next_action or "Retry prompt migration.",
                        command="langsmith-migrator resume",
                    )
                self.mark_blocked(
                    item_id,
                    "prompt_migration_failed",
                    next_action="Re-run `langsmith-migrator resume` after reviewing the error.",
                    evidence={"error": error_msg},
                )

            if self.config.migration.verbose:
                import traceback
                self.log(f"Full traceback: {traceback.format_exc()}", "error")
            return None
