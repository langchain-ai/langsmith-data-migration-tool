"""Prompt migration logic."""

import json
from typing import Dict, List, Any, Optional
from langsmith import Client
import requests

from .base import BaseMigrator
from ..api_client import NotFoundError


class PromptMigrator(BaseMigrator):
    """Handles prompt migration."""

    def __init__(self, source_client, dest_client, state, config):
        super().__init__(source_client, dest_client, state, config)

        # Create client kwargs with SSL verification settings
        source_kwargs = {
            "api_key": config.source.api_key,
            "api_url": self._get_api_url(config.source.base_url),
            "info": {}  # Skip automatic /info fetch to avoid compatibility issues
        }
        dest_kwargs = {
            "api_key": config.destination.api_key,
            "api_url": self._get_api_url(config.destination.base_url),
            "info": {}  # Skip automatic /info fetch to avoid compatibility issues
        }

        # Add custom session with SSL verification disabled if needed
        # Note: verify_ssl is stored in config.source and config.destination, not config.migration
        if not config.source.verify_ssl or not config.destination.verify_ssl:
            import requests
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            # Create sessions with SSL verification disabled
            if not config.source.verify_ssl:
                source_session = requests.Session()
                source_session.verify = False
                source_kwargs["session"] = source_session

            if not config.destination.verify_ssl:
                dest_session = requests.Session()
                dest_session.verify = False
                dest_kwargs["session"] = dest_session

        # Create LangSmith SDK clients for prompt operations
        self.source_ls_client = Client(**source_kwargs)
        self.dest_ls_client = Client(**dest_kwargs)

    def check_prompts_api_available(self) -> tuple[bool, str]:
        """
        Check if the prompts API is available on the destination instance.
        Tests both read (list) and write (push) operations.

        Returns:
            Tuple of (is_available, error_message)
        """
        # Test 1: Check if we can list prompts (READ access)
        try:
            response = self.dest_ls_client.list_prompts(limit=1)
        except Exception as e:
            error_msg = str(e)
            if "405" in error_msg or "Not Allowed" in error_msg:
                return False, "Prompts API returned 405 Not Allowed - prompts may not be enabled on this instance"
            elif "404" in error_msg:
                return False, "Prompts API endpoints not found - this instance may not support prompts"
            else:
                return False, f"Prompts API read check failed: {error_msg}"

        # Note: We're NOT testing write access here because:
        # 1. Testing with fake prompts may give false negatives
        # 2. The SDK's push_prompt has an existence check that may fail even if push works
        # 3. Better to attempt actual migration and provide good error messages if it fails
        return True, ""

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
        prompts = []
        try:
            offset = 0
            limit = 100
            max_iterations = 1000  # Safety limit to prevent infinite loops
            iterations = 0

            self.log(f"Fetching prompts (archived={is_archived})...", "info")

            while iterations < max_iterations:
                iterations += 1

                self.log(f"Fetching prompts: offset={offset}, limit={limit}", "info")

                try:
                    response = self.source_ls_client.list_prompts(
                        limit=limit,
                        offset=offset,
                        is_archived=is_archived,
                        is_public=False  # Only fetch private prompts from this tenant
                    )
                except Exception as api_error:
                    self.log(f"API error while listing prompts: {api_error}", "error")
                    # If we got some prompts already, return them
                    if prompts:
                        self.log(f"Returning {len(prompts)} prompts fetched before error", "warning")
                        return prompts
                    raise

                # Note: ListPromptsResponse has 'repos' attribute, not 'prompts'
                if not response or not hasattr(response, 'repos') or not response.repos:
                    self.log(f"No more prompts found at offset {offset}", "info")
                    break

                batch_size = len(response.repos)
                self.log(f"Retrieved {batch_size} prompt(s) in this batch", "info")

                for prompt in response.repos:
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

                # If we got fewer results than the limit, we've reached the end
                if batch_size < limit:
                    self.log(f"Reached end of prompts (got {batch_size} < {limit})", "info")
                    break

                offset += batch_size

            if iterations >= max_iterations:
                self.log(f"Reached maximum iteration limit ({max_iterations}), stopping", "warning")

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
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would migrate prompt: {prompt_identifier}")
            return prompt_identifier

        try:
            self.log(f"Migrating prompt: {prompt_identifier}", "info")

            commits_migrated = 0

            if include_all_commits:
                commits = self.get_prompt_commits(prompt_identifier)
                self.log(f"Found {len(commits)} commits for {prompt_identifier}", "info")

                for i, commit in enumerate(commits):
                    try:
                        commit_hash = commit['commit_hash']
                        self.log(f"Attempting to pull commit {commit_hash[:16]}...", "info")

                        # Try the prompt:commit format (common in LangSmith)
                        prompt_obj = self.source_ls_client.pull_prompt(f"{prompt_identifier}:{commit_hash}")

                        if not prompt_obj:
                            self.log(f"Pull returned empty object for commit {commit_hash[:16]}", "warning")
                            continue

                        parent_hash = commit['parent_commit_hash'] if i > 0 else None

                        try:
                            new_commit_hash = self.dest_ls_client.push_prompt(
                                prompt_identifier,
                                object=prompt_obj,
                                parent_commit_hash=parent_hash
                            )

                            self.log(f"Migrated commit {commit_hash[:16]} -> {new_commit_hash[:16] if new_commit_hash else 'unknown'}", "success")
                            commits_migrated += 1
                        except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as jde:
                            # SDK's _prompt_exists check failed - try using direct API approach
                            self.log(f"SDK existence check failed, trying alternative approach...", "warning")
                            try:
                                new_commit_hash = self._push_prompt_direct(
                                    prompt_identifier,
                                    prompt_obj,
                                    parent_hash
                                )
                                self.log(f"Migrated commit {commit_hash[:16]} using direct API -> {new_commit_hash[:16] if new_commit_hash else 'unknown'}", "success")
                                commits_migrated += 1
                            except Exception as direct_error:
                                self.log(f"Direct API push also failed for commit {commit['commit_hash'][:16]}: {direct_error}", "warning")
                                continue

                    except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as e:
                        self.log(f"JSON decode error for commit {commit['commit_hash'][:16]}: {e}", "warning")
                        self.log(f"This may indicate the prompt format is incompatible or the SDK version mismatch", "warning")
                        continue
                    except AttributeError as e:
                        self.log(f"SDK attribute error for commit {commit['commit_hash'][:16]}: {e}", "warning")
                        self.log(f"SDK may not support this prompt format", "warning")
                        continue
                    except Exception as e:
                        self.log(f"Failed to migrate commit {commit['commit_hash'][:16]}: {e}", "warning")
                        import traceback
                        if self.config.migration.verbose:
                            self.log(f"Traceback: {traceback.format_exc()}", "error")
                        continue

                # Fallback: if no commits were successfully migrated, try latest version
                if commits_migrated == 0:
                    self.log(f"No commits migrated, falling back to latest version", "warning")
                    try:
                        prompt_obj = self.source_ls_client.pull_prompt(prompt_identifier)

                        if not prompt_obj:
                            raise ValueError("pull_prompt returned empty object")

                        try:
                            new_commit_hash = self.dest_ls_client.push_prompt(
                                prompt_identifier,
                                object=prompt_obj
                            )
                        except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as jde:
                            # SDK's _prompt_exists check failed - try using direct API approach
                            self.log(f"SDK existence check failed, trying alternative approach...", "warning")
                            new_commit_hash = self._push_prompt_direct(
                                prompt_identifier,
                                prompt_obj,
                                None
                            )

                        self.log(f"Migrated prompt (latest only): {prompt_identifier} (commit: {new_commit_hash[:16] if new_commit_hash else 'unknown'})", "success")
                        return prompt_identifier
                    except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as jde:
                        self.log(f"Fallback migration failed: JSON decode error during SDK operation", "error")
                        self.log(f"This prompt may not be accessible via the SDK or may be in an incompatible format", "error")
                        self.log(f"Hint: These prompts might be created via API and not accessible through the prompt hub", "warning")
                        raise ValueError(f"Prompt not accessible via SDK: {jde}")
                    except Exception as fallback_error:
                        self.log(f"Fallback migration also failed: {fallback_error}", "error")
                        raise
                else:
                    self.log(f"Successfully migrated {commits_migrated}/{len(commits)} commits", "success")
            else:
                # Migrate only latest version
                self.log(f"Pulling latest version of {prompt_identifier}", "info")
                try:
                    prompt_obj = self.source_ls_client.pull_prompt(prompt_identifier)

                    if not prompt_obj:
                        raise ValueError("pull_prompt returned empty object")

                    self.log(f"Pushing to destination...", "info")
                    try:
                        new_commit_hash = self.dest_ls_client.push_prompt(
                            prompt_identifier,
                            object=prompt_obj
                        )
                    except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as jde:
                        # SDK's _prompt_exists check failed - try using direct API approach
                        self.log(f"SDK existence check failed, trying alternative approach...", "warning")
                        new_commit_hash = self._push_prompt_direct(
                            prompt_identifier,
                            prompt_obj,
                            None
                        )

                    self.log(f"Migrated prompt: {prompt_identifier} (commit: {new_commit_hash[:16] if new_commit_hash else 'unknown'})", "success")
                except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as e:
                    self.log(f"JSON decode error: {e}", "error")
                    self.log(f"This prompt may not be accessible via the SDK or may be in an incompatible format", "error")
                    self.log(f"Hint: These prompts might be created via API and not accessible through the prompt hub", "warning")
                    raise ValueError(f"Prompt not accessible via SDK: {e}")
                except Exception as e:
                    self.log(f"Error during pull/push: {e}", "error")
                    raise

            return prompt_identifier

        except Exception as e:
            error_msg = str(e)
            
            # Provide specific guidance for 405 errors
            if "405" in error_msg or "Not Allowed" in error_msg:
                self.log(f"Failed to migrate prompt {prompt_identifier}: {e}", "error")
                self.log("", "error")
                self.log("The destination instance does not support prompt write operations.", "error")
                self.log("This feature may not be enabled or available on your LangSmith instance.", "error")
                self.log("Please contact your LangSmith administrator for assistance.", "error")
            else:
                self.log(f"Failed to migrate prompt {prompt_identifier}: {e}", "error")
            
            if self.config.migration.verbose:
                import traceback
                self.log(f"Full traceback: {traceback.format_exc()}", "error")
            return None
