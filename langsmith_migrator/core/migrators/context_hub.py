"""Context Hub migration logic.

Migrates Context Hub *contexts* - versioned agent and skill repos served under
``/api/v1/platform/hub/repos/{owner}/{repo}/directories`` - between two
LangSmith instances or workspaces. Contexts are the version-controlled bundles
of agent instructions (``AGENTS.md``) and tools, or reusable skills
(``SKILL.md``), that agents pull in production.

The Context Hub directories API is served by the Go backend and exposes only
three routes: GET a single commit's flattened file tree, POST a new commit, and
DELETE a repo. Crucially, there is **no endpoint that enumerates a repo's
commit history** (unlike the legacy prompt hub's ``list_prompt_commits``).
Directory repos store a flattened file tree rather than prompt manifests, so
their history cannot be replayed through the public API. This migrator
therefore copies each context at its **latest commit** as a single fresh commit
on the destination, along with repo metadata (``description``, ``readme``,
``tags``, ``is_public``).

Listing goes through the raw ``/repos`` hub endpoint (via the shared
``EnhancedAPIClient``) rather than the SDK's ``list_agents`` / ``list_skills``,
because ``/repos`` exposes the ``source`` field that the SDK's typed model
drops. This lets the migrator mirror the Context Hub UI, which lists contexts
with ``exclude_source=external`` and therefore hides externally-created repos
(e.g. Agent Builder drafts, usually with raw-UUID handles). Pulls, pushes, and
existence checks use the SDK (``pull_agent`` / ``pull_skill``, ``push_agent`` /
``push_skill``, ``agent_exists`` / ``skill_exists``).

Cross-instance caveat: an entry in a context's ``files`` map can be a *link* to
another agent or skill repo (``AgentEntry`` / ``SkillEntry``) rather than inline
content. A pinned link carries a source-instance ``commit_id`` / ``commit_hash``
that does not exist on the destination. Mirroring the Fleet agent migrator, by
default (cross-instance) these pins are stripped so the link resolves to the
destination's latest commit of the linked repo, and the downgrade is recorded
as an issue. Pass ``same_instance=True`` to preserve link pins verbatim (e.g.
workspace-to-workspace on the same deployment).
"""

from typing import Any, Dict, List, Optional

import requests
from langsmith import Client

from .base import BaseMigrator

# File-entry types that reference another hub repo instead of inlining content.
_LINK_ENTRY_TYPES = ("agent", "skill")


class ContextHubMigrator(BaseMigrator):
    """Handles Context Hub context (agent + skill repo) migration."""

    def __init__(
        self,
        source_client,
        dest_client,
        state,
        config,
        *,
        same_instance: bool = False,
        include_external: bool = False,
    ):
        super().__init__(source_client, dest_client, state, config)
        self.same_instance = same_instance
        # By default, match the Context Hub UI, which lists contexts with
        # exclude_source=external (hiding externally-created agents/skills such
        # as Agent Builder drafts with UUID handles). Set include_external=True
        # to migrate every repo the API returns.
        self.include_external = include_external

        # Managed sessions mirror PromptMigrator: the parent EnhancedAPIClient
        # may set X-Tenant-Id after init via orchestrator.set_workspace_context(),
        # so we sync that header into these sessions before each SDK operation.
        self._source_session = requests.Session()
        self._dest_session = requests.Session()

        if not config.source.verify_ssl or not config.destination.verify_ssl:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        if not config.source.verify_ssl:
            self._source_session.verify = False
        if not config.destination.verify_ssl:
            self._dest_session.verify = False

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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_api_url(self, base_url: str) -> str:
        """Return the base API URL the SDK Client expects.

        The SDK normalizes the host itself and builds the Context Hub directory
        paths relative to it, so it must receive the bare host WITHOUT a
        ``/api/v1`` suffix. Passing ``.../api/v1`` here double-appends the
        prefix when the SDK builds the hub path and yields 404s on
        ``pull_agent`` / ``pull_skill``. The orchestrator may hand us a URL
        that already has ``/api/v1`` appended, so strip it if present.
        """
        clean_url = base_url.rstrip("/")
        if clean_url.endswith("/api/v1"):
            clean_url = clean_url[: -len("/api/v1")]
        return clean_url

    def _sync_workspace_headers(self) -> None:
        """Sync X-Tenant-Id from parent API clients into the SDK sessions."""
        for session, client in [
            (self._source_session, self.source),
            (self._dest_session, self.dest),
        ]:
            ws_id = client.session.headers.get("X-Tenant-Id")
            if ws_id:
                session.headers["X-Tenant-Id"] = ws_id
            else:
                session.headers.pop("X-Tenant-Id", None)

    def _list_for_type(self, repo_type: str) -> List[Dict[str, Any]]:
        """List repos of one type (``agent`` or ``skill``) from the source.

        Uses the raw ``/repos`` hub endpoint (rather than the SDK's
        ``list_agents`` / ``list_skills``) for two reasons:

        1. The endpoint exposes the ``source`` field, which the SDK's typed
           ``Prompt`` model drops. We need it to match what the Context Hub UI
           shows: the UI lists contexts with ``exclude_source=external``, i.e.
           it hides repos whose ``source`` is ``external`` (agents/skills
           created by an external harness such as the Agent Builder, whose
           handles are usually raw UUIDs). We keep ``internal`` and ``NULL``
           source repos, which is exactly the set the UI displays.
        2. It lets us pass ``exclude_source=external`` server-side while also
           filtering client-side, so behaviour is correct whether or not the
           deployed backend honours the query parameter.

        Pass ``include_external=True`` on the migrator to disable this filter
        and migrate every repo the API returns.
        """
        contexts: List[Dict[str, Any]] = []
        seen_handles: set = set()
        offset = 0
        limit = 100
        while True:
            # Scope to tenant-private repos so we do not sweep in the public
            # catalog. ``repo_type`` narrows to agents or skills; the offset/
            # limit paging matches the SDK's hub listing.
            params: Dict[str, Any] = {
                "limit": limit,
                "offset": offset,
                "repo_type": repo_type,
                "is_public": "false",
                "is_archived": "false",
            }
            if not self.include_external:
                params["exclude_source"] = "external"
            response = self.source.get("/repos", params=params)
            repos = list((response or {}).get("repos") or [])
            if not repos:
                break
            for repo in repos:
                handle = repo.get("repo_handle")
                if not handle or handle in seen_handles:
                    continue
                # Client-side guard: the deployed backend may ignore
                # ``exclude_source``, so drop external repos here to match the
                # UI regardless.
                if not self.include_external and repo.get("source") == "external":
                    continue
                seen_handles.add(handle)
                contexts.append(
                    {
                        "id": str(repo.get("id")),
                        "repo_type": repo_type,
                        "repo_handle": handle,
                        "owner": repo.get("owner"),
                        "full_name": repo.get("full_name"),
                        "description": repo.get("description"),
                        "readme": repo.get("readme"),
                        "is_public": repo.get("is_public"),
                        "tags": repo.get("tags") or [],
                        "num_commits": repo.get("num_commits"),
                        "source": repo.get("source"),
                    }
                )
            if len(repos) < limit:
                break
            offset += len(repos)
        return contexts

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------
    def list_contexts(
        self, *, include_agents: bool = True, include_skills: bool = True
    ) -> List[Dict[str, Any]]:
        """List agent and/or skill contexts visible to the source API key."""
        self._sync_workspace_headers()
        contexts: List[Dict[str, Any]] = []
        try:
            if include_agents:
                contexts.extend(self._list_for_type("agent"))
            if include_skills:
                contexts.extend(self._list_for_type("skill"))
        except Exception as e:  # noqa: BLE001 - surface partial results to caller
            self.log(f"Failed to list contexts: {e}", "error")
            if not contexts:
                raise
            self.log(f"Returning {len(contexts)} context(s) fetched before error", "warning")
        return contexts

    def get_context_files(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Pull the latest commit's file tree for a context summary.

        Returns a dict with ``commit_hash`` and ``files`` (path -> Entry model).
        """
        self._sync_workspace_headers()
        identifier = summary["repo_handle"]
        if summary["repo_type"] == "agent":
            ctx = self.source_ls_client.pull_agent(identifier)
        else:
            ctx = self.source_ls_client.pull_skill(identifier)
        return {"commit_hash": ctx.commit_hash, "files": dict(ctx.files)}

    def context_exists(self, summary: Dict[str, Any]) -> bool:
        """Check whether a context with this handle exists on the destination."""
        self._sync_workspace_headers()
        identifier = summary["repo_handle"]
        try:
            if summary["repo_type"] == "agent":
                return self.dest_ls_client.agent_exists(identifier)
            return self.dest_ls_client.skill_exists(identifier)
        except Exception as e:  # noqa: BLE001 - best-effort existence check
            self.log(f"Failed to check for existing context {identifier}: {e}", "warning")
            return False

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------
    def _prepare_files(
        self, files: Dict[str, Any], *, item_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Sanitize a pulled file map for pushing to the destination.

        Inline ``file`` entries carry over verbatim. Link entries
        (``agent`` / ``skill``) reference other repos by handle; cross-instance
        their pinned ``commit_id`` / ``commit_hash`` do not exist on the
        destination, so we strip the pins (letting the link resolve to the
        destination's latest commit of the linked repo) and record the
        downgrade. With ``same_instance`` the pins are preserved verbatim.
        """
        prepared: Dict[str, Any] = {}
        stripped_links: List[str] = []
        for path, entry in files.items():
            entry_type = getattr(entry, "type", None)
            if entry_type not in _LINK_ENTRY_TYPES:
                # Inline file entry (or anything else) - carry it over as-is.
                prepared[path] = entry
                continue

            if self.same_instance:
                prepared[path] = entry
                continue

            # Cross-instance: strip the source-instance commit pin so the link
            # resolves to the linked repo's latest commit on the destination.
            unpinned = entry.model_copy(update={"commit_id": None, "commit_hash": None})
            prepared[path] = unpinned
            if getattr(entry, "commit_id", None) or getattr(entry, "commit_hash", None):
                stripped_links.append(path)

        if stripped_links:
            self.record_issue(
                "degraded",
                "context_link_pin_stripped",
                (
                    f"Stripped source commit pins from {len(stripped_links)} linked "
                    "repo reference(s); links now resolve to the destination's "
                    "latest commit of each linked repo."
                ),
                item_id=item_id,
                next_action=(
                    "Migrate the linked agent/skill repos and, if you need "
                    "reproducibility, re-pin the links on the destination."
                ),
                evidence={"paths": stripped_links},
            )
        return prepared

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------
    def create_context(self, summary: Dict[str, Any], *, item_id: Optional[str] = None) -> str:
        """Push a context to the destination at its latest commit.

        Creates the repo if it does not exist (with metadata) and commits the
        prepared file tree. Returns the destination commit URL.
        """
        self._sync_workspace_headers()
        snapshot = self.get_context_files(summary)
        files = self._prepare_files(snapshot["files"], item_id=item_id)

        identifier = summary["repo_handle"]
        push_kwargs: Dict[str, Any] = {
            "files": files,
            "description": summary.get("description"),
            "readme": summary.get("readme"),
            "tags": summary.get("tags") or None,
            "is_public": summary.get("is_public"),
        }

        if summary["repo_type"] == "agent":
            url = self.dest_ls_client.push_agent(identifier, **push_kwargs)
        else:
            url = self.dest_ls_client.push_skill(identifier, **push_kwargs)
        self.log(f"Migrated context {identifier}: {url}", "success")
        return url
