"""Context Hub migration logic.

Migrates Context Hub *contexts* - versioned agent and skill repos served under
``/api/v1/platform/hub/repos/{owner}/{repo}/directories`` - between two
LangSmith instances or workspaces. Contexts are the version-controlled bundles
of agent instructions (``AGENTS.md``) and tools, or reusable skills
(``SKILL.md``), that agents pull in production.

The Context Hub directories API is served by the Go backend. Reads/writes of a
commit's flattened file tree go through the directories endpoints (GET a single
commit, POST a new commit, DELETE a repo), while the commit *chain* is
enumerable via ``list_prompt_commits`` (which also works for directory-type
repos). This migrator therefore replays the **full commit history** by default:
each source commit is pulled by hash and pushed oldest->newest, so the
destination reproduces the source's history - and, because directory commits
are content-addressed, the same commit hashes. Repo metadata (``description``,
``readme``, ``tags``, ``is_public``) is applied on the first commit. Set
``include_all_commits=False`` to copy only the latest commit as a single fresh
commit instead.

Commit tags are copied by default too. This includes the ``production`` /
``staging`` environment tags that back the Context Hub "promote" feature (a tag
is just a named pointer to a commit). After a repo's commits are migrated, each
source tag is re-created on the destination pointing at the commit with the same
content-addressed hash. Set ``migrate_tags=False`` to skip tags.

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
        include_all_commits: bool = True,
        migrate_tags: bool = True,
    ):
        super().__init__(source_client, dest_client, state, config)
        self.same_instance = same_instance
        # Commit tags (including the environment tags ``production`` /
        # ``staging`` that back the Context Hub "promote" feature) are copied by
        # default: after a repo's commits are migrated, each source tag is
        # re-created on the destination pointing at the same commit (matched by
        # content-addressed commit hash). Set migrate_tags=False to skip tags.
        self.migrate_tags = migrate_tags
        # By default, match the Context Hub UI, which lists contexts with
        # exclude_source=external (hiding externally-created agents/skills such
        # as Agent Builder drafts with UUID handles). Set include_external=True
        # to migrate every repo the API returns.
        self.include_external = include_external
        # Full commit history is replayed by default: every source commit is
        # pushed oldest->newest so the destination reproduces the source's
        # history (and, because directory commits are content-addressed, the
        # same commit hashes). Set include_all_commits=False to copy only the
        # latest commit as a single fresh commit on the destination.
        self.include_all_commits = include_all_commits

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

    def get_context_files(
        self, summary: Dict[str, Any], *, version: Optional[str] = None
    ) -> Dict[str, Any]:
        """Pull a commit's file tree for a context summary.

        Pulls the latest commit by default, or a specific commit hash/tag when
        ``version`` is given.

        Returns a dict with ``commit_hash`` and ``files`` (path -> Entry model).
        """
        self._sync_workspace_headers()
        identifier = summary["repo_handle"]
        puller = (
            self.source_ls_client.pull_agent
            if summary["repo_type"] == "agent"
            else self.source_ls_client.pull_skill
        )
        ctx = puller(identifier, version=version) if version else puller(identifier)
        return {"commit_hash": ctx.commit_hash, "files": dict(ctx.files)}

    def list_context_commits(self, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return the source commit chain for a context, oldest commit first.

        Uses ``list_prompt_commits`` (which works for directory-type repos too)
        to enumerate the full history, then reverses it so callers can replay
        commits in creation order.
        """
        self._sync_workspace_headers()
        identifier = summary["repo_handle"]
        commits = list(self.source_ls_client.list_prompt_commits(identifier))
        # list_prompt_commits yields newest-first; replay needs oldest-first.
        return [
            {
                "commit_hash": cm.commit_hash,
                "parent_commit_hash": cm.parent_commit_hash,
            }
            for cm in reversed(commits)
        ]

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
    def _push_commit(
        self,
        summary: Dict[str, Any],
        files: Dict[str, Any],
        *,
        parent_commit: Optional[str] = None,
        with_metadata: bool = True,
    ) -> str:
        """Push one commit to the destination repo, returning the commit URL.

        Repo metadata (description/readme/tags/is_public) is only sent on the
        first commit (``with_metadata=True``); later commits in a history replay
        omit it to avoid redundant metadata patches.
        """
        identifier = summary["repo_handle"]
        push_kwargs: Dict[str, Any] = {"files": files}
        if parent_commit:
            push_kwargs["parent_commit"] = parent_commit
        if with_metadata:
            push_kwargs.update(
                {
                    "description": summary.get("description"),
                    "readme": summary.get("readme"),
                    "tags": summary.get("tags") or None,
                    "is_public": summary.get("is_public"),
                }
            )
        pusher = (
            self.dest_ls_client.push_agent
            if summary["repo_type"] == "agent"
            else self.dest_ls_client.push_skill
        )
        return pusher(identifier, **push_kwargs)

    def get_dest_head(self, summary: Dict[str, Any]) -> Optional[str]:
        """Return the destination repo's current head commit hash, if any."""
        self._sync_workspace_headers()
        identifier = summary["repo_handle"]
        puller = (
            self.dest_ls_client.pull_agent
            if summary["repo_type"] == "agent"
            else self.dest_ls_client.pull_skill
        )
        try:
            return puller(identifier).commit_hash
        except Exception:  # noqa: BLE001 - head may not exist yet
            return None

    # ------------------------------------------------------------------
    # Commit tags (including production/staging environment tags)
    # ------------------------------------------------------------------
    def _tags_endpoint(self, summary: Dict[str, Any], tag_name: Optional[str] = None) -> str:
        """Build the ``/repos/{owner}/{repo}/tags`` endpoint for a context.

        Contexts are migrated under the current tenant, so the owner segment is
        ``-`` (current tenant) to match how the repos were created.
        """
        base = f"/repos/-/{summary['repo_handle']}/tags"
        return f"{base}/{tag_name}" if tag_name else base

    def list_source_tags(self, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return the source repo's commit tags (name + target commit hash)."""
        try:
            resp = self.source.get(self._tags_endpoint(summary))
        except Exception as e:  # noqa: BLE001 - tags are best-effort
            self.log(f"Failed to list tags for {summary['repo_handle']}: {e}", "warning")
            return []
        tags = resp if isinstance(resp, list) else (resp or {}).get("tags") or []
        return [
            {"tag_name": t.get("tag_name"), "commit_hash": t.get("commit_hash")}
            for t in tags
            if isinstance(t, dict) and t.get("tag_name") and t.get("commit_hash")
        ]

    def _dest_commit_id_for_hash(
        self, summary: Dict[str, Any], commit_hash: str
    ) -> Optional[str]:
        """Resolve a commit hash to the destination repo's commit_id, if present.

        The destination reproduces the source's content-addressed commit hashes,
        so the same hash identifies the corresponding destination commit.
        """
        puller = (
            self.dest_ls_client.pull_agent
            if summary["repo_type"] == "agent"
            else self.dest_ls_client.pull_skill
        )
        try:
            return str(puller(summary["repo_handle"], version=commit_hash).commit_id)
        except Exception:  # noqa: BLE001 - hash may not exist on destination
            return None

    def migrate_context_tags(
        self, summary: Dict[str, Any], *, item_id: Optional[str] = None
    ) -> None:
        """Re-create the source repo's commit tags on the destination.

        Each source tag (environment tags ``production`` / ``staging`` and any
        custom commit tags) is pointed at the destination commit with the same
        content-addressed hash. Tags whose target commit is not present on the
        destination (e.g. under ``--latest-only``) are skipped and reported.
        """
        self._sync_workspace_headers()
        source_tags = self.list_source_tags(summary)
        if not source_tags:
            return

        migrated: List[str] = []
        skipped: List[str] = []
        for tag in source_tags:
            dest_commit_id = self._dest_commit_id_for_hash(summary, tag["commit_hash"])
            if not dest_commit_id:
                skipped.append(tag["tag_name"])
                continue
            endpoint = self._tags_endpoint(summary)
            try:
                # Create the tag; if it already exists, re-point it with PATCH.
                self.dest.post(
                    endpoint,
                    {"tag_name": tag["tag_name"], "commit_id": dest_commit_id},
                )
            except Exception:  # noqa: BLE001 - fall back to update on conflict
                try:
                    self.dest.patch(
                        self._tags_endpoint(summary, tag["tag_name"]),
                        {"commit_id": dest_commit_id},
                    )
                except Exception as e:  # noqa: BLE001 - report and continue
                    self.log(
                        f"Failed to set tag {tag['tag_name']} on "
                        f"{summary['repo_handle']}: {e}",
                        "warning",
                    )
                    skipped.append(tag["tag_name"])
                    continue
            migrated.append(tag["tag_name"])

        if migrated:
            self.log(
                f"Tagged {summary['repo_handle']}: {', '.join(sorted(migrated))}",
                "success",
            )
        if skipped:
            self.record_issue(
                "degraded",
                "context_tag_not_migrated",
                (
                    f"Could not migrate {len(skipped)} tag(s) for "
                    f"{summary['repo_handle']}: {', '.join(sorted(skipped))}. "
                    "The target commit is not present on the destination."
                ),
                item_id=item_id,
                next_action=(
                    "Re-run without --latest-only so the tagged commit is "
                    "migrated, then set the tag on the destination."
                ),
                evidence={"tags": sorted(skipped)},
            )

    def create_context(self, summary: Dict[str, Any], *, item_id: Optional[str] = None) -> str:
        """Push a context to the destination and return the final commit URL.

        Replays the full source commit history by default: every source commit
        is pulled by hash and pushed oldest->newest, chaining ``parent_commit``
        so the destination reproduces the source's chain (and, because directory
        commits are content-addressed, the same hashes). Repo metadata is
        applied on the first commit. With ``include_all_commits=False`` only the
        source's latest commit is copied as a single fresh commit.
        """
        self._sync_workspace_headers()
        identifier = summary["repo_handle"]

        # Determine the commits to replay (oldest-first). Latest-only mode, or a
        # repo whose history cannot be enumerated, collapses to a single commit.
        commits: List[Dict[str, Any]] = []
        if self.include_all_commits:
            commits = self.list_context_commits(summary)
        if not commits:
            snapshot = self.get_context_files(summary)
            files = self._prepare_files(snapshot["files"], item_id=item_id)
            url = self._push_commit(summary, files, with_metadata=True)
            self.log(f"Migrated context {identifier}: {url}", "success")
            if self.migrate_tags:
                self.migrate_context_tags(summary, item_id=item_id)
            return url

        url = ""
        parent: Optional[str] = None
        for index, commit in enumerate(commits):
            snapshot = self.get_context_files(summary, version=commit["commit_hash"])
            files = self._prepare_files(snapshot["files"], item_id=item_id)
            url = self._push_commit(
                summary,
                files,
                parent_commit=parent,
                with_metadata=(index == 0),
            )
            # Chain the next commit onto the destination's new head.
            parent = self.get_dest_head(summary) or snapshot["commit_hash"]
        self.log(
            f"Migrated context {identifier} with {len(commits)} commit(s): {url}",
            "success",
        )
        if self.migrate_tags:
            self.migrate_context_tags(summary, item_id=item_id)
        return url
