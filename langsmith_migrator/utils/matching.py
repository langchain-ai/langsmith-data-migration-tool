"""Helpers for duplicate-aware exact-name matching."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Tuple


def build_name_buckets(
    records: Iterable[dict],
    *,
    name_key: str = "name",
    id_key: str = "id",
) -> Dict[str, List[str]]:
    """Index records by exact name, preserving duplicates."""
    buckets: Dict[str, List[str]] = defaultdict(list)
    for record in records:
        name = record.get(name_key)
        record_id = record.get(id_key)
        if name and record_id:
            buckets[name].append(record_id)
    return dict(buckets)


def unique_name_map(
    records: Iterable[dict],
    *,
    name_key: str = "name",
    id_key: str = "id",
) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """Return unique exact-name matches and a duplicate index."""
    buckets = build_name_buckets(records, name_key=name_key, id_key=id_key)
    unique = {name: ids[0] for name, ids in buckets.items() if len(ids) == 1}
    duplicates = {name: ids for name, ids in buckets.items() if len(ids) > 1}
    return unique, duplicates
