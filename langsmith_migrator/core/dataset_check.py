"""Compare migrated datasets between the source and destination instances.

A dataset copy can finish without errors and still leave the destination short,
so the counts each instance reports are compared after the copy.
"""

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from .api_client import APIError


@dataclass
class DatasetComparison:
    name: str
    source_id: str
    dest_id: Optional[str]
    source_examples: int
    dest_examples: int
    source_experiments: Optional[int] = None
    dest_experiments: Optional[int] = None
    error: Optional[str] = None

    @property
    def matches(self) -> bool:
        return (
            self.error is None
            and self.dest_id is not None
            and self.source_examples == self.dest_examples
            and self.source_experiments == self.dest_experiments
        )


def compare_datasets(
    migrator: Any,
    datasets: Iterable[Dict[str, Any]],
    include_experiments: bool = False,
) -> List[DatasetComparison]:
    """Compare example (and experiment) counts of each source dataset with its copy.

    ``migrator`` is a DatasetMigrator whose clients are scoped to the workspace
    pair being compared. The copy is found the way the migration finds it: by
    source ID with preserve_ids, by name otherwise.
    """
    results = []
    for dataset in datasets:
        source_id = str(dataset["id"])
        name = str(dataset.get("name") or source_id)
        try:
            source = migrator.source.get(f"/datasets/{source_id}") or {}
            dest_id = migrator.find_existing_dataset(name, source_id)
            dest = migrator.dest.get(f"/datasets/{dest_id}") or {} if dest_id else {}
            row = DatasetComparison(
                name=name,
                source_id=source_id,
                dest_id=dest_id,
                source_examples=int(source.get("example_count") or 0),
                dest_examples=int(dest.get("example_count") or 0),
            )
            if include_experiments:
                row.source_experiments = _experiment_count(migrator.source, source_id)
                row.dest_experiments = (
                    _experiment_count(migrator.dest, dest_id) if dest_id else 0
                )
        except APIError as e:
            row = DatasetComparison(
                name=name, source_id=source_id, dest_id=None,
                source_examples=int(dataset.get("example_count") or 0),
                dest_examples=0, error=str(e),
            )
        results.append(row)
    return results


def _experiment_count(client: Any, dataset_id: str) -> int:
    return sum(
        1
        for _ in client.get_paginated(
            "/sessions", params={"reference_dataset": dataset_id}, raise_errors=True
        )
    )
