"""Dataset migration logic."""

from typing import Dict, List, Any, Optional, Generator, Tuple
import requests
import uuid
import tempfile
import os
import shutil

from .base import BaseMigrator
from ..api_client import APIError, NotFoundError


class DatasetMigrator(BaseMigrator):
    """Handles dataset migration with streaming and batching."""

    def list_datasets(self) -> List[Dict[str, Any]]:
        """List all datasets from source."""
        datasets = []
        count = 0
        for dataset in self.source.get_paginated("/datasets", page_size=100):
            if isinstance(dataset, dict):
                datasets.append(dataset)
                count += 1

        if self.config.migration.verbose:
            self.log(f"Fetched {count} datasets from source", "info")

        return datasets

    def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """Get a specific dataset."""
        response = self.source.get(f"/datasets/{dataset_id}")
        if not isinstance(response, dict):
            raise APIError(f"Invalid response format for dataset {dataset_id}")
        return response

    def find_existing_dataset(self, name: str) -> Optional[str]:
        """Check if dataset already exists in destination."""
        try:
            response = self.dest.get("/datasets", params={"name": name})
            datasets = response if isinstance(response, list) else []

            if len(datasets) == 1:
                first_dataset = datasets[0]
                if isinstance(first_dataset, dict):
                    return first_dataset.get("id")
            elif len(datasets) > 1:
                self.log(f"Multiple datasets found with name '{name}'", "warning")
        except NotFoundError:
            pass

        return None

    def create_dataset(self, dataset: Dict[str, Any]) -> str:
        """Create dataset in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create dataset: {dataset['name']}")
            return f"dry-run-{dataset['id']}"

        payload = {
            "name": dataset["name"],
            "description": dataset.get("description") or "",
            "created_at": dataset.get("created_at"),
            "inputs_schema_definition": dataset.get("inputs_schema_definition"),
            "outputs_schema_definition": dataset.get("outputs_schema_definition"),
            "externally_managed": dataset.get("externally_managed", False),
            "transformations": dataset.get("transformations") or [],
            "data_type": dataset.get("data_type", "kv")
        }

        response = self.dest.post("/datasets", payload)
        return response["id"]

    def stream_examples(self, dataset_id: str) -> Generator[Dict[str, Any], None, None]:
        """Stream examples from a dataset without loading all into memory."""
        # Include attachment_urls and outputs in the select to get all necessary data
        params = {
            "dataset": dataset_id,
            "select": ["attachment_urls", "outputs"]
        }
        for example in self.source.get_paginated("/examples", params=params):
            yield example

    def download_attachments(self, attachments: Dict[str, Any]) -> Dict[str, Tuple[str, str, str]]:
        """
        Download attachments from source to temporary files.

        Args:
            attachments: Dictionary of attachment URLs from source example

        Returns:
            Dictionary mapping attachment names to (mime_type, temp_file_path, original_filename) tuples
        """
        if not attachments:
            return {}

        downloaded = {}

        for key, attachment_info in attachments.items():
            try:
                # Debug: log all available fields in attachment_info
                self.log(f"Attachment '{key}' metadata fields: {list(attachment_info.keys())}", "info")
                
                # Get the presigned URL from source
                presigned_url = attachment_info.get("presigned_url")
                if not presigned_url:
                    self.log(f"No presigned URL for attachment '{key}', skipping", "warning")
                    continue

                # Download the attachment content streaming to a temp file
                with requests.get(presigned_url, verify=self.source.verify_ssl, timeout=60, stream=True) as response:
                    response.raise_for_status()
                    
                    # Create a temp file
                    fd, temp_path = tempfile.mkstemp()
                    with os.fdopen(fd, 'wb') as f:
                        shutil.copyfileobj(response.raw, f)

                # Get attachment metadata - use mime_type from attachment_info
                content_type = attachment_info.get("mime_type") or attachment_info.get("content_type", "application/octet-stream")

                # Extract filename from key by removing 'attachment.' prefix
                original_filename = key.replace("attachment.", "", 1) if key.startswith("attachment.") else key

                # Store as tuple of (mime_type, temp_file_path, original_filename)
                downloaded[key] = (content_type, temp_path, original_filename)
                file_size = os.path.getsize(temp_path)
                self.log(f"Downloaded attachment '{key}' to '{temp_path}' ({file_size} bytes)", "info")

            except Exception as e:
                self.log(f"Failed to download attachment '{key}': {e}", "error")
                continue

        return downloaded

    def create_examples_with_attachments(
        self,
        dataset_id: str,
        examples_with_attachments: List[Tuple[str, Dict[str, Any], Dict[str, Tuple[str, str, str]]]]
    ) -> Dict[str, str]:
        """
        Create examples with attachments using LangSmith SDK.

        Args:
            dataset_id: Destination dataset ID
            examples_with_attachments: List of (original_id, example_data, attachments) tuples
                where attachments is Dict[key, (mime_type, temp_file_path, filename)]

        Returns:
            Dictionary mapping original_id to new example ID
        """
        try:
            from langsmith import Client
            from langsmith.schemas import Attachment
        except ImportError:
            self.log("LangSmith SDK not installed. Install with: pip install langsmith", "error")
            raise

        # Initialize LangSmith client with destination credentials
        # SDK expects base URL without /api/v1 suffix
        sdk_url = self.dest.base_url.replace("/api/v1", "")
        api_key = self.dest.headers.get("X-API-Key") or self.dest.headers.get("x-api-key", "")
        client = Client(
            api_url=sdk_url,
            api_key=api_key,
        )

        id_mapping = {}

        # Process examples one at a time due to SDK limitations
        for original_id, example_data, attachments in examples_with_attachments:
            temp_files_to_cleanup = []
            try:
                # Convert attachment tuples to SDK Attachment objects
                # Use the original filename to preserve the file extension
                sdk_attachments = {}
                for att_name, (mime_type, temp_path, original_filename) in attachments.items():
                    try:
                        temp_files_to_cleanup.append(temp_path)
                        with open(temp_path, "rb") as f:
                            data = f.read()
                        
                        # Use the original filename which should include the extension
                        sdk_attachments[original_filename] = Attachment(
                            mime_type=mime_type,
                            data=data
                        )
                        self.log(f"Mapping attachment: {att_name} -> {original_filename} ({mime_type})", "info")
                    except Exception as e:
                        self.log(f"Failed to read attachment file {temp_path}: {e}", "error")
                        continue

                # Create example with attachments using SDK
                example_dict = {
                    "inputs": example_data.get("inputs", {}),
                    "outputs": example_data.get("outputs", {}),
                    "metadata": example_data.get("metadata", {}),
                    "attachments": sdk_attachments,
                }

                # Use SDK to create example
                self.log(f"Creating example with {len(sdk_attachments)} attachment(s)...", "info")
                created_examples = client.create_examples(
                    dataset_id=dataset_id,
                    examples=[example_dict]
                )

                self.log(f"SDK returned: {type(created_examples)} with value: {created_examples}", "info")

                # Handle both dict response and list response from SDK
                if isinstance(created_examples, dict):
                    # SDK returned dict format: {'example_ids': [...], 'count': N}
                    example_ids = created_examples.get('example_ids', [])
                    if example_ids:
                        new_id = example_ids[0]
                        id_mapping[original_id] = str(new_id)
                        self.log(f"Created example with attachments: {original_id} -> {new_id}", "success")
                    else:
                        self.log(f"SDK returned dict but no example_ids for {original_id}", "error")
                elif created_examples and len(created_examples) > 0:
                    # SDK returned list of objects
                    new_id = created_examples[0].id
                    id_mapping[original_id] = str(new_id)
                    self.log(f"Created example with attachments: {original_id} -> {new_id}", "success")
                else:
                    self.log(f"SDK returned empty or invalid response for {original_id}", "error")

            except Exception as e:
                import traceback
                self.log(f"Failed to create example {original_id} with attachments: {e}", "error")
                self.log(f"Traceback: {traceback.format_exc()}", "error")
                continue
            finally:
                # Clean up temp files for this example
                for path in temp_files_to_cleanup:
                    try:
                        if os.path.exists(path):
                            os.remove(path)
                    except Exception as e:
                        self.log(f"Failed to remove temp file {path}: {e}", "warning")

        return id_mapping

    def migrate_examples_streaming(
        self,
        source_dataset_id: str,
        dest_dataset_id: str,
        progress_callback=None
    ) -> Dict[str, str]:
        """Migrate examples using streaming to avoid memory issues."""
        if self.config.migration.dry_run:
            self.log("[DRY RUN] Would migrate examples")
            return {}

        id_mapping = {}
        batch = []
        batch_count = 0
        total_migrated = 0

        for example in self.stream_examples(source_dataset_id):
            # Debug: log example data
            self.log(f"Example {example['id']} has outputs: {bool(example.get('outputs'))}", "info")
            if example.get("outputs"):
                self.log(f"Outputs content: {example.get('outputs')}", "info")

            # Download attachments if present
            downloaded_attachments = {}
            attachment_urls = example.get("attachment_urls")
            if attachment_urls:
                self.log(f"Found attachments in example {example['id']}: {list(attachment_urls.keys())}")
                downloaded_attachments = self.download_attachments(attachment_urls)
                if downloaded_attachments:
                    self.log(f"Successfully downloaded {len(downloaded_attachments)} attachment(s)", "success")
                else:
                    self.log(f"No attachments were downloaded", "warning")

            # Prepare example for destination
            migrated_example = {
                "dataset_id": dest_dataset_id,
                "inputs": example.get("inputs", {}),
                "outputs": example.get("outputs", {}),
                "metadata": example.get("metadata", {}),
                "created_at": example.get("created_at"),
                "split": ((example.get("metadata") or {}).get("dataset_split") or "base")
            }

            batch.append((example["id"], migrated_example, downloaded_attachments))

            # Process batch when it reaches configured size
            if len(batch) >= self.config.migration.batch_size:
                batch_count += 1
                self.log(f"Processing batch {batch_count} ({len(batch)} examples)")

                # Check if any example has attachments
                has_attachments = any(len(ex[2]) > 0 for ex in batch)

                if has_attachments:
                    # Use SDK for examples with attachments
                    self.log("Using LangSmith SDK for examples with attachments")
                    try:
                        batch_id_mapping = self.create_examples_with_attachments(dest_dataset_id, batch)
                        id_mapping.update(batch_id_mapping)
                        total_migrated += len(batch_id_mapping)
                    except Exception as e:
                        self.log(f"SDK upload failed: {e}", "error")
                else:
                    # Use regular bulk endpoint
                    payloads = [ex[1] for ex in batch]
                    responses = self.dest.post_batch(
                        "/examples/bulk",
                        payloads,
                        batch_size=self.config.migration.batch_size
                    )

                    # Update ID mappings
                    for i, (original_id, _, _) in enumerate(batch):
                        if i < len(responses) and responses[i] and isinstance(responses[i], dict):
                            new_id = responses[i].get("id")
                            if new_id:
                                id_mapping[original_id] = new_id
                                total_migrated += 1

                if progress_callback:
                    progress_callback(total_migrated)

                batch.clear()

        # Process remaining examples
        if batch:
            has_attachments = any(len(ex[2]) > 0 for ex in batch)

            if has_attachments:
                # Use SDK for examples with attachments
                self.log("Using LangSmith SDK for remaining examples with attachments")
                try:
                    batch_id_mapping = self.create_examples_with_attachments(dest_dataset_id, batch)
                    id_mapping.update(batch_id_mapping)
                    total_migrated += len(batch_id_mapping)
                except Exception as e:
                    self.log(f"SDK upload failed: {e}", "error")
            else:
                # Use regular bulk endpoint
                payloads = [ex[1] for ex in batch]
                responses = self.dest.post_batch(
                    "/examples/bulk",
                    payloads,
                    batch_size=self.config.migration.batch_size
                )

                for i, (original_id, _, _) in enumerate(batch):
                    if i < len(responses) and responses[i] and isinstance(responses[i], dict):
                        new_id = responses[i].get("id")
                        if new_id:
                            id_mapping[original_id] = new_id
                            total_migrated += 1

            if progress_callback:
                progress_callback(total_migrated)

        self.log(f"Migrated {total_migrated} examples", "success")
        return id_mapping

    def migrate_dataset(
        self,
        dataset_id: str,
        include_examples: bool = True
    ) -> Tuple[str, Dict[str, str]]:
        """
        Migrate a single dataset.

        Returns:
            Tuple of (new_dataset_id, example_id_mapping)
        """
        # Get dataset details
        dataset = self.get_dataset(dataset_id)

        # Check if already exists
        if self.config.migration.skip_existing:
            existing_id = self.find_existing_dataset(dataset["name"])
            if existing_id:
                self.log(f"Dataset '{dataset['name']}' already exists, skipping", "warning")
                return existing_id, {}

        # Create dataset
        new_dataset_id = self.create_dataset(dataset)
        self.log(f"Created dataset: {dataset['name']} -> {new_dataset_id}", "success")

        # Migrate examples if requested
        example_mapping = {}
        if include_examples:
            example_mapping = self.migrate_examples_streaming(dataset_id, new_dataset_id)

        return new_dataset_id, example_mapping
