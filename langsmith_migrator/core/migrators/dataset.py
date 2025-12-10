"""Dataset migration logic."""

from typing import Dict, List, Any, Optional, Generator, Tuple
import requests
import uuid
import tempfile
import os
import shutil
import json
import hashlib

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

    def _hash_inputs(self, inputs: Dict[str, Any]) -> str:
        """Create a stable hash of example inputs for matching."""
        # Sort keys to ensure consistent ordering
        serialized = json.dumps(inputs, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    def get_existing_examples(self, dataset_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Get existing examples from destination dataset, indexed by inputs hash.

        Returns:
            Dict mapping inputs_hash -> example dict (with id, inputs, outputs, metadata)
        """
        existing = {}
        params = {
            "dataset": dataset_id,
            "select": ["inputs", "outputs", "metadata"]
        }
        for example in self.dest.get_paginated("/examples", params=params):
            if isinstance(example, dict) and example.get("inputs"):
                inputs_hash = self._hash_inputs(example["inputs"])
                existing[inputs_hash] = example
        return existing

    def update_example(self, example_id: str, example_data: Dict[str, Any]) -> None:
        """Update an existing example in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update example: {example_id}")
            return

        payload = {
            "inputs": example_data.get("inputs"),
            "outputs": example_data.get("outputs"),
            "metadata": example_data.get("metadata"),
        }
        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}

        self.dest.patch(f"/examples/{example_id}", payload)

    def update_dataset(self, dataset_id: str, dataset: Dict[str, Any]) -> None:
        """Update existing dataset in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update dataset: {dataset['name']} ({dataset_id})")
            return

        payload = {
            "name": dataset["name"],
            "description": dataset.get("description") or "",
            "inputs_schema_definition": dataset.get("inputs_schema_definition"),
            "outputs_schema_definition": dataset.get("outputs_schema_definition"),
            "transformations": dataset.get("transformations") or [],
            "metadata": dataset.get("metadata"),
        }
        
        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}

        self.dest.patch(f"/datasets/{dataset_id}", payload)
        self.log(f"Updated dataset: {dataset['name']} ({dataset_id})", "success")

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
            "data_type": dataset.get("data_type", "kv"),
            "metadata": dataset.get("metadata"),
        }

        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}

        response = self.dest.post("/datasets", payload)
        return response["id"]

    def stream_examples(self, dataset_id: str) -> Generator[Dict[str, Any], None, None]:
        """Stream examples from a dataset without loading all into memory."""
        # Include attachment_urls, outputs, and metadata in the select to get all necessary data
        params = {
            "dataset": dataset_id,
            "select": ["attachment_urls", "outputs", "metadata"]
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
                # Suppress SSL warnings if verification is disabled
                if not self.source.verify_ssl:
                    import urllib3
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

    def _create_example_individual_with_attachments(
        self,
        dataset_id: str,
        example_data: Dict[str, Any],
        attachments: Dict[str, tuple]
    ) -> Optional[str]:
        """
        Create a single example with attachments using direct API (fallback method).

        Args:
            dataset_id: Destination dataset ID
            example_data: Example data dict with inputs, outputs, metadata
            attachments: Dict mapping attachment name to (mime_type, data) tuple

        Returns:
            Created example ID or None if failed
        """
        try:
            # First, create the example without attachments using direct API
            payload = {
                "dataset_id": dataset_id,
                "inputs": example_data.get("inputs", {}),
                "outputs": example_data.get("outputs", {}),
                "metadata": example_data.get("metadata", {}),
            }

            # Use POST /examples (singular) to create one example
            response = self.dest.post("/examples", payload)
            example_id = response.get("id")

            if not example_id:
                self.log("Failed to create example: no ID returned", "error")
                return None

            self.log(f"Created example {example_id}, now uploading {len(attachments)} attachment(s)", "info")

            # Now upload attachments one by one
            # This requires getting presigned URLs for upload
            for att_name, (mime_type, data) in attachments.items():
                try:
                    # Get presigned URL for upload
                    upload_url_response = self.dest.post(
                        f"/examples/{example_id}/attachments",
                        {
                            "name": att_name,
                            "content_type": mime_type,
                        }
                    )

                    presigned_url = upload_url_response.get("presigned_url") or upload_url_response.get("upload_url")

                    if not presigned_url:
                        self.log(f"No presigned URL for attachment {att_name}", "warning")
                        continue

                    # Upload attachment data to presigned URL
                    import requests
                    # Suppress SSL warnings if verification is disabled
                    if not self.dest.verify_ssl:
                        import urllib3
                        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

                    upload_response = requests.put(
                        presigned_url,
                        data=data,
                        headers={"Content-Type": mime_type},
                        verify=self.dest.verify_ssl
                    )
                    upload_response.raise_for_status()

                    self.log(f"Uploaded attachment {att_name} for example {example_id}", "success")

                except Exception as att_error:
                    self.log(f"Failed to upload attachment {att_name}: {att_error}", "warning")
                    # Continue with other attachments even if one fails

            return example_id

        except Exception as e:
            self.log(f"Individual example creation failed: {e}", "error")
            return None

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

        # Create client kwargs
        client_kwargs = {
            "api_url": sdk_url,
            "api_key": api_key,
            "info": {}  # Skip automatic /info fetch to avoid compatibility issues
        }

        # Add custom session with SSL verification disabled if needed
        if not self.config.destination.verify_ssl:
            import requests
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            # Create session with SSL verification disabled
            session = requests.Session()
            session.verify = False
            client_kwargs["session"] = session

            self.log("SSL verification disabled for LangSmith SDK client", "info")

        client = Client(**client_kwargs)

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
                error_str = str(e)

                # Check if it's a 405 error (endpoint not allowed)
                if "405" in error_str or "Not Allowed" in error_str:
                    self.log(f"Bulk endpoint not allowed, falling back to individual creation for {original_id}", "warning")

                    # Fall back to individual example creation
                    try:
                        # Convert SDK Attachment objects to (mime_type, data) tuples
                        attachment_tuples = {}
                        for att_name, attachment_obj in sdk_attachments.items():
                            attachment_tuples[att_name] = (attachment_obj.mime_type, attachment_obj.data)

                        new_id = self._create_example_individual_with_attachments(
                            dataset_id,
                            example_data,
                            attachment_tuples
                        )
                        if new_id:
                            id_mapping[original_id] = str(new_id)
                            self.log(f"Created example with attachments (fallback): {original_id} -> {new_id}", "success")
                    except Exception as fallback_error:
                        self.log(f"Fallback creation also failed for {original_id}: {fallback_error}", "error")
                        continue
                else:
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
        progress_callback=None,
        upsert: bool = True
    ) -> Dict[str, str]:
        """
        Migrate examples using streaming to avoid memory issues.

        Args:
            source_dataset_id: Source dataset ID
            dest_dataset_id: Destination dataset ID
            progress_callback: Optional callback for progress updates
            upsert: If True, update existing examples matched by inputs hash.
                    If False, always create new examples.
        """
        if self.config.migration.dry_run:
            self.log("[DRY RUN] Would migrate examples")
            return {}

        id_mapping = {}
        batch = []
        batch_count = 0
        total_migrated = 0
        total_updated = 0
        total_created = 0

        # If upserting, get existing examples from destination indexed by inputs hash
        existing_examples = {}
        if upsert:
            self.log("Fetching existing examples from destination for upsert matching...")
            existing_examples = self.get_existing_examples(dest_dataset_id)
            if existing_examples:
                self.log(f"Found {len(existing_examples)} existing examples in destination", "info")

        for example in self.stream_examples(source_dataset_id):
            # Debug: log example data
            if self.config.migration.verbose:
                self.log(f"Example {example['id']} has outputs: {bool(example.get('outputs'))}", "info")
                if example.get("outputs"):
                    self.log(f"Outputs content: {example.get('outputs')}", "info")

            source_inputs = example.get("inputs", {})
            inputs_hash = self._hash_inputs(source_inputs) if upsert else None

            # Check if this example already exists in destination (by inputs hash)
            if upsert and inputs_hash in existing_examples:
                existing = existing_examples[inputs_hash]
                existing_id = existing.get("id")

                # Update the existing example
                try:
                    self.update_example(existing_id, {
                        "inputs": source_inputs,
                        "outputs": example.get("outputs", {}),
                        "metadata": example.get("metadata", {}),
                    })
                    id_mapping[example["id"]] = existing_id
                    total_updated += 1
                    total_migrated += 1
                    if self.config.migration.verbose:
                        self.log(f"Updated existing example: {example['id']} -> {existing_id}", "info")
                except Exception as e:
                    self.log(f"Failed to update example {existing_id}: {e}", "error")

                if progress_callback:
                    progress_callback(total_migrated)
                continue

            # Download attachments if present (only for new examples)
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
                "inputs": source_inputs,
                "outputs": example.get("outputs", {}),
                "metadata": example.get("metadata", {}),
                "created_at": example.get("created_at"),
                "split": ((example.get("metadata") or {}).get("dataset_split") or "base")
            }

            batch.append((example["id"], migrated_example, downloaded_attachments))

            # Process batch when it reaches configured size
            if len(batch) >= self.config.migration.batch_size:
                batch_count += 1
                self.log(f"Processing batch {batch_count} ({len(batch)} new examples)")

                created = self._process_example_batch(dest_dataset_id, batch, id_mapping)
                total_created += created
                total_migrated += created

                if progress_callback:
                    progress_callback(total_migrated)

                batch.clear()

        # Process remaining examples
        if batch:
            created = self._process_example_batch(dest_dataset_id, batch, id_mapping)
            total_created += created
            total_migrated += created

            if progress_callback:
                progress_callback(total_migrated)

        self.log(f"Migration complete: {total_created} created, {total_updated} updated ({total_migrated} total)", "success")
        return id_mapping

    def _process_example_batch(
        self,
        dest_dataset_id: str,
        batch: List[Tuple[str, Dict[str, Any], Dict]],
        id_mapping: Dict[str, str]
    ) -> int:
        """
        Process a batch of examples for creation.

        Returns:
            Number of examples successfully created
        """
        created_count = 0

        # Check if any example has attachments
        has_attachments = any(len(ex[2]) > 0 for ex in batch)

        if has_attachments:
            # Use SDK for examples with attachments
            self.log("Using LangSmith SDK for examples with attachments")
            try:
                batch_id_mapping = self.create_examples_with_attachments(dest_dataset_id, batch)
                id_mapping.update(batch_id_mapping)
                created_count = len(batch_id_mapping)
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
                        created_count += 1

        return created_count

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
        existing_id = self.find_existing_dataset(dataset["name"])
        
        if existing_id:
            if self.config.migration.skip_existing:
                self.log(f"Dataset '{dataset['name']}' already exists, skipping", "warning")
                return existing_id, {}
            else:
                self.log(f"Dataset '{dataset['name']}' exists, updating...", "info")
                self.update_dataset(existing_id, dataset)
                new_dataset_id = existing_id
        else:
            # Create dataset
            new_dataset_id = self.create_dataset(dataset)
            self.log(f"Created dataset: {dataset['name']} -> {new_dataset_id}", "success")

        # Migrate examples if requested
        example_mapping = {}
        if include_examples:
            example_mapping = self.migrate_examples_streaming(dataset_id, new_dataset_id)

        return new_dataset_id, example_mapping
