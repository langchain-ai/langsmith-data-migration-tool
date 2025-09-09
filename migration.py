import requests
from langsmith import Client
from typing import Dict, Literal, Optional, List, Any
import json
import os
from dotenv import load_dotenv
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.panel import Panel
import inquirer


class MigrationError(Exception):
    """Custom exception for migration errors"""
    pass


class APIClient:
    """Wrapper for HTTP requests with consistent error handling"""
    
    def __init__(self, base_url: str, headers: Dict[str, str], verify_ssl: bool = True):
        self.base_url = base_url
        self.headers = headers
        self.verify_ssl = verify_ssl
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make GET request and return JSON response"""
        response = requests.get(
            f"{self.base_url}{endpoint}",
            headers=self.headers,
            params=params,
            verify=self.verify_ssl
        )
        if not response.ok:
            raise MigrationError(f"GET {endpoint} failed: {response.status_code} - {response.text}")
        return response.json()
    
    def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make POST request and return JSON response"""
        response = requests.post(
            f"{self.base_url}{endpoint}",
            headers=self.headers,
            json=data,
            verify=self.verify_ssl
        )
        if not response.ok:
            raise MigrationError(f"POST {endpoint} failed: {response.status_code} - {response.text}")
        return response.json()


class DatasetMigrator:
    """Handles dataset-specific migration logic"""
    
    def __init__(self, old_client: APIClient, new_client: APIClient):
        self.old_client = old_client
        self.new_client = new_client
    
    def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """Fetch dataset by ID"""
        dataset = self.old_client.get(f"/datasets/{dataset_id}")
        if 'name' not in dataset:
            available_keys = list(dataset.keys())
            raise MigrationError(f"Dataset response missing 'name' field. Available keys: {available_keys}")
        return dataset
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """List all datasets from old client"""
        response = self.old_client.get("/datasets")
        if "detail" in response:
            return []
        return response
    
    def find_existing_dataset(self, name: str) -> Optional[str]:
        """Find existing dataset by name, return ID if found"""
        response = self.new_client.get("/datasets", params={"name": name})
        
        if "detail" in response:
            return None
            
        datasets = response
        if len(datasets) > 1:
            raise MigrationError(f"Found multiple datasets with name {name} in new instance")
        
        return datasets[0]["id"] if datasets else None
    
    def create_dataset(self, original_dataset: Dict[str, Any]) -> str:
        """Create new dataset from original dataset data"""
        payload = {
            "name": original_dataset["name"],
            "description": original_dataset["description"],
            "created_at": original_dataset["created_at"],
            "inputs_schema_definition": original_dataset["inputs_schema_definition"],
            "outputs_schema_definition": original_dataset["outputs_schema_definition"],
            "externally_managed": original_dataset["externally_managed"],
            "transformations": original_dataset["transformations"] or [],
            "data_type": original_dataset["data_type"],
        }
        
        response = self.new_client.post("/datasets", payload)
        if 'id' not in response:
            raise MigrationError("Dataset creation response missing 'id' field")
        
        return response['id']
    
    def fetch_all_examples(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Fetch all examples from a dataset with pagination"""
        examples = []
        offset = 0
        max_iterations = 10000
        
        for _ in range(max_iterations):
            batch = self.old_client.get("/examples", params={"dataset": dataset_id, "offset": offset})
            
            if not batch:
                break
                
            examples.extend(batch)
            offset = len(examples)
            
            # Stop if we got fewer than 100 (end of data)
            if len(batch) < 100:
                break
        
        return examples
    
    def create_examples(self, examples: List[Dict[str, Any]], new_dataset_id: str) -> List[Dict[str, Any]]:
        """Create examples in bulk for new dataset"""
        payload = [
            {
                "dataset_id": new_dataset_id,
                "inputs": example["inputs"],
                "outputs": example["outputs"],
                "metadata": example["metadata"],
                "created_at": example["created_at"],
                "split": (example["metadata"] or {}).get("dataset_split", "base"),
            }
            for example in examples
        ]
        
        response = self.new_client.post("/examples/bulk", payload)
        if not isinstance(response, list):
            raise MigrationError("Bulk examples response should be a list")
        
        return response
    
    def migrate_examples(self, original_dataset_id: str, new_dataset_id: str) -> Dict[str, str]:
        """Migrate all examples and return ID mapping"""
        original_examples = self.fetch_all_examples(original_dataset_id)
        new_examples = self.create_examples(original_examples, new_dataset_id)
        
        return {
            original_examples[i]["id"]: new_examples[i]["id"]
            for i in range(len(new_examples))
        }


class ExperimentMigrator:
    """Handles experiment migration logic"""
    
    def __init__(self, old_client: APIClient, new_client: APIClient):
        self.old_client = old_client
        self.new_client = new_client
    
    def fetch_experiments(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Fetch all experiments for a dataset"""
        experiments = []
        offset = 0
        
        while True:
            batch = self.old_client.get("/sessions", params={
                "reference_dataset": dataset_id,
                "offset": offset
            })
            
            if len(batch) < 100:
                experiments.extend(batch)
                break
                
            experiments.extend(batch)
            offset = len(experiments)
        
        return experiments
    
    def create_experiment(self, experiment: Dict[str, Any], new_dataset_id: str) -> str:
        """Create single experiment"""
        payload = {
            "name": experiment["name"],
            "description": experiment["description"],
            "reference_dataset_id": new_dataset_id,
            "default_dataset_id": experiment["default_dataset_id"],
            "start_time": experiment["start_time"],
            "end_time": experiment["end_time"],
            "extra": experiment["extra"],
            "trace_tier": experiment.get("trace_tier"),
        }
        
        response = self.new_client.post("/sessions", payload)
        return response["id"]
    
    def migrate_experiment_runs(self, experiment_ids: List[str], id_mappings: Dict[str, Dict[str, str]]):
        """Migrate all runs for experiments"""
        if not experiment_ids:
            return
            
        total_runs = 0
        payload = {
            "session": experiment_ids,
            "skip_pagination": False,
        }
        
        while True:
            response = self.old_client.post("/runs/query", payload)
            runs = response.get("runs", [])
            
            if runs:
                self._create_runs_batch(runs, id_mappings)
                total_runs += len(runs)
            
            next_cursor = response.get("cursors", {}).get("next")
            if not next_cursor:
                break
                
            payload["cursor"] = next_cursor
        
        return total_runs
    
    def _create_runs_batch(self, runs: List[Dict[str, Any]], id_mappings: Dict[str, Dict[str, str]]):
        """Create a batch of runs"""
        if not runs:
            return
            
        experiment_mapping = id_mappings["experiments"]
        example_mapping = id_mappings["examples"]
        
        runs_to_create = []
        for run in runs:
            # Skip runs that don't have a mapped session_id
            if run.get("session_id") not in experiment_mapping:
                continue
                
            run_data = {
                "name": run["name"],
                "inputs": run["inputs"],
                "run_type": run["run_type"],
                "start_time": run["start_time"],
                "end_time": run["end_time"],
                "extra": run.get("extra"),
                "error": run.get("error"),
                "serialized": run.get("serialized", {}),
                "outputs": run.get("outputs"),
                "parent_run_id": run.get("parent_run_id"),
                "events": run.get("events", []),
                "tags": run.get("tags", []),
                "trace_id": run["trace_id"],
                "id": run["id"],
                "dotted_order": run.get("dotted_order"),
                "session_id": experiment_mapping[run["session_id"]],
                "session_name": run.get("session_name"),
                "reference_example_id": example_mapping.get(run.get("reference_example_id")),
                "input_attachments": run.get("input_attachments", {}),
                "output_attachments": run.get("output_attachments", {})
            }
            runs_to_create.append(run_data)
        
        if runs_to_create:
            runs_payload = {"post": runs_to_create}
            self.new_client.post("/runs/batch", runs_payload)


class AnnotationQueueMigrator:
    """Handles annotation queue migration"""
    
    def __init__(self, old_client: APIClient, new_client: APIClient, dataset_migrator):
        self.old_client = old_client
        self.new_client = new_client
        self.dataset_migrator = dataset_migrator
    
    def get_queue(self, queue_id: str) -> Dict[str, Any]:
        """Fetch annotation queue by ID"""
        return self.old_client.get(f"/annotation_queues/{queue_id}")
    
    def list_queues(self) -> List[Dict[str, Any]]:
        """List all annotation queues from old client"""
        response = self.old_client.get("/annotation_queues")
        if "detail" in response:
            return []
        return response
    
    def find_existing_queue(self, name: str) -> Optional[str]:
        """Find existing annotation queue by name"""
        response = self.new_client.get("/annotation_queues", params={"name": name})
        
        if "detail" in response:
            return None
            
        queues = response
        if len(queues) > 1:
            raise MigrationError(f"Found multiple annotation queues with name {name} in new instance")
        
        return queues[0]["id"] if queues else None
    
    def create_queue(self, original_queue: Dict[str, Any], default_dataset_id: Optional[str] = None) -> str:
        """Create new annotation queue"""
        payload = {
            "name": original_queue["name"],
            "description": original_queue["description"],
            "created_at": original_queue["created_at"],
            "updated_at": original_queue["updated_at"],
            "default_dataset": default_dataset_id,
            "num_reviewers_per_item": original_queue["num_reviewers_per_item"],
            "enable_reservations": original_queue["enable_reservations"],
            "reservation_minutes": original_queue["reservation_minutes"],
            "rubric_items": original_queue["rubric_items"],
            "rubric_instructions": original_queue["rubric_instructions"],
            "session_ids": []
        }
        
        response = self.new_client.post("/annotation_queues", payload)
        return response["id"]


class LangsmithMigrator:
    """Main migrator class that orchestrates all migration operations"""
    
    def __init__(self, old_api_key: str, new_api_key: str, 
                 old_base_url: str = "https://api.smith.langchain.com", 
                 new_base_url: str = "https://api.smith.langchain.com", 
                 verify_ssl: bool = True):
        
        if not verify_ssl:
            urllib3.disable_warnings(InsecureRequestWarning)
        
        # Clean and prepare base URLs
        old_clean = self._prepare_base_url(old_base_url)
        new_clean = self._prepare_base_url(new_base_url)
        
        # Initialize API clients
        self.old_client = APIClient(old_clean, {"X-API-Key": old_api_key}, verify_ssl)
        self.new_client = APIClient(new_clean, {"X-API-Key": new_api_key}, verify_ssl)
        
        # Initialize LangSmith clients for prompts
        self.old_langsmith = Client(api_key=old_api_key, api_url=old_base_url)
        self.new_langsmith = Client(api_key=new_api_key, api_url=new_base_url)
        
        # Initialize specialized migrators
        self.dataset_migrator = DatasetMigrator(self.old_client, self.new_client)
        self.experiment_migrator = ExperimentMigrator(self.old_client, self.new_client)
        self.queue_migrator = AnnotationQueueMigrator(self.old_client, self.new_client, self.dataset_migrator)
    
    def _prepare_base_url(self, base_url: str) -> str:
        """Clean and prepare base URL"""
        clean_url = base_url.rstrip('/')
        return f"{clean_url}/api/v1" if not clean_url.endswith('/api/v1') else clean_url
    
    def migrate_dataset(self, 
                       original_dataset_id: str, 
                       check_if_already_exists: bool = True,
                       migration_mode: Literal["EXAMPLES", "EXAMPLES_AND_EXPERIMENTS", "DATASET_ONLY"] = "EXAMPLES") -> str:
        """Migrate a dataset and its content"""
        
        # Get original dataset
        original_dataset = self.dataset_migrator.get_dataset(original_dataset_id)
        
        # Check if already exists
        if check_if_already_exists:
            existing_id = self.dataset_migrator.find_existing_dataset(original_dataset['name'])
            if existing_id:
                return existing_id
        
        # Create new dataset
        new_dataset_id = self.dataset_migrator.create_dataset(original_dataset)
        
        # Handle migration modes
        migration_handlers = {
            "EXAMPLES": self._migrate_examples_only,
            "EXAMPLES_AND_EXPERIMENTS": self._migrate_examples_and_experiments,
            "DATASET_ONLY": lambda *args: None
        }
        
        handler = migration_handlers[migration_mode]
        handler(original_dataset_id, new_dataset_id)
        
        return new_dataset_id
    
    def _migrate_examples_only(self, original_dataset_id: str, new_dataset_id: str):
        """Migrate only examples"""
        self.dataset_migrator.migrate_examples(original_dataset_id, new_dataset_id)
    
    def _migrate_examples_and_experiments(self, original_dataset_id: str, new_dataset_id: str):
        """Migrate examples and experiments"""
        example_mapping = self.dataset_migrator.migrate_examples(original_dataset_id, new_dataset_id)
        
        # Migrate experiments
        experiments = self.experiment_migrator.fetch_experiments(original_dataset_id)
        
        if not experiments:
            return
            
        experiment_mapping = {}
        
        for experiment in experiments:
            new_exp_id = self.experiment_migrator.create_experiment(experiment, new_dataset_id)
            experiment_mapping[experiment["id"]] = new_exp_id
        
        # Migrate runs
        id_mappings = {
            "experiments": experiment_mapping,
            "examples": example_mapping
        }
        
        old_experiment_ids = [exp["id"] for exp in experiments]
        runs_count = self.experiment_migrator.migrate_experiment_runs(old_experiment_ids, id_mappings)
        
        # Return info about what was migrated
        return {
            "experiments": len(experiments),
            "runs": runs_count if runs_count else 0
        }
    
    def migrate_annotation_queue(self,
                                old_annotation_queue_id: str,
                                check_if_already_exists: bool = True,
                                migration_mode: Literal["QUEUE_AND_DATASET", "QUEUE_ONLY"] = "QUEUE_AND_DATASET") -> str:
        """Migrate an annotation queue"""
        
        # Get original queue
        original_queue = self.queue_migrator.get_queue(old_annotation_queue_id)
        
        # Check if already exists
        if check_if_already_exists:
            existing_id = self.queue_migrator.find_existing_queue(original_queue['name'])
            if existing_id:
                return existing_id
        
        # Handle dataset migration
        default_dataset_id = None
        should_migrate_dataset = (migration_mode == "QUEUE_AND_DATASET" and 
                                original_queue.get("default_dataset"))
        
        if should_migrate_dataset:
            default_dataset_id = self.migrate_dataset(
                original_queue["default_dataset"],
                check_if_already_exists=True,
                migration_mode="EXAMPLES"
            )
        
        # Create new queue
        return self.queue_migrator.create_queue(original_queue, default_dataset_id)
    
    def migrate_project_rules(self, old_project_id: str, new_project_id: str):
        """Migrate all rules from a tracing project"""
        
        # Get original rules
        old_rules = self.old_client.get("/runs/rules", params={"session_id": old_project_id})
        
        for old_rule in old_rules:
            if old_rule["dataset_id"] is not None:
                continue
            
            # Migrate dependencies
            add_to_dataset_id = self._migrate_rule_dataset(old_rule)
            add_to_annotation_queue_id = self._migrate_rule_annotation_queue(old_rule)
            
            # Create new rule
            self._create_project_rule(old_rule, new_project_id, add_to_dataset_id, add_to_annotation_queue_id)
    
    def _migrate_rule_dataset(self, old_rule: Dict[str, Any]) -> Optional[str]:
        """Migrate dataset for a rule if needed"""
        if not old_rule.get("add_to_dataset_id"):
            return None
        
        return self.migrate_dataset(
            old_rule["add_to_dataset_id"],
            check_if_already_exists=True,
            migration_mode="EXAMPLES"
        )
    
    def _migrate_rule_annotation_queue(self, old_rule: Dict[str, Any]) -> Optional[str]:
        """Migrate annotation queue for a rule if needed"""
        if not old_rule.get("add_to_annotation_queue_id"):
            return None
        
        return self.migrate_annotation_queue(
            old_rule["add_to_annotation_queue_id"],
            check_if_already_exists=True,
            migration_mode="QUEUE_AND_DATASET"
        )
    
    def _create_project_rule(self, old_rule: Dict[str, Any], new_project_id: str,
                           add_to_dataset_id: Optional[str], add_to_annotation_queue_id: Optional[str]):
        """Create a new project rule"""
        payload = {
            "display_name": old_rule["display_name"],
            "session_id": new_project_id,
            "is_enabled": old_rule["is_enabled"],
            "dataset_id": None,
            "sampling_rate": old_rule["sampling_rate"],
            "filter": old_rule["filter"],
            "trace_filter": old_rule["trace_filter"],
            "tree_filter": old_rule["tree_filter"],
            "add_to_annotation_queue_id": add_to_annotation_queue_id,
            "add_to_dataset_id": add_to_dataset_id,
            "add_to_dataset_prefer_correction": old_rule["add_to_dataset_prefer_correction"],
            "use_corrections_dataset": old_rule["use_corrections_dataset"],
            "num_few_shot_examples": old_rule["num_few_shot_examples"],
            "extend_only": old_rule["extend_only"],
            "transient": old_rule["transient"],
            "backfill_from": old_rule["backfill_from"],
            "evaluators": old_rule["evaluators"],
            "code_evaluators": old_rule["code_evaluators"],
            "alerts": old_rule["alerts"],
            "webhooks": old_rule["webhooks"]
        }
        
        self.new_client.post("/runs/rules", payload)
    
    def list_prompts(self) -> List[Dict[str, Any]]:
        """List all prompts from old client"""
        response = self.old_client.get("/prompts")
        if "detail" in response:
            return []
        return response
    
    def migrate_prompt(self, original_prompt_id: str):
        """Migrate a prompt from original instance to new instance"""
        prompt_object = self.old_langsmith.pull_prompt_commit(
            original_prompt_id, include_model=True
        )
        self.new_langsmith.push_prompt(
            prompt_identifier=original_prompt_id, 
            object=prompt_object.manifest
        )


def validate_environment() -> tuple[str, str, str, str, bool]:
    """Validate and return environment configuration"""
    console = Console()
    
    old_api_key = os.getenv('LANGSMITH_OLD_API_KEY')
    new_api_key = os.getenv('LANGSMITH_NEW_API_KEY')
    old_base_url = os.getenv('LANGSMITH_OLD_BASE_URL', 'https://api.smith.langchain.com')
    new_base_url = os.getenv('LANGSMITH_NEW_BASE_URL', 'https://api.smith.langchain.com')
    verify_ssl = os.getenv('LANGSMITH_VERIFY_SSL', 'true').lower() != 'false'
    
    if not old_api_key or not new_api_key:
        console.print("[red]❌ Missing required environment variables[/red]")
        console.print("Please set the following environment variables:")
        console.print("  • LANGSMITH_OLD_API_KEY - API key for source LangSmith instance")
        console.print("  • LANGSMITH_NEW_API_KEY - API key for destination LangSmith instance")
        console.print("\nOptional environment variables:")
        console.print("  • LANGSMITH_OLD_BASE_URL - Base URL for source instance (default: https://api.smith.langchain.com)")
        console.print("  • LANGSMITH_NEW_BASE_URL - Base URL for destination instance (default: https://api.smith.langchain.com)")
        console.print("  • LANGSMITH_VERIFY_SSL - Set to 'false' to disable SSL verification (default: true)")
        raise SystemExit(1)
    
    return old_api_key, new_api_key, old_base_url, new_base_url, verify_ssl


def display_datasets(datasets: List[Dict[str, Any]], console: Console = None) -> None:
    """Display datasets in a nice table"""
    if console is None:
        console = Console()
    
    if not datasets:
        console.print("[yellow]No datasets found[/yellow]")
        return
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Name", style="cyan")
    table.add_column("ID", style="dim")
    table.add_column("Description", style="green")
    table.add_column("Examples", justify="right", style="blue")
    
    for dataset in datasets:
        name = dataset.get('name', 'Unknown')
        dataset_id = dataset.get('id', 'Unknown')
        description = dataset.get('description', '')[:50] + ('...' if len(dataset.get('description', '')) > 50 else '')
        example_count = str(dataset.get('example_count', 'Unknown'))
        
        table.add_row(name, dataset_id, description, example_count)
    
    console.print(table)


def display_queues(queues: List[Dict[str, Any]], console: Console = None) -> None:
    """Display annotation queues in a nice table"""
    if console is None:
        console = Console()
    
    if not queues:
        console.print("[yellow]No annotation queues found[/yellow]")
        return
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Name", style="cyan")
    table.add_column("ID", style="dim")
    table.add_column("Description", style="green")
    
    for queue in queues:
        name = queue.get('name', 'Unknown')
        queue_id = queue.get('id', 'Unknown')
        description = queue.get('description', '')[:50] + ('...' if len(queue.get('description', '')) > 50 else '')
        
        table.add_row(name, queue_id, description)
    
    console.print(table)


def display_prompts(prompts: List[Dict[str, Any]], console: Console = None) -> None:
    """Display prompts in a nice table"""
    if console is None:
        console = Console()
    
    if not prompts:
        console.print("[yellow]No prompts found[/yellow]")
        return
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Name", style="cyan")
    table.add_column("ID", style="dim")
    
    for prompt in prompts:
        name = prompt.get('name', 'Unknown')
        prompt_id = prompt.get('id', 'Unknown')
        
        table.add_row(name, prompt_id)
    
    console.print(table)


def select_datasets(migrator: 'LangsmithMigrator') -> List[str]:
    """Let user select datasets to migrate"""
    console = Console()
    
    with console.status("[bold green]Fetching datasets..."):
        datasets = migrator.dataset_migrator.list_datasets()
    
    if not datasets:
        console.print("[yellow]No datasets found in source instance[/yellow]")
        return []
    
    console.print("\n[bold]Available datasets:[/bold]")
    display_datasets(datasets, console)
    
    choices = [f"{d['name']} ({d['id']})" for d in datasets]
    questions = [
        inquirer.Checkbox(
            'datasets',
            message="Select datasets to migrate (use SPACE to select, ENTER to confirm)",
            choices=choices,
        ),
    ]
    
    answers = inquirer.prompt(questions)
    if not answers or not answers['datasets']:
        return []
    
    # Extract IDs from selected choices
    selected_ids = []
    for choice in answers['datasets']:
        dataset_id = choice.split('(')[1].rstrip(')')
        selected_ids.append(dataset_id)
    
    return selected_ids


def select_queues(migrator: 'LangsmithMigrator') -> List[str]:
    """Let user select annotation queues to migrate"""
    console = Console()
    
    with console.status("[bold green]Fetching annotation queues..."):
        queues = migrator.queue_migrator.list_queues()
    
    if not queues:
        console.print("[yellow]No annotation queues found in source instance[/yellow]")
        return []
    
    console.print("\n[bold]Available annotation queues:[/bold]")
    display_queues(queues, console)
    
    choices = [f"{q['name']} ({q['id']})" for q in queues]
    questions = [
        inquirer.Checkbox(
            'queues',
            message="Select annotation queues to migrate (use SPACE to select, ENTER to confirm)",
            choices=choices,
        ),
    ]
    
    answers = inquirer.prompt(questions)
    if not answers or not answers['queues']:
        return []
    
    # Extract IDs from selected choices
    selected_ids = []
    for choice in answers['queues']:
        queue_id = choice.split('(')[1].rstrip(')')
        selected_ids.append(queue_id)
    
    return selected_ids


def _migrate_single_dataset(migrator: 'LangsmithMigrator', dataset_id: str, migration_mode: str, console: Console) -> bool:
    """Migrate a single dataset and return success status"""
    try:
        result_id = migrator.migrate_dataset(dataset_id, True, migration_mode)
        
        # Get dataset name for better feedback
        dataset_info = migrator.dataset_migrator.get_dataset(dataset_id)
        dataset_name = dataset_info.get('name', dataset_id)
        
        if migration_mode == 'EXAMPLES_AND_EXPERIMENTS':
            console.print(f"[green]✓[/green] Dataset '{dataset_name}' migrated with examples and experiments. New ID: {result_id}")
        elif migration_mode == 'EXAMPLES':
            console.print(f"[green]✓[/green] Dataset '{dataset_name}' migrated with examples. New ID: {result_id}")
        else:
            console.print(f"[green]✓[/green] Dataset '{dataset_name}' metadata migrated. New ID: {result_id}")
        return True
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to migrate dataset {dataset_id}: {str(e)}")
        return False


def migrate_datasets_interactive(migrator: 'LangsmithMigrator') -> None:
    """Interactive dataset migration"""
    console = Console()
    
    dataset_ids = select_datasets(migrator)
    if not dataset_ids:
        console.print("[yellow]No datasets selected for migration[/yellow]")
        return
    
    # Ask for migration mode
    mode_question = [
        inquirer.List(
            'mode',
            message="Select migration mode",
            choices=[
                ('Examples only', 'EXAMPLES'),
                ('Examples and experiments', 'EXAMPLES_AND_EXPERIMENTS'),
                ('Dataset metadata only', 'DATASET_ONLY'),
            ],
        ),
    ]
    mode_answer = inquirer.prompt(mode_question)
    if not mode_answer:
        migration_mode = 'EXAMPLES'
    else:
        migration_mode = mode_answer['mode']
    
    # Ask for confirmation
    if not Confirm.ask(f"Migrate {len(dataset_ids)} dataset(s) with mode '{migration_mode}'?"):
        console.print("[yellow]Migration cancelled[/yellow]")
        return
    
    # Migrate datasets with progress bar
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Migrating datasets...", total=len(dataset_ids))
        
        for dataset_id in dataset_ids:
            progress.update(task, description=f"Migrating dataset {dataset_id}...")
            _migrate_single_dataset(migrator, dataset_id, migration_mode, console)
            progress.advance(task)


def _migrate_single_queue(migrator: 'LangsmithMigrator', queue_id: str, migration_mode: str, console: Console) -> bool:
    """Migrate a single annotation queue and return success status"""
    result_id = migrator.migrate_annotation_queue(queue_id, True, migration_mode)
    console.print(f"[green]✓[/green] Annotation queue {queue_id} migrated successfully. New ID: {result_id}")
    return True


def migrate_queues_interactive(migrator: 'LangsmithMigrator') -> None:
    """Interactive annotation queue migration"""
    console = Console()
    
    queue_ids = select_queues(migrator)
    if not queue_ids:
        console.print("[yellow]No annotation queues selected for migration[/yellow]")
        return
    
    # Ask for migration mode
    mode_question = [
        inquirer.List(
            'mode',
            message="Select migration mode",
            choices=[
                ('Queue and associated dataset', 'QUEUE_AND_DATASET'),
                ('Queue only', 'QUEUE_ONLY'),
            ],
        ),
    ]
    mode_answer = inquirer.prompt(mode_question)
    if not mode_answer:
        migration_mode = 'QUEUE_AND_DATASET'
    else:
        migration_mode = mode_answer['mode']
    
    # Ask for confirmation
    if not Confirm.ask(f"Migrate {len(queue_ids)} annotation queue(s) with mode '{migration_mode}'?"):
        console.print("[yellow]Migration cancelled[/yellow]")
        return
    
    # Migrate queues with progress bar
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Migrating annotation queues...", total=len(queue_ids))
        
        for queue_id in queue_ids:
            progress.update(task, description=f"Migrating queue {queue_id}...")
            _migrate_single_queue(migrator, queue_id, migration_mode, console)
            progress.advance(task)


def migrate_project_rules_interactive(migrator: 'LangsmithMigrator') -> None:
    """Interactive project rules migration"""
    console = Console()
    
    console.print("\n[bold]Project Rules Migration[/bold]")
    console.print("You need to provide the project IDs for source and destination projects.")
    
    old_project_id = Prompt.ask("Enter source project ID")
    new_project_id = Prompt.ask("Enter destination project ID")
    
    if not old_project_id or not new_project_id:
        console.print("[yellow]Migration cancelled - missing project IDs[/yellow]")
        return
    
    if not Confirm.ask(f"Migrate rules from project {old_project_id} to {new_project_id}?"):
        console.print("[yellow]Migration cancelled[/yellow]")
        return
    
    with console.status("[bold green]Migrating project rules..."):
        migrator.migrate_project_rules(old_project_id, new_project_id)
    console.print(f"[green]✓[/green] Project rules migrated successfully")


def _migrate_single_prompt(migrator: 'LangsmithMigrator', prompt_id: str, console: Console) -> bool:
    """Migrate a single prompt and return success status"""
    migrator.migrate_prompt(prompt_id)
    console.print(f"[green]✓[/green] Prompt {prompt_id} migrated successfully")
    return True


def migrate_prompts_interactive(migrator: 'LangsmithMigrator') -> None:
    """Interactive prompt migration"""
    console = Console()
    
    with console.status("[bold green]Fetching prompts..."):
        prompts = migrator.list_prompts()
    
    if not prompts:
        console.print("[yellow]No prompts found in source instance[/yellow]")
        return
    
    console.print("\n[bold]Available prompts:[/bold]")
    display_prompts(prompts, console)
    
    choices = [f"{p['name']} ({p['id']})" for p in prompts]
    questions = [
        inquirer.Checkbox(
            'prompts',
            message="Select prompts to migrate (use SPACE to select, ENTER to confirm)",
            choices=choices,
        ),
    ]
    
    answers = inquirer.prompt(questions)
    if not answers or not answers['prompts']:
        console.print("[yellow]No prompts selected for migration[/yellow]")
        return
    
    # Extract IDs from selected choices
    selected_ids = []
    for choice in answers['prompts']:
        prompt_id = choice.split('(')[1].rstrip(')')
        selected_ids.append(prompt_id)
    
    if not Confirm.ask(f"Migrate {len(selected_ids)} prompt(s)?"):
        console.print("[yellow]Migration cancelled[/yellow]")
        return
    
    # Migrate prompts with progress bar
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Migrating prompts...", total=len(selected_ids))
        
        for prompt_id in selected_ids:
            progress.update(task, description=f"Migrating prompt {prompt_id}...")
            _migrate_single_prompt(migrator, prompt_id, console)
            progress.advance(task)


def migrate_all_interactive(migrator: 'LangsmithMigrator') -> None:
    """Migrate all available data from source to destination"""
    console = Console()
    
    console.print("\n[bold yellow]⚠️  This will migrate ALL available data:[/bold yellow]")
    console.print("  • All datasets (with examples and experiments)")
    console.print("  • All annotation queues")
    console.print("  • All prompts")
    console.print("  • Project rules require manual selection\n")
    
    if not Confirm.ask("[bold]Are you sure you want to migrate ALL data?[/bold]"):
        console.print("[yellow]Migration cancelled[/yellow]")
        return
    
    # Track overall progress
    total_items = 0
    migrated_items = 0
    
    # Migrate all datasets
    console.print("\n[bold cyan]📊 Migrating all datasets...[/bold cyan]")
    try:
        datasets = migrator.dataset_migrator.list_datasets()
        if datasets:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Migrating datasets...", total=len(datasets))
                
                for dataset in datasets:
                    dataset_id = dataset['id']
                    progress.update(task, description=f"Migrating dataset: {dataset['name']}...")
                    try:
                        result_id = migrator.migrate_dataset(dataset_id, True, 'EXAMPLES_AND_EXPERIMENTS')
                        console.print(f"  [green]✓[/green] Dataset '{dataset['name']}' migrated successfully")
                        migrated_items += 1
                    except Exception as e:
                        console.print(f"  [red]✗[/red] Failed to migrate dataset '{dataset['name']}': {str(e)}")
                    progress.advance(task)
                    total_items += 1
        else:
            console.print("  [yellow]No datasets found[/yellow]")
    except Exception as e:
        console.print(f"  [red]Error listing datasets: {str(e)}[/red]")
    
    # Migrate all annotation queues
    console.print("\n[bold cyan]📝 Migrating all annotation queues...[/bold cyan]")
    try:
        queues = migrator.queue_migrator.list_queues()
        if queues:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Migrating queues...", total=len(queues))
                
                for queue in queues:
                    queue_id = queue['id']
                    progress.update(task, description=f"Migrating queue: {queue['name']}...")
                    try:
                        result_id = migrator.migrate_annotation_queue(queue_id, True, 'QUEUE_AND_DATASET')
                        console.print(f"  [green]✓[/green] Queue '{queue['name']}' migrated successfully")
                        migrated_items += 1
                    except Exception as e:
                        console.print(f"  [red]✗[/red] Failed to migrate queue '{queue['name']}': {str(e)}")
                    progress.advance(task)
                    total_items += 1
        else:
            console.print("  [yellow]No annotation queues found[/yellow]")
    except Exception as e:
        console.print(f"  [red]Error listing queues: {str(e)}[/red]")
    
    # Migrate all prompts
    console.print("\n[bold cyan]💬 Migrating all prompts...[/bold cyan]")
    try:
        prompts = migrator.list_prompts()
        if prompts:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Migrating prompts...", total=len(prompts))
                
                for prompt in prompts:
                    prompt_id = prompt['id']
                    progress.update(task, description=f"Migrating prompt: {prompt['name']}...")
                    try:
                        new_prompt_id = migrator.migrate_prompt(prompt_id)
                        console.print(f"  [green]✓[/green] Prompt '{prompt['name']}' migrated successfully")
                        migrated_items += 1
                    except Exception as e:
                        console.print(f"  [red]✗[/red] Failed to migrate prompt '{prompt['name']}': {str(e)}")
                    progress.advance(task)
                    total_items += 1
        else:
            console.print("  [yellow]No prompts found[/yellow]")
    except Exception as e:
        console.print(f"  [red]Error listing prompts: {str(e)}[/red]")
    
    # Summary
    console.print("\n" + "="*50)
    console.print(f"[bold green]Migration Complete![/bold green]")
    console.print(f"Successfully migrated {migrated_items} out of {total_items} items")
    
    if total_items > migrated_items:
        console.print(f"[yellow]Note: {total_items - migrated_items} items failed or were skipped (may already exist)[/yellow]")
    
    console.print("\n[yellow]Note: Project rules require manual migration as they need project ID mapping[/yellow]")


def _show_welcome_banner(console: Console) -> None:
    """Display welcome banner"""
    console.print(Panel.fit(
        "[bold blue]🚀 LangSmith Migration Tool[/bold blue]\n"
        "Migrate datasets, experiments, annotation queues, and prompts\n"
        "between LangSmith instances",
        border_style="blue"
    ))


def _show_configuration(console: Console, old_base_url: str, new_base_url: str, verify_ssl: bool) -> None:
    """Display configuration information"""
    console.print(f"\n[bold]Configuration:[/bold]")
    console.print(f"  Source: {old_base_url}")
    console.print(f"  Destination: {new_base_url}")
    console.print(f"  SSL Verification: {'Enabled' if verify_ssl else 'Disabled'}")


def _run_main_loop(migrator: 'LangsmithMigrator', console: Console) -> None:
    """Run the main interactive loop"""
    while True:
        console.print("\n[bold]What would you like to migrate?[/bold]")
        
        main_menu = [
            inquirer.List(
                'action',
                message="Select an option",
                choices=[
                    ('🚀 Migrate ALL (Datasets, Queues, Rules, Prompts)', 'all'),
                    ('📊 Datasets (with examples/experiments)', 'datasets'),
                    ('📝 Annotation Queues', 'queues'),
                    ('🔧 Project Rules', 'rules'),
                    ('💬 Prompts', 'prompts'),
                    ('❌ Exit', 'exit'),
                ],
            ),
        ]
        
        answer = inquirer.prompt(main_menu)
        if not answer:
            break
            
        action = answer['action']
        
        if action == 'exit':
            break
        
        action_handlers = {
            'all': migrate_all_interactive,
            'datasets': migrate_datasets_interactive,
            'queues': migrate_queues_interactive,
            'rules': migrate_project_rules_interactive,
            'prompts': migrate_prompts_interactive,
        }
        
        handler = action_handlers.get(action)
        if handler:
            handler(migrator)


def main():
    load_dotenv()
    console = Console()
    
    _show_welcome_banner(console)
    
    # Validate environment
    old_api_key, new_api_key, old_base_url, new_base_url, verify_ssl = validate_environment()
    
    _show_configuration(console, old_base_url, new_base_url, verify_ssl)
    
    # Initialize migrator
    with console.status("[bold green]Initializing migrator..."):
        migrator = LangsmithMigrator(old_api_key, new_api_key, old_base_url, new_base_url, verify_ssl)
    
    _run_main_loop(migrator, console)
    
    console.print("\n[bold green]👋 Thanks for using LangSmith Migration Tool![/bold green]")
    return 0


if __name__ == '__main__':
    exit(main())