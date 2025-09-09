"""Migration state management for resume capability."""

import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field, asdict
from enum import Enum


class MigrationStatus(Enum):
    """Status of a migration item."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class MigrationItem:
    """Represents an item being migrated."""
    id: str
    type: str  # dataset, experiment, queue, prompt, etc.
    name: str
    source_id: str
    destination_id: Optional[str] = None
    status: MigrationStatus = MigrationStatus.PENDING
    error: Optional[str] = None
    attempts: int = 0
    last_attempt: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "source_id": self.source_id,
            "destination_id": self.destination_id,
            "status": self.status.value,
            "error": self.error,
            "attempts": self.attempts,
            "last_attempt": self.last_attempt,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MigrationItem':
        """Create from dictionary."""
        return cls(
            id=data["id"],
            type=data["type"],
            name=data["name"],
            source_id=data["source_id"],
            destination_id=data.get("destination_id"),
            status=MigrationStatus(data["status"]),
            error=data.get("error"),
            attempts=data.get("attempts", 0),
            last_attempt=data.get("last_attempt"),
            metadata=data.get("metadata", {})
        )


@dataclass
class MigrationState:
    """Tracks the state of an entire migration session."""
    session_id: str
    started_at: float
    updated_at: float
    source_url: str
    destination_url: str
    items: Dict[str, MigrationItem] = field(default_factory=dict)
    id_mappings: Dict[str, Dict[str, str]] = field(default_factory=dict)  # type -> {source_id: dest_id}
    statistics: Dict[str, int] = field(default_factory=dict)
    
    def add_item(self, item: MigrationItem):
        """Add an item to track."""
        self.items[item.id] = item
        self.updated_at = time.time()
    
    def update_item_status(self, item_id: str, status: MigrationStatus, 
                          destination_id: Optional[str] = None, error: Optional[str] = None):
        """Update the status of an item."""
        if item_id in self.items:
            item = self.items[item_id]
            item.status = status
            item.last_attempt = time.time()
            item.attempts += 1
            
            if destination_id:
                item.destination_id = destination_id
                # Update ID mappings
                if item.type not in self.id_mappings:
                    self.id_mappings[item.type] = {}
                self.id_mappings[item.type][item.source_id] = destination_id
            
            if error:
                item.error = error
            
            self.updated_at = time.time()
    
    def get_pending_items(self, item_type: Optional[str] = None) -> List[MigrationItem]:
        """Get all pending items, optionally filtered by type."""
        items = []
        for item in self.items.values():
            if item.status == MigrationStatus.PENDING:
                if item_type is None or item.type == item_type:
                    items.append(item)
        return items
    
    def get_failed_items(self, max_attempts: int = 3) -> List[MigrationItem]:
        """Get failed items that haven't exceeded max attempts."""
        items = []
        for item in self.items.values():
            if item.status == MigrationStatus.FAILED and item.attempts < max_attempts:
                items.append(item)
        return items
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get migration statistics."""
        stats = {
            "total": len(self.items),
            "completed": 0,
            "failed": 0,
            "pending": 0,
            "in_progress": 0,
            "skipped": 0,
            "by_type": {}
        }
        
        for item in self.items.values():
            stats[item.status.value.lower()] += 1
            
            if item.type not in stats["by_type"]:
                stats["by_type"][item.type] = {
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                    "pending": 0
                }
            
            stats["by_type"][item.type]["total"] += 1
            stats["by_type"][item.type][item.status.value.lower()] += 1
        
        # Calculate completion percentage
        if stats["total"] > 0:
            stats["completion_percentage"] = (stats["completed"] / stats["total"]) * 100
        else:
            stats["completion_percentage"] = 0
        
        # Calculate elapsed time
        stats["elapsed_time"] = self.updated_at - self.started_at
        
        return stats
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "session_id": self.session_id,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "source_url": self.source_url,
            "destination_url": self.destination_url,
            "items": {k: v.to_dict() for k, v in self.items.items()},
            "id_mappings": self.id_mappings,
            "statistics": self.get_statistics()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MigrationState':
        """Create from dictionary."""
        state = cls(
            session_id=data["session_id"],
            started_at=data["started_at"],
            updated_at=data["updated_at"],
            source_url=data["source_url"],
            destination_url=data["destination_url"],
            id_mappings=data.get("id_mappings", {})
        )
        
        # Reconstruct items
        for item_id, item_data in data.get("items", {}).items():
            state.items[item_id] = MigrationItem.from_dict(item_data)
        
        return state


class StateManager:
    """Manages migration state persistence."""
    
    def __init__(self, state_dir: Optional[Path] = None):
        """
        Initialize state manager.
        
        Args:
            state_dir: Directory for state files (default: ~/.langsmith-migrator/state)
        """
        self.state_dir = state_dir or Path.home() / ".langsmith-migrator" / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.current_state: Optional[MigrationState] = None
        self.state_file: Optional[Path] = None
    
    def create_session(self, source_url: str, destination_url: str) -> MigrationState:
        """Create a new migration session."""
        session_id = f"migration_{int(time.time())}"
        self.current_state = MigrationState(
            session_id=session_id,
            started_at=time.time(),
            updated_at=time.time(),
            source_url=source_url,
            destination_url=destination_url
        )
        
        self.state_file = self.state_dir / f"{session_id}.json"
        self.save()
        
        return self.current_state
    
    def load_session(self, session_id: str) -> Optional[MigrationState]:
        """Load an existing migration session."""
        state_file = self.state_dir / f"{session_id}.json"
        
        if not state_file.exists():
            return None
        
        with open(state_file, 'r') as f:
            data = json.load(f)
        
        self.current_state = MigrationState.from_dict(data)
        self.state_file = state_file
        
        return self.current_state
    
    def list_sessions(self) -> List[Dict[str, Any]]:
        """List all available migration sessions."""
        sessions = []
        
        for state_file in self.state_dir.glob("migration_*.json"):
            try:
                with open(state_file, 'r') as f:
                    data = json.load(f)
                
                sessions.append({
                    "session_id": data["session_id"],
                    "started_at": data["started_at"],
                    "updated_at": data["updated_at"],
                    "source_url": data["source_url"],
                    "destination_url": data["destination_url"],
                    "statistics": data.get("statistics", {})
                })
            except Exception:
                continue
        
        # Sort by updated_at descending
        sessions.sort(key=lambda x: x["updated_at"], reverse=True)
        
        return sessions
    
    def save(self):
        """Save current state to disk."""
        if not self.current_state or not self.state_file:
            return
        
        with open(self.state_file, 'w') as f:
            json.dump(self.current_state.to_dict(), f, indent=2)
    
    def delete_session(self, session_id: str) -> bool:
        """Delete a migration session."""
        state_file = self.state_dir / f"{session_id}.json"
        
        if state_file.exists():
            state_file.unlink()
            return True
        
        return False
    
    def get_resume_info(self, state: MigrationState) -> Dict[str, Any]:
        """Get information about what can be resumed."""
        stats = state.get_statistics()
        
        return {
            "session_id": state.session_id,
            "total_items": stats["total"],
            "completed": stats["completed"],
            "failed": stats["failed"],
            "pending": stats["pending"],
            "can_resume": stats["pending"] > 0 or stats["failed"] > 0,
            "elapsed_time": stats["elapsed_time"],
            "by_type": stats["by_type"]
        }