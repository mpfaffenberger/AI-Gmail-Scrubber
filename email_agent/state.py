"""State persistence for tracking processed emails.

Manages JSON-based state storage for processed emails and their decisions.
Provides atomic operations and proper error handling.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class StateManager:
    """Manages persistent state for email processing.

    Stores processed emails and their decisions in JSON format.
    Provides atomic operations and proper error handling.

    Example:
        state = StateManager(Path('~/.email_agent/state.json'))
        state.mark_processed('email_id', {'action': 'archive'})
        is_processed = state.is_processed('email_id')
    """

    def __init__(self, state_file: Path = None):
        """Initialize state manager.

        Args:
            state_file: Path to state JSON file. Defaults to
                       ~/.email_agent/state.json
        """
        if state_file is None:
            state_file = Path.home() / ".email_agent" / "state.json"

        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._state: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """Load state from file."""
        if not self.state_file.exists():
            self._state = {"processed_emails": {}, "metadata": {}}
            self._save()
            return

        try:
            with open(self.state_file, "r") as f:
                self._state = json.load(f)
            logger.debug(f"Loaded state from {self.state_file}")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load state: {e}. Starting fresh.")
            self._state = {"processed_emails": {}, "metadata": {}}

    def _save(self) -> None:
        """Save state to file atomically."""
        try:
            # Write to temp file first, then rename (atomic on most filesystems)
            temp_file = self.state_file.with_suffix(".json.tmp")
            with open(temp_file, "w") as f:
                json.dump(self._state, f, indent=2, default=str)
            temp_file.replace(self.state_file)
            logger.debug(f"Saved state to {self.state_file}")
        except IOError as e:
            logger.error(f"Failed to save state: {e}")
            raise

    def mark_processed(
        self,
        email_id: str,
        decisions: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Mark an email as processed with its decisions.

        Args:
            email_id: Unique email identifier.
            decisions: Dict of decisions made about the email
                      (e.g., {'action': 'archive', 'reason': '...'}).
            metadata: Optional additional metadata.
        """
        processed_emails = self._state.setdefault("processed_emails", {})
        processed_emails[email_id] = {
            "decisions": decisions,
            "metadata": metadata or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._save()
        logger.debug(f"Marked email {email_id} as processed")

    def is_processed(self, email_id: str) -> bool:
        """Check if an email has been processed.

        Args:
            email_id: Unique email identifier.

        Returns:
            True if email was processed, False otherwise.
        """
        return email_id in self._state.get("processed_emails", {})

    def get_processed(self, email_id: str) -> Optional[Dict[str, Any]]:
        """Get processing record for an email.

        Args:
            email_id: Unique email identifier.

        Returns:
            Dict with decisions and metadata, or None if not processed.
        """
        return self._state.get("processed_emails", {}).get(email_id)

    def get_all_processed(self) -> Dict[str, Any]:
        """Get all processed emails.

        Returns:
            Dict of all processed emails and their decisions.
        """
        return self._state.get("processed_emails", {}).copy()

    def set_metadata(self, key: str, value: Any) -> None:
        """Set global metadata.

        Args:
            key: Metadata key.
            value: Metadata value.
        """
        metadata = self._state.setdefault("metadata", {})
        metadata[key] = value
        self._save()

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get global metadata.

        Args:
            key: Metadata key.
            default: Default value if key not found.

        Returns:
            Metadata value or default.
        """
        return self._state.get("metadata", {}).get(key, default)

    def clear(self) -> None:
        """Clear all state (useful for testing)."""
        self._state = {"processed_emails": {}, "metadata": {}}
        self._save()
        logger.info("State cleared")

    def save_decision(
        self,
        email_id: str,
        decision: Dict[str, Any],
        applied: bool = False,
    ) -> None:
        """Save a classification decision for an email.

        Args:
            email_id: Unique email identifier.
            decision: Decision dict with action, confidence, reasoning, tags, etc.
            applied: Whether the decision was successfully applied.
        """
        processed_emails = self._state.setdefault("processed_emails", {})
        processed_emails[email_id] = {
            "decision": decision,
            "applied": applied,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "retry_count": 0,
            "error_message": None,
        }
        self._save()
        logger.debug(f"Saved decision for email {email_id}, applied={applied}")

    def get_decision(self, email_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a past decision for an email.

        Args:
            email_id: Unique email identifier.

        Returns:
            Decision dict or None if not found.
        """
        record = self._state.get("processed_emails", {}).get(email_id)
        if record:
            return record.get("decision")
        return None

    def mark_as_applied(self, email_id: str) -> bool:
        """Mark a decision as successfully applied.

        Args:
            email_id: Unique email identifier.

        Returns:
            True if marked successfully, False if email_id not found.
        """
        record = self._state.get("processed_emails", {}).get(email_id)
        if record:
            record["applied"] = True
            record["timestamp"] = datetime.now(timezone.utc).isoformat()
            self._save()
            logger.debug(f"Marked email {email_id} decision as applied")
            return True
        return False

    def record_error(self, email_id: str, error_message: str) -> bool:
        """Record that a decision application failed.

        Args:
            email_id: Unique email identifier.
            error_message: Description of the error.

        Returns:
            True if recorded successfully, False if email_id not found.
        """
        record = self._state.get("processed_emails", {}).get(email_id)
        if record:
            record["applied"] = False
            record["error_message"] = error_message
            record["retry_count"] = record.get("retry_count", 0) + 1
            self._save()
            logger.debug(f"Recorded error for email {email_id}: {error_message}")
            return True
        return False

    def get_failed_emails(self) -> List[Dict[str, Any]]:
        """Get all emails where actions failed.

        Returns:
            List of dicts with email_id and failure details.
        """
        failed = []
        for email_id, record in self._state.get("processed_emails", {}).items():
            if not record.get("applied") and record.get("error_message"):
                failed.append(
                    {
                        "email_id": email_id,
                        "error_message": record.get("error_message"),
                        "retry_count": record.get("retry_count", 0),
                        "timestamp": record.get("timestamp"),
                    }
                )
        return failed

    def cleanup_old_entries(self, days: int = 30) -> int:
        """Clean up old state entries.

        Removes entries older than the specified number of days.

        Args:
            days: Number of days to keep. Defaults to 30.

        Returns:
            Number of entries removed.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        processed_emails = self._state.get("processed_emails", {})

        to_remove = []
        for email_id, record in processed_emails.items():
            timestamp_str = record.get("timestamp")
            if timestamp_str:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str)
                    if timestamp < cutoff:
                        to_remove.append(email_id)
                except ValueError:
                    # Skip entries with invalid timestamps
                    pass

        for email_id in to_remove:
            del processed_emails[email_id]

        if to_remove:
            self._save()
            logger.info(f"Cleaned up {len(to_remove)} old entries")

        return len(to_remove)
