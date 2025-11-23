"""Tests for enhanced StateManager from Phase 3."""

from datetime import datetime, timedelta, timezone


from email_agent.state import StateManager


class TestSaveDecision:
    """Test save_decision method."""

    def test_save_decision(self, tmp_path):
        """Test saving a classification decision."""
        state = StateManager(tmp_path / "state.json")

        decision = {
            "action": "archive",
            "confidence": 0.85,
            "reasoning": "Newsletter content",
            "tags": ["newsletter"],
        }

        state.save_decision("email_123", decision, applied=False)

        # Verify it was saved
        retrieved = state.get_decision("email_123")
        assert retrieved == decision

    def test_save_decision_with_applied(self, tmp_path):
        """Test saving decision with applied flag."""
        state = StateManager(tmp_path / "state.json")

        decision = {"action": "keep", "confidence": 0.95}
        state.save_decision("email_456", decision, applied=True)

        record = state._state["processed_emails"]["email_456"]
        assert record["applied"] is True


class TestGetDecision:
    """Test get_decision method."""

    def test_get_decision_exists(self, tmp_path):
        """Test retrieving an existing decision."""
        state = StateManager(tmp_path / "state.json")

        decision = {"action": "delete", "confidence": 0.9}
        state.save_decision("email_789", decision)

        retrieved = state.get_decision("email_789")
        assert retrieved == decision

    def test_get_decision_not_exists(self, tmp_path):
        """Test retrieving a non-existent decision."""
        state = StateManager(tmp_path / "state.json")

        retrieved = state.get_decision("nonexistent")
        assert retrieved is None


class TestMarkAsApplied:
    """Test mark_as_applied method."""

    def test_mark_as_applied_success(self, tmp_path):
        """Test marking a decision as applied."""
        state = StateManager(tmp_path / "state.json")

        decision = {"action": "archive"}
        state.save_decision("email_111", decision, applied=False)

        result = state.mark_as_applied("email_111")

        assert result is True
        record = state._state["processed_emails"]["email_111"]
        assert record["applied"] is True

    def test_mark_as_applied_not_found(self, tmp_path):
        """Test marking non-existent email as applied."""
        state = StateManager(tmp_path / "state.json")

        result = state.mark_as_applied("nonexistent")
        assert result is False


class TestRecordError:
    """Test record_error method."""

    def test_record_error(self, tmp_path):
        """Test recording an error for a decision."""
        state = StateManager(tmp_path / "state.json")

        decision = {"action": "move"}
        state.save_decision("email_222", decision)

        result = state.record_error("email_222", "Connection timeout")

        assert result is True
        record = state._state["processed_emails"]["email_222"]
        assert record["applied"] is False
        assert record["error_message"] == "Connection timeout"
        assert record["retry_count"] == 1

    def test_record_error_increments_retry_count(self, tmp_path):
        """Test that retry count increments."""
        state = StateManager(tmp_path / "state.json")

        decision = {"action": "move"}
        state.save_decision("email_333", decision)

        state.record_error("email_333", "Error 1")
        state.record_error("email_333", "Error 2")

        record = state._state["processed_emails"]["email_333"]
        assert record["retry_count"] == 2


class TestGetFailedEmails:
    """Test get_failed_emails method."""

    def test_get_failed_emails_empty(self, tmp_path):
        """Test getting failed emails when none exist."""
        state = StateManager(tmp_path / "state.json")

        failed = state.get_failed_emails()
        assert failed == []

    def test_get_failed_emails_with_failures(self, tmp_path):
        """Test getting list of failed emails."""
        state = StateManager(tmp_path / "state.json")

        # Save a failed email
        decision1 = {"action": "move"}
        state.save_decision("email_444", decision1)
        state.record_error("email_444", "Timeout")

        # Save a successful email
        decision2 = {"action": "archive"}
        state.save_decision("email_555", decision2, applied=True)

        failed = state.get_failed_emails()

        assert len(failed) == 1
        assert failed[0]["email_id"] == "email_444"
        assert failed[0]["error_message"] == "Timeout"
        assert failed[0]["retry_count"] == 1


class TestCleanupOldEntries:
    """Test cleanup_old_entries method."""

    def test_cleanup_old_entries_removes_old(self, tmp_path):
        """Test that old entries are removed."""
        state = StateManager(tmp_path / "state.json")

        # Add a recent entry
        decision1 = {"action": "keep"}
        state.save_decision("email_recent", decision1)

        # Manually add an old entry
        old_timestamp = (datetime.now(timezone.utc) - timedelta(days=31)).isoformat()
        state._state["processed_emails"]["email_old"] = {
            "decision": {"action": "delete"},
            "applied": True,
            "timestamp": old_timestamp,
            "retry_count": 0,
            "error_message": None,
        }
        state._save()

        # Cleanup with 30 day threshold
        removed = state.cleanup_old_entries(days=30)

        assert removed == 1
        assert "email_old" not in state._state["processed_emails"]
        assert "email_recent" in state._state["processed_emails"]

    def test_cleanup_keeps_recent(self, tmp_path):
        """Test that recent entries are not removed."""
        state = StateManager(tmp_path / "state.json")

        decision = {"action": "keep"}
        state.save_decision("email_new", decision)

        removed = state.cleanup_old_entries(days=30)

        assert removed == 0
        assert "email_new" in state._state["processed_emails"]

    def test_cleanup_handles_invalid_timestamps(self, tmp_path):
        """Test that invalid timestamps are skipped gracefully."""
        state = StateManager(tmp_path / "state.json")

        # Add entry with invalid timestamp
        state._state["processed_emails"]["email_bad_ts"] = {
            "decision": {"action": "keep"},
            "applied": True,
            "timestamp": "invalid-timestamp",
            "retry_count": 0,
            "error_message": None,
        }
        state._save()

        # Should not crash
        removed = state.cleanup_old_entries(days=30)

        assert removed == 0  # Invalid timestamp was skipped
        assert "email_bad_ts" in state._state["processed_emails"]


class TestIntegration:
    """Integration tests for Phase 3 state enhancements."""

    def test_decision_workflow(self, tmp_path):
        """Test complete decision workflow."""
        state = StateManager(tmp_path / "state.json")

        # 1. Save a decision
        decision = {
            "action": "move",
            "confidence": 0.9,
            "reasoning": "Newsletter",
            "tags": ["newsletter"],
        }
        state.save_decision("email_workflow", decision, applied=False)

        # 2. Retrieve it
        retrieved = state.get_decision("email_workflow")
        assert retrieved["action"] == "move"

        # 3. Try to apply it, encounter error
        state.record_error("email_workflow", "IMAP error")

        # 4. Check failed emails
        failed = state.get_failed_emails()
        assert len(failed) == 1

        # 5. Retry and succeed
        state.mark_as_applied("email_workflow")

        # 6. Verify it's applied
        failed = state.get_failed_emails()
        assert len(failed) == 0
