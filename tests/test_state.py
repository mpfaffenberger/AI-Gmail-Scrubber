"""Tests for state management."""

from email_agent.state import StateManager


def test_state_manager_initialization(temp_state_file):
    """Test StateManager initializes correctly."""
    state = StateManager(temp_state_file)

    assert state.state_file == temp_state_file
    assert state.get_all_processed() == {}


def test_state_manager_mark_processed(temp_state_file):
    """Test marking an email as processed."""
    state = StateManager(temp_state_file)

    decisions = {"action": "archive", "reason": "newsletter"}
    state.mark_processed("email_123", decisions)

    assert state.is_processed("email_123")
    assert state.get_processed("email_123")["decisions"] == decisions


def test_state_manager_persistence(temp_state_file):
    """Test that state persists across instances."""
    # First instance
    state1 = StateManager(temp_state_file)
    state1.mark_processed("email_1", {"action": "delete"})

    # Second instance
    state2 = StateManager(temp_state_file)

    assert state2.is_processed("email_1")
    assert state2.get_processed("email_1")["decisions"]["action"] == "delete"


def test_state_manager_metadata(temp_state_file):
    """Test metadata operations."""
    state = StateManager(temp_state_file)

    state.set_metadata("last_run", "2024-01-15")
    assert state.get_metadata("last_run") == "2024-01-15"
    assert state.get_metadata("nonexistent", "default") == "default"


def test_state_manager_clear(temp_state_file):
    """Test clearing state."""
    state = StateManager(temp_state_file)
    state.mark_processed("email_1", {"action": "keep"})
    state.set_metadata("key", "value")

    assert state.is_processed("email_1")

    state.clear()

    assert not state.is_processed("email_1")
    assert len(state.get_all_processed()) == 0
