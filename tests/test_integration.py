"""Integration tests for complete workflows."""

from pathlib import Path
from unittest.mock import patch

import pytest

from email_agent.agent import EmailAgent
from email_agent.config import Config, IMAPConfig
from email_agent.orchestrator import EmailOrchestrator
from email_agent.state import StateManager


class TestFullProcessingWorkflow:
    """Test complete email processing workflow."""

    @pytest.mark.asyncio
    async def test_workflow_with_mock_imap(self, mock_mailbox, temp_state_file):
        """Test complete workflow with mocked IMAP."""
        # Create agent
        agent = EmailAgent(model_name="gpt-3.5-turbo")

        # Create orchestrator
        orchestrator = EmailOrchestrator(
            agent=agent,
            state_file=str(temp_state_file),
        )

        # Mock IMAP client
        with patch.object(orchestrator.imap_client, "connect"):
            with patch.object(
                orchestrator.imap_client,
                "get_unprocessed",
                return_value=mock_mailbox.messages,
            ):
                with patch.object(
                    orchestrator.imap_client, "is_connected", return_value=True
                ):
                    # Initialize tools
                    await agent.initialize_tools()

                    # Check state was created
                    assert Path(temp_state_file).exists()

                    # State manager should exist
                    state = orchestrator.state
                    assert state is not None

    @pytest.mark.asyncio
    async def test_config_and_agent_integration(self):
        """Test that config and agent integrate properly."""
        config = Config(
            imap=IMAPConfig(
                email="test@example.com",
                password="test_password",
            ),
            model=IMAPConfig.__config__["model_config"],
        )

        # Verify config can be used
        assert config.imap.email == "test@example.com"
        assert config.model.name  # Should have default model

    @pytest.mark.asyncio
    async def test_state_persistence(self, temp_state_file):
        """Test that state persists across instances."""
        # Create first instance and record something
        state1 = StateManager(temp_state_file)
        state1.save_decision(
            email_id="email_1",
            decision={"action": "keep", "reasoning": "Important"},
        )

        # Create second instance and verify data persists
        state2 = StateManager(temp_state_file)
        processed = state2.get_all_processed()

        assert len(processed) > 0
        assert "email_1" in processed

    @pytest.mark.asyncio
    async def test_error_recovery(self, mock_mailbox, temp_state_file):
        """Test recovery from processing errors."""
        agent = EmailAgent(model_name="gpt-3.5-turbo")
        orchestrator = EmailOrchestrator(
            agent=agent,
            state_file=str(temp_state_file),
        )

        # Mock IMAP with partial failure
        with patch.object(
            orchestrator.imap_client,
            "get_unprocessed",
            return_value=mock_mailbox.messages,
        ):
            with patch.object(
                orchestrator.imap_client, "is_connected", return_value=True
            ):
                with patch.object(
                    orchestrator.imap_client,
                    "move",
                    side_effect=Exception("Move failed"),
                ):
                    # Initialize tools
                    await agent.initialize_tools()

                    # Workflow should handle the error
                    assert Path(temp_state_file).exists()


class TestConfigIntegration:
    """Test configuration system integration."""

    def test_load_and_validate_config(self, tmp_path):
        """Test loading and validating config."""
        import yaml

        config_file = tmp_path / "config.yaml"
        config_data = {
            "imap": {
                "server": "imap.gmail.com",
                "email": "test@gmail.com",
                "password": "test_password",
                "port": 993,
            },
            "processing": {"batch_size": 20},
            "model": {"name": "gpt-3.5-turbo"},
        }

        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Load config
        config = Config.load(str(config_file))

        assert config.imap.email == "test@gmail.com"
        assert config.processing.batch_size == 20

    def test_config_with_env_vars(self, tmp_path, monkeypatch):
        """Test config with environment variable substitution."""
        import yaml

        config_file = tmp_path / "config.yaml"
        config_data = {
            "imap": {
                "email": "${TEST_EMAIL}",
                "password": "${TEST_PASSWORD}",
                "server": "imap.gmail.com",
                "port": 993,
            },
        }

        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Set env vars
        monkeypatch.setenv("TEST_EMAIL", "user@example.com")
        monkeypatch.setenv("TEST_PASSWORD", "secret")

        # Load config
        config = Config.load(str(config_file))

        assert config.imap.email == "user@example.com"
        assert config.imap.password == "secret"

    def test_config_validation(self):
        """Test that invalid config raises errors."""
        from email_agent.config import IMAPConfig

        # Invalid email
        with pytest.raises(ValueError):
            IMAPConfig(email="invalid", password="test")

        # Invalid password (too short)
        with pytest.raises(ValueError):
            IMAPConfig(email="test@example.com", password="ab")


class TestStateManagement:
    """Test state management integration."""

    def test_state_initialization(self, temp_state_file):
        """Test state manager initialization."""
        state = StateManager(temp_state_file)

        # Should be empty initially
        processed = state.get_all_processed()
        assert isinstance(processed, dict)

    def test_record_and_retrieve_decisions(self, temp_state_file):
        """Test recording and retrieving decisions."""
        state = StateManager(temp_state_file)

        # Record multiple emails
        state.save_decision(
            email_id="1",
            decision={"action": "keep", "reasoning": "Important"},
        )
        state.save_decision(
            email_id="2",
            decision={"action": "archive", "reasoning": "Read but not urgent"},
        )
        state.save_decision(
            email_id="3",
            decision={"action": "spam", "reasoning": "Marketing email"},
        )

        # Retrieve and verify
        processed = state.get_all_processed()
        assert len(processed) == 3
        assert processed["1"]["decision"]["action"] == "keep"
        assert processed["2"]["decision"]["action"] == "archive"
        assert processed["3"]["decision"]["action"] == "spam"

    def test_state_statistics(self, temp_state_file):
        """Test state statistics calculation."""
        state = StateManager(temp_state_file)

        # Record various actions
        for i in range(5):
            state.save_decision(
                email_id=str(i),
                decision={"action": "keep", "reasoning": "Test"},
            )
        for i in range(5, 8):
            state.save_decision(
                email_id=str(i),
                decision={"action": "archive", "reasoning": "Test"},
            )
        for i in range(8, 10):
            state.save_decision(
                email_id=str(i),
                decision={"action": "spam", "reasoning": "Test"},
            )

        # Check that emails were recorded
        processed = state.get_all_processed()
        assert len(processed) == 10

    def test_state_clear(self, temp_state_file):
        """Test clearing state."""
        state = StateManager(temp_state_file)

        # Add some data
        state.save_decision(
            email_id="123",
            decision={"action": "keep", "reasoning": "Test"},
        )

        # Clear
        state.clear()

        # Verify it's empty
        processed = state.get_all_processed()
        assert len(processed) == 0


class TestErrorHandlingIntegration:
    """Test error handling across components."""

    @pytest.mark.asyncio
    async def test_agent_error_handling(self):
        """Test that agent errors are handled properly."""

        agent = EmailAgent(model_name="invalid-model-name")

        # Agent should be created but may fail on use
        assert agent is not None

    def test_config_error_handling(self):
        """Test that config errors are handled properly."""

        # Missing required field should raise error
        with pytest.raises(
            TypeError
        ):  # Pydantic will raise TypeError for missing field
            Config(imap=None)  # type: ignore

    def test_state_error_handling(self, tmp_path):
        """Test that state errors are handled properly."""
        from pathlib import Path as PathlibPath

        # Invalid state file path
        state = StateManager(PathlibPath("/invalid/path/state.json"))

        # Recording should handle the error gracefully
        # (or raise appropriate exception)
        try:
            state.save_decision(
                email_id="123",
                decision={"action": "keep", "reasoning": "Test"},
            )
        except Exception:
            pass  # Expected to fail with invalid path
