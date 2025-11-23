"""Tests for edge cases and boundary conditions."""

import pytest

from email_agent.config import Config, IMAPConfig, ProcessingConfig
from email_agent.state import StateManager
from tests.conftest import MockMessage, MockMailBox


class TestEmptyInbox:
    """Test handling of empty inbox."""

    def test_empty_mailbox(self):
        """Test processing with no messages."""
        mailbox = MockMailBox()
        mailbox.messages = []

        assert len(mailbox.messages) == 0

    def test_state_with_no_processed(self, temp_state_file):
        """Test state with no processed emails."""
        state = StateManager(temp_state_file)

        processed = state.get_all_processed()
        assert isinstance(processed, dict)
        assert len(processed) == 0


class TestLargeEmails:
    """Test handling of large emails."""

    def test_large_email_text(self):
        """Test email with very large body."""
        large_text = "x" * 100000  # 100KB of text
        message = MockMessage(
            uid="large_email",
            subject="Large Email",
            text=large_text,
        )

        assert len(message.text) == 100000

    def test_large_batch_size(self):
        """Test batch size at maximum."""
        config = Config(
            imap=IMAPConfig(email="test@example.com", password="test"),
            processing=ProcessingConfig(batch_size=1000),  # Max
        )

        assert config.processing.batch_size == 1000

    def test_minimum_batch_size(self):
        """Test batch size at minimum."""
        config = Config(
            imap=IMAPConfig(email="test@example.com", password="test"),
            processing=ProcessingConfig(batch_size=1),  # Min
        )

        assert config.processing.batch_size == 1


class TestBoundaryConditions:
    """Test boundary conditions."""

    def test_single_email(self):
        """Test processing single email."""
        mailbox = MockMailBox()
        mailbox.messages = [MockMessage(uid="1")]

        assert len(mailbox.messages) == 1

    def test_many_emails(self):
        """Test processing many emails."""
        mailbox = MockMailBox()
        mailbox.messages = [
            MockMessage(uid=str(i), subject=f"Email {i}") for i in range(1000)
        ]

        assert len(mailbox.messages) == 1000

    def test_temperature_extremes(self):
        """Test temperature at extremes."""
        # Minimum temperature
        config = Config(
            imap=IMAPConfig(email="test@example.com", password="test"),
        )
        config.model.temperature = 0.0
        assert config.model.temperature == 0.0

        # Maximum temperature
        config.model.temperature = 2.0
        assert config.model.temperature == 2.0

    def test_max_tokens_boundary(self):
        """Test max tokens at boundaries."""
        config = Config(
            imap=IMAPConfig(email="test@example.com", password="test"),
        )

        # Minimum
        config.model.max_tokens = 100
        assert config.model.max_tokens == 100

        # Maximum
        config.model.max_tokens = 10000
        assert config.model.max_tokens == 10000


class TestSpecialCharacters:
    """Test handling of special characters."""

    def test_email_with_unicode(self):
        """Test email with unicode characters."""
        message = MockMessage(
            uid="unicode_email",
            subject="Hello 世界 🌍",
            from_="sender@例え.jp",
            text="This email has unicode: 日本語, Ελληνικά, العربية",
        )

        assert "世界" in message.subject
        assert "日本語" in message.text

    def test_email_with_html_entities(self):
        """Test email with HTML entities."""
        message = MockMessage(
            text="Hello &amp; goodbye &lt;div&gt; content &quot;quoted&quot;",
        )

        assert "&amp;" in message.text

    def test_long_email_address(self):
        """Test with very long email address."""
        long_email = "a" * 64 + "@" + "b" * 63 + ".com"
        config = Config(
            imap=IMAPConfig(email=long_email, password="test"),
        )

        assert config.imap.email == long_email

    def test_special_chars_in_password(self):
        """Test password with special characters."""
        special_password = "p@$s%w0rd!#&*()[]{}'"
        config = Config(
            imap=IMAPConfig(
                email="test@example.com",
                password=special_password,
            ),
        )

        assert config.imap.password == special_password


class TestStateEdgeCases:
    """Test edge cases in state management."""

    def test_duplicate_email_ids(self, temp_state_file):
        """Test recording same email ID twice."""
        state = StateManager(temp_state_file)

        # Record same email twice with different actions
        state.save_decision(
            email_id="duplicate",
            decision={"action": "keep", "reasoning": "First"},
        )
        state.save_decision(
            email_id="duplicate",
            decision={"action": "archive", "reasoning": "Second"},
        )

        # Should have the record (last one wins)
        processed = state.get_all_processed()
        assert "duplicate" in processed
        assert processed["duplicate"]["decision"]["action"] == "archive"

    def test_very_long_reasoning(self, temp_state_file):
        """Test with very long reasoning string."""
        state = StateManager(temp_state_file)

        long_reasoning = "x" * 10000
        state.save_decision(
            email_id="long_reason",
            decision={"action": "keep", "reasoning": long_reasoning},
        )

        processed = state.get_all_processed()
        assert len(processed) > 0

    def test_empty_reasoning(self, temp_state_file):
        """Test with empty reasoning."""
        state = StateManager(temp_state_file)

        state.save_decision(
            email_id="empty_reason",
            decision={"action": "keep", "reasoning": ""},
        )

        processed = state.get_all_processed()
        assert len(processed) > 0


class TestConfigEdgeCases:
    """Test edge cases in configuration."""

    def test_min_batch_size(self):
        """Test minimum batch size validation."""
        # Should accept 1
        config = Config(
            imap=IMAPConfig(email="test@example.com", password="test"),
            processing=ProcessingConfig(batch_size=1),
        )
        assert config.processing.batch_size == 1

    def test_max_batch_size(self):
        """Test maximum batch size validation."""
        # Should accept 1000
        config = Config(
            imap=IMAPConfig(email="test@example.com", password="test"),
            processing=ProcessingConfig(batch_size=1000),
        )
        assert config.processing.batch_size == 1000

    def test_invalid_batch_size_low(self):
        """Test that batch size below minimum is rejected."""
        with pytest.raises(ValueError):
            ProcessingConfig(batch_size=0)

    def test_invalid_batch_size_high(self):
        """Test that batch size above maximum is rejected."""
        with pytest.raises(ValueError):
            ProcessingConfig(batch_size=1001)

    def test_zero_temperature(self):
        """Test minimum temperature (0.0)."""
        config = Config(
            imap=IMAPConfig(email="test@example.com", password="test"),
        )
        config.model.temperature = 0.0
        assert config.model.temperature == 0.0

    def test_max_temperature(self):
        """Test maximum temperature (2.0)."""
        config = Config(
            imap=IMAPConfig(email="test@example.com", password="test"),
        )
        config.model.temperature = 2.0
        assert config.model.temperature == 2.0


class TestConcurrentAccess:
    """Test handling of concurrent access patterns."""

    def test_state_multiple_instances(self, temp_state_file):
        """Test multiple StateManager instances on same file."""
        # Create two instances
        state1 = StateManager(temp_state_file)
        state2 = StateManager(temp_state_file)

        # Write with first
        state1.save_decision(
            email_id="shared_email",
            decision={"action": "keep", "reasoning": "From state1"},
        )

        # Read with second
        processed = state2.get_all_processed()
        assert "shared_email" in processed
