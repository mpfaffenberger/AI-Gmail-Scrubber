"""Tests for error handling and custom exceptions."""

import pytest

from email_agent.exceptions import (
    EmailAgentError,
    ConfigurationError,
    IMAPConnectionError,
    AuthenticationError,
    ClassificationError,
    DecisionApplyError,
    StateError,
    ProcessingError,
)
from email_agent.security import (
    sanitize_error_message,
    is_credentials_safe,
    validate_config_permissions,
    setup_secure_files,
)
import stat


class TestEmailAgentError:
    """Test base EmailAgentError."""

    def test_error_with_message(self):
        """Test creating error with message."""
        error = EmailAgentError("Something went wrong")
        assert str(error) == "Something went wrong"

    def test_error_with_context(self):
        """Test creating error with context."""
        context = {"email_id": "123", "action": "move"}
        error = EmailAgentError(
            "Failed to move email",
            context=context,
        )
        assert "email_id=123" in str(error)
        assert "action=move" in str(error)

    def test_error_with_suggestion(self):
        """Test creating error with suggestion."""
        error = EmailAgentError(
            "Connection failed",
            suggestion="Check your network connection",
        )
        assert "Suggestion:" in str(error)
        assert "network" in str(error)

    def test_error_with_all_fields(self):
        """Test creating error with all fields."""
        error = EmailAgentError(
            "Something bad happened",
            context={"user": "john@example.com"},
            suggestion="Try again later",
        )
        assert "bad happened" in str(error)
        assert "Context:" in str(error)
        assert "Suggestion:" in str(error)


class TestConfigurationError:
    """Test ConfigurationError."""

    def test_configuration_error_creation(self):
        """Test creating configuration error."""
        error = ConfigurationError(
            "Invalid email format",
            suggestion="Email must contain @",
        )
        assert isinstance(error, EmailAgentError)
        assert "Invalid email" in str(error)

    def test_missing_imap_settings(self):
        """Test error for missing IMAP settings."""
        error = ConfigurationError(
            "Missing IMAP email",
            context={"field": "imap.email"},
            suggestion="Set IMAP_EMAIL environment variable",
        )
        assert "Missing IMAP" in str(error)


class TestIMAPConnectionError:
    """Test IMAPConnectionError."""

    def test_connection_error_creation(self):
        """Test creating connection error."""
        error = IMAPConnectionError(
            "Failed to connect",
            context={"server": "imap.gmail.com", "port": 993},
        )
        assert "imap.gmail.com" in str(error)
        assert "993" in str(error)


class TestAuthenticationError:
    """Test AuthenticationError."""

    def test_auth_error_creation(self):
        """Test creating authentication error."""
        error = AuthenticationError(
            "Authentication failed",
            suggestion="Check your password",
        )
        assert "Authentication" in str(error)


class TestClassificationError:
    """Test ClassificationError."""

    def test_classification_error_creation(self):
        """Test creating classification error."""
        error = ClassificationError(
            "Failed to classify email",
            context={"email_id": "456"},
        )
        assert "classify" in str(error).lower()


class TestDecisionApplyError:
    """Test DecisionApplyError."""

    def test_decision_apply_error_creation(self):
        """Test creating decision apply error."""
        error = DecisionApplyError(
            "Failed to move email",
            context={"email_id": "789", "folder": "Archive"},
            suggestion="Check that folder exists",
        )
        assert "move" in str(error).lower()
        assert "Archive" in str(error)


class TestStateError:
    """Test StateError."""

    def test_state_error_creation(self):
        """Test creating state error."""
        error = StateError(
            "Failed to save state",
            context={"state_file": "~/.email_agent/state.json"},
        )
        assert "save" in str(error).lower()


class TestProcessingError:
    """Test ProcessingError."""

    def test_processing_error_creation(self):
        """Test creating processing error."""
        error = ProcessingError(
            "Batch processing failed",
            context={"batch_size": 20},
        )
        assert "processing" in str(error).lower()


class TestSanitizeErrorMessage:
    """Test error message sanitization."""

    def test_sanitize_password(self):
        """Test that passwords are sanitized."""
        message = 'Error: password="mysecretpass" in connection'
        sanitized = sanitize_error_message(message)
        assert "mysecretpass" not in sanitized
        assert "[REDACTED]" in sanitized

    def test_sanitize_api_key(self):
        """Test that API keys are sanitized."""
        message = "Config: api_key=sk-123456789 invalid"
        sanitized = sanitize_error_message(message)
        assert "sk-123456789" not in sanitized
        assert "[REDACTED]" in sanitized

    def test_sanitize_token(self):
        """Test that tokens are sanitized."""
        message = "Bearer token=abcdef123456 expired"
        sanitized = sanitize_error_message(message)
        assert "abcdef123456" not in sanitized
        assert "[REDACTED]" in sanitized

    def test_sanitize_email(self):
        """Test that emails are sanitized."""
        message = "User john@example.com failed authentication"
        sanitized = sanitize_error_message(message)
        assert "john@example.com" not in sanitized or "[EMAIL]" in sanitized

    def test_sanitize_case_insensitive(self):
        """Test that sanitization is case-insensitive."""
        message = 'PASSWORD="secret123"'
        sanitized = sanitize_error_message(message)
        assert "secret123" not in sanitized


class TestIsCredentialsSafe:
    """Test credential safety checking."""

    def test_safe_message(self):
        """Test that safe messages pass."""
        assert is_credentials_safe("Processing email from user@example.com")

    def test_password_in_text(self):
        """Test that password patterns are detected."""
        assert not is_credentials_safe("password = mysecret")
        assert not is_credentials_safe("Password: secret123")

    def test_api_key_in_text(self):
        """Test that API key patterns are detected."""
        assert not is_credentials_safe("api_key=sk-12345")
        assert not is_credentials_safe("apikey: secret")

    def test_token_in_text(self):
        """Test that token patterns are detected."""
        assert not is_credentials_safe("token=abc123")
        assert not is_credentials_safe("access_token: secret")

    def test_case_insensitive_detection(self):
        """Test that detection is case-insensitive."""
        assert not is_credentials_safe("PASSWORD=secret")
        assert not is_credentials_safe("API_KEY=secret")


class TestValidateConfigPermissions:
    """Test config file permission validation."""

    def test_nonexistent_file_ok(self, tmp_path):
        """Test that nonexistent file passes check."""
        config_file = tmp_path / "config.yaml"
        assert validate_config_permissions(config_file, should_exist=False)

    def test_nonexistent_file_fails_when_required(self, tmp_path):
        """Test that missing file fails when should_exist=True."""
        config_file = tmp_path / "config.yaml"
        with pytest.raises(FileNotFoundError):
            validate_config_permissions(config_file, should_exist=True)

    def test_secure_permissions_pass(self, tmp_path):
        """Test that 0o600 permissions pass."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("test config")
        config_file.chmod(0o600)
        assert validate_config_permissions(config_file)

    def test_insecure_permissions_fixed(self, tmp_path):
        """Test that insecure permissions are fixed."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("test config")
        config_file.chmod(0o644)  # Readable by anyone

        # Should fix the permissions
        assert validate_config_permissions(config_file)

        # Check that permissions are now 0o600
        perms = stat.S_IMODE(config_file.stat().st_mode)
        assert perms == 0o600


class TestSetupSecureFiles:
    """Test secure file setup."""

    def test_create_secure_directory(self, tmp_path):
        """Test that directory is created with secure permissions."""
        secure_dir = tmp_path / "secure"
        setup_secure_files(secure_dir)

        assert secure_dir.exists()
        perms = stat.S_IMODE(secure_dir.stat().st_mode)
        assert perms == 0o700

    def test_existing_directory_secured(self, tmp_path):
        """Test that existing directory permissions are secured."""
        secure_dir = tmp_path / "secure"
        secure_dir.mkdir()
        secure_dir.chmod(0o777)  # Insecure

        setup_secure_files(secure_dir)

        perms = stat.S_IMODE(secure_dir.stat().st_mode)
        assert perms == 0o700
