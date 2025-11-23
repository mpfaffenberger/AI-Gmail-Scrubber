"""Tests for configuration system."""

import os

import pytest
import yaml

from email_agent.config import (
    Config,
    ClassificationRules,
    FolderMappings,
    IMAPConfig,
    LoggingConfig,
    ModelConfig,
    ProcessingConfig,
    create_example_config,
    init_config_file,
)


class TestIMAPConfig:
    """Test IMAP configuration validation."""

    def test_valid_imap_config(self):
        """Test creating valid IMAP config."""
        config = IMAPConfig(
            server="imap.gmail.com",
            email="user@gmail.com",
            password="app_password_123",
        )

        assert config.email == "user@gmail.com"
        assert config.port == 993

    def test_invalid_email(self):
        """Test that invalid email is rejected."""
        with pytest.raises(ValueError, match="Invalid email"):
            IMAPConfig(
                server="imap.gmail.com",
                email="notanemail",
                password="password",
            )

    def test_invalid_password(self):
        """Test that short password is rejected."""
        with pytest.raises(ValueError, match="Password too short"):
            IMAPConfig(
                server="imap.gmail.com",
                email="user@gmail.com",
                password="ab",
            )


class TestProcessingConfig:
    """Test processing configuration."""

    def test_default_processing_config(self):
        """Test default processing config."""
        config = ProcessingConfig()

        assert config.batch_size == 20
        assert config.agent_tag == "X-EmailAgent-Processed"
        assert config.dry_run is False

    def test_batch_size_validation(self):
        """Test batch size constraints."""
        # Valid
        config = ProcessingConfig(batch_size=50)
        assert config.batch_size == 50

        # Too small
        with pytest.raises(ValueError):
            ProcessingConfig(batch_size=0)

        # Too large
        with pytest.raises(ValueError):
            ProcessingConfig(batch_size=2000)


class TestModelConfig:
    """Test model configuration."""

    def test_default_model_config(self):
        """Test default model config."""
        config = ModelConfig()

        assert config.name == "claude-3-5-sonnet"
        assert config.temperature == 0.7
        assert config.max_tokens == 2000

    def test_temperature_validation(self):
        """Test temperature constraints."""
        # Valid
        config = ModelConfig(temperature=1.5)
        assert config.temperature == 1.5

        # Invalid
        with pytest.raises(ValueError):
            ModelConfig(temperature=-0.5)

        with pytest.raises(ValueError):
            ModelConfig(temperature=2.5)


class TestClassificationRules:
    """Test classification rules config."""

    def test_default_rules(self):
        """Test default rules config."""
        rules = ClassificationRules()

        assert rules.trusted_senders == []
        assert rules.spam_keywords == []
        assert rules.enabled is True

    def test_rules_with_data(self):
        """Test rules with custom data."""
        rules = ClassificationRules(
            trusted_senders=["boss@company.com"],
            spam_keywords=["unsubscribe"],
        )

        assert "boss@company.com" in rules.trusted_senders
        assert "unsubscribe" in rules.spam_keywords


class TestFolderMappings:
    """Test folder mappings."""

    def test_to_dict_excludes_none(self):
        """Test that to_dict excludes None values."""
        mappings = FolderMappings(
            receipts="Receipts",
            newsletters="Archives/Newsletters",
            invoices=None,
        )

        mapping_dict = mappings.to_dict()

        assert "receipts" in mapping_dict
        assert "newsletters" in mapping_dict
        assert "invoices" not in mapping_dict


class TestLoggingConfig:
    """Test logging configuration."""

    def test_default_logging(self):
        """Test default logging config."""
        config = LoggingConfig()

        assert config.level == "INFO"
        assert "%(levelname)s" in config.format


class TestConfigLoading:
    """Test main Config loading."""

    def test_load_from_file(self, tmp_path):
        """Test loading config from YAML file."""
        config_file = tmp_path / "config.yaml"
        config_content = """
imap:
  server: imap.example.com
  email: user@example.com
  password: secretpassword
  port: 993

processing:
  batch_size: 30
  dry_run: true

model:
  name: gpt-4
  temperature: 0.5
"""
        config_file.write_text(config_content)

        config = Config.load(str(config_file))

        assert config.imap.email == "user@example.com"
        assert config.processing.batch_size == 30
        assert config.model.name == "gpt-4"
        assert config.model.temperature == 0.5

    def test_load_file_not_found(self):
        """Test that missing file raises error."""
        with pytest.raises(FileNotFoundError):
            Config.load("/nonexistent/path/config.yaml")

    def test_load_invalid_yaml(self, tmp_path):
        """Test that invalid YAML raises error."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("invalid: yaml: content: [")

        with pytest.raises(ValueError, match="Invalid YAML"):
            Config.load(str(config_file))


class TestEnvironmentVariableSubstitution:
    """Test environment variable substitution."""

    def test_substitute_env_vars_in_dict(self):
        """Test substituting env vars in dict."""
        os.environ["TEST_EMAIL"] = "test@example.com"
        os.environ["TEST_PASSWORD"] = "secret123"

        data = {
            "email": "${TEST_EMAIL}",
            "password": "${TEST_PASSWORD}",
        }

        result = Config._substitute_env_vars(data)

        assert result["email"] == "test@example.com"
        assert result["password"] == "secret123"

    def test_substitute_env_vars_missing_defaults_to_empty(self):
        """Test that missing env vars default to empty string."""
        data = {"value": "${NONEXISTENT_VAR}"}

        result = Config._substitute_env_vars(data)

        assert result["value"] == ""

    def test_substitute_env_vars_in_list(self):
        """Test substituting env vars in lists."""
        os.environ["TEST_SENDER"] = "boss@company.com"

        data = {"trusted": ["${TEST_SENDER}", "mom@example.com"]}

        result = Config._substitute_env_vars(data)

        assert result["trusted"][0] == "boss@company.com"
        assert result["trusted"][1] == "mom@example.com"

    def test_substitute_env_vars_nested(self):
        """Test substituting in nested structures."""
        os.environ["IMAP_EMAIL"] = "nested@example.com"

        data = {
            "imap": {
                "email": "${IMAP_EMAIL}",
                "nested": {
                    "value": "literal",
                },
            },
        }

        result = Config._substitute_env_vars(data)

        assert result["imap"]["email"] == "nested@example.com"
        assert result["imap"]["nested"]["value"] == "literal"


class TestConfigCreation:
    """Test config creation and validation."""

    def test_load_with_defaults(self, tmp_path, monkeypatch):
        """Test load_with_defaults when file doesn't exist."""
        # Monkeypatch DEFAULT_CONFIG_FILE to nonexistent path
        import email_agent.config as config_module

        monkeypatch.setattr(
            config_module, "DEFAULT_CONFIG_FILE", tmp_path / "nonexistent.yaml"
        )

        # Should not raise, just use defaults
        config = Config.load_with_defaults()
        assert config is not None

    def test_validate_imap_credentials(self, tmp_path):
        """Test IMAP credential validation."""
        config = Config(
            imap=IMAPConfig(
                email="user@example.com",
                password="password123",
            )
        )

        # Should not raise
        assert config.validate_imap_credentials() is True

    def test_to_dict(self):
        """Test converting config to dict."""
        config = Config(
            imap=IMAPConfig(
                email="user@example.com",
                password="password123",
            ),
            processing=ProcessingConfig(batch_size=50),
        )

        config_dict = config.to_dict()

        assert config_dict["imap"]["email"] == "user@example.com"
        assert config_dict["processing"]["batch_size"] == 50


class TestCreateExampleConfig:
    """Test example config generation."""

    def test_create_example_config_returns_yaml(self):
        """Test that example config is valid YAML."""
        yaml_content = create_example_config()

        # Should be valid YAML
        parsed = yaml.safe_load(yaml_content)

        assert "imap" in parsed
        assert "processing" in parsed
        assert "model" in parsed
        assert parsed["imap"]["email"] == "${GMAIL_EMAIL}"


class TestInitConfigFile:
    """Test config file initialization."""

    def test_init_config_file_creates(self, tmp_path):
        """Test that init_config_file creates the file."""
        config_file = tmp_path / "config.yaml"

        result = init_config_file(str(config_file))

        assert config_file.exists()
        assert result == config_file

        # File should be valid YAML
        content = yaml.safe_load(config_file.read_text())
        assert "imap" in content

    def test_init_config_file_does_not_overwrite_by_default(self, tmp_path):
        """Test that existing file is not overwritten without force."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("original: content")

        init_config_file(str(config_file), force=False)

        assert config_file.read_text() == "original: content"

    def test_init_config_file_overwrites_with_force(self, tmp_path):
        """Test that existing file is overwritten with force=True."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("original: content")

        init_config_file(str(config_file), force=True)

        content = yaml.safe_load(config_file.read_text())
        assert "imap" in content
