"""Configuration system for email agent.

Handles loading YAML configuration with environment variable substitution,
validation via Pydantic, and sensible defaults.
"""

import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)

# Default config paths
DEFAULT_CONFIG_DIR = Path.home() / ".email_agent"
DEFAULT_CONFIG_FILE = DEFAULT_CONFIG_DIR / "config.yaml"


class IMAPConfig(BaseModel):
    """IMAP server configuration.

    Supports both password and OAuth2 authentication.
    Exactly one must be configured.
    """

    model_config = ConfigDict(validate_assignment=True)

    server: str = Field(default="imap.gmail.com", description="IMAP server address")
    email: str = Field(description="Email address")
    password: Optional[str] = Field(
        default=None, description="Password or app-specific password"
    )
    port: int = Field(default=993, description="IMAP port")
    use_oauth: bool = Field(default=True, description="Use OAuth2 instead of password")

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        if "@" not in v:
            raise ValueError("Invalid email address")
        return v

    @field_validator("use_oauth")
    @classmethod
    def validate_auth_method(cls, v: bool, info) -> bool:
        """Ensure exactly one auth method is configured."""
        password = info.data.get("password")

        if v and password:
            raise ValueError(
                "Cannot use both OAuth and password. Set use_oauth=false to use password."
            )

        if not v and not password:
            raise ValueError("Either use_oauth=true or provide a password")

        return v


class OAuthConfig(BaseModel):
    """Google OAuth 2.0 configuration."""

    client_id: str = Field(default="", description="Google OAuth client ID")
    client_secret: str = Field(default="", description="Google OAuth client secret")
    redirect_uri: str = Field(
        default="http://localhost:8080", description="Redirect URI for OAuth callback"
    )


class ProcessingConfig(BaseModel):
    """Email processing configuration."""

    batch_size: int = Field(default=20, ge=1, le=1000)
    agent_tag: str = Field(default="X-EmailAgent-Processed")
    dry_run: bool = Field(default=False)
    skip_processed: bool = Field(default=True)


class ModelConfig(BaseModel):
    """AI model configuration."""

    name: str = Field(default="claude-3-5-sonnet")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    max_tokens: int = Field(default=2000, ge=100, le=10000)


class ClassificationRules(BaseModel):
    """Classification rules configuration."""

    trusted_senders: List[str] = Field(default_factory=list)
    spam_keywords: List[str] = Field(default_factory=list)
    enabled: bool = Field(default=True)


class FolderMappings(BaseModel):
    """Folder mapping configuration."""

    receipts: Optional[str] = None
    invoices: Optional[str] = None
    newsletters: Optional[str] = None
    notifications: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        """Convert to dict, excluding None values."""
        return {k: v for k, v in self.model_dump().items() if v is not None}


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = Field(default="INFO")
    format: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class Config(BaseModel):
    """Main configuration for email agent.

    Loads from YAML with environment variable substitution.

    Example:
        config = Config.load()  # Loads from ~/.email_agent/config.yaml
        config = Config.load("/path/to/config.yaml")
    """

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    imap: IMAPConfig
    oauth: OAuthConfig = Field(default_factory=OAuthConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    model: ModelConfig = Field(default_factory=ModelConfig)
    rules: ClassificationRules = Field(default_factory=ClassificationRules)
    folders: FolderMappings = Field(default_factory=FolderMappings)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "Config":
        """Load configuration from YAML file.

        Args:
            config_path: Path to config.yaml. Defaults to ~/.email_agent/config.yaml

        Returns:
            Config instance.

        Raises:
            FileNotFoundError: If config file not found.
            ValueError: If configuration is invalid.
        """
        if config_path is None:
            config_path = str(DEFAULT_CONFIG_FILE)

        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(
                f"Config file not found at {config_file}. "
                f"Please create it or use Config.load_with_defaults()"
            )

        try:
            with open(config_file, "r") as f:
                raw_config = yaml.safe_load(f) or {}

            # Substitute environment variables
            raw_config = cls._substitute_env_vars(raw_config)

            logger.info(f"Loaded config from {config_file}")
            return cls(**raw_config)

        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in config file: {e}") from e
        except Exception as e:
            raise ValueError(f"Failed to load config: {e}") from e

    @classmethod
    def load_with_defaults(cls) -> "Config":
        """Load configuration with fallback to defaults.

        Tries to load from ~/.email_agent/config.yaml, but returns
        a config with defaults if file doesn't exist.

        Returns:
            Config instance.
        """
        if DEFAULT_CONFIG_FILE.exists():
            try:
                return cls.load()
            except Exception as e:
                logger.warning(f"Failed to load config, using defaults: {e}")

        # Return minimal config - will fail if IMAP credentials not provided
        return cls(
            imap=IMAPConfig(
                email="user@example.com", password="dummy", use_oauth=False
            ),
        )

    @staticmethod
    def _substitute_env_vars(data: Any) -> Any:
        """Recursively substitute environment variables in config.

        Supports ${VAR_NAME} syntax. Falls back to empty string if not found.

        Args:
            data: Config dict/list to process.

        Returns:
            Config with environment variables substituted.
        """
        if isinstance(data, dict):
            return {k: Config._substitute_env_vars(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [Config._substitute_env_vars(item) for item in data]
        elif isinstance(data, str):
            # Substitute ${VAR_NAME} style variables
            def replace_var(match: re.Match) -> str:
                var_name = match.group(1)
                return os.getenv(var_name, "")

            return re.sub(r"\$\{([^}]+)\}", replace_var, data)
        else:
            return data

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dict.

        Returns:
            Configuration as dictionary.
        """
        return self.model_dump()

    def validate_imap_credentials(self) -> bool:
        """Validate that IMAP credentials are configured.

        Returns:
            True if valid, raises ValueError otherwise.
        """
        if not self.imap.email or "@" not in self.imap.email:
            raise ValueError("Invalid IMAP email address")

        if self.imap.use_oauth:
            # OAuth: check that client_id is configured
            if not self.oauth.client_id:
                raise ValueError(
                    "OAuth enabled but client_id not configured. "
                    "Set GOOGLE_CLIENT_ID or add oauth.client_id to config."
                )
        else:
            # Password: check that password is provided
            if not self.imap.password or len(self.imap.password) < 3:
                raise ValueError("Invalid IMAP password")

        return True


def create_example_config() -> str:
    """Generate example config YAML content.

    Returns:
        YAML string for example config.
    """
    return """# Email Agent Configuration
# Copy this to ~/.email_agent/config.yaml and customize

imap:
  server: imap.gmail.com
  email: ${GMAIL_EMAIL}
  port: 993
  
  # OAuth 2.0 (recommended for Gmail)
  use_oauth: true
  # password: not needed when use_oauth=true
  
  # Alternative: Use app-specific password (legacy)
  # use_oauth: false
  # password: ${GMAIL_APP_PASSWORD}

# OAuth configuration (only needed if use_oauth=true)
oauth:
  client_id: ${GOOGLE_CLIENT_ID}     # From Google Cloud Console
  client_secret: ${GOOGLE_CLIENT_SECRET}
  redirect_uri: http://localhost:8080

processing:
  batch_size: 20
  agent_tag: X-EmailAgent-Processed
  dry_run: false
  skip_processed: true

model:
  name: claude-3-5-sonnet
  temperature: 0.7
  max_tokens: 2000

rules:
  trusted_senders:
    - boss@company.com
    - team@company.com
  spam_keywords:
    - unsubscribe
    - click here now
    - limited time offer
  enabled: true

folders:
  receipts: Receipts
  invoices: Invoices/Business
  newsletters: Archives/Newsletters
  notifications: Notifications

logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
"""


def init_config_file(config_path: Optional[str] = None, force: bool = False) -> Path:
    """Initialize a config file with example content.

    Args:
        config_path: Path where to create config. Defaults to ~/.email_agent/config.yaml
        force: If True, overwrite existing file.

    Returns:
        Path to created config file.
    """
    if config_path is None:
        config_path = str(DEFAULT_CONFIG_FILE)

    config_file = Path(config_path)
    config_file.parent.mkdir(parents=True, exist_ok=True)

    if config_file.exists() and not force:
        logger.info(f"Config file already exists at {config_file}")
        return config_file

    with open(config_file, "w") as f:
        f.write(create_example_config())

    logger.info(f"Created example config at {config_file}")
    return config_file
