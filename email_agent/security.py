"""Security utilities for email agent.

Handles credential protection, file permissions, and sensitive data sanitization.
"""

import logging
import stat
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Sensitive patterns to redact in logs and errors
SENSITIVE_PATTERNS = [
    ("password", r'["\']?(password|passwd)["\']?\s*[=:][^\n]*', "[REDACTED]"),
    ("api_key", r'["\']?(api_key|apikey|api-key|key)["\']?\s*[=:][^\n]*', "[REDACTED]"),
    ("token", r'["\']?(token|access_token)["\']?\s*[=:][^\n]*', "[REDACTED]"),
    ("secret", r'["\']?(secret|client_secret)["\']?\s*[=:][^\n]*', "[REDACTED]"),
    ("email", r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", "[EMAIL]"),
]


def sanitize_error_message(message: str) -> str:
    """Remove sensitive data from error message.

    Redacts passwords, API keys, tokens, and PII from error messages
    to prevent accidental logging of sensitive information.

    Args:
        message: Error message that may contain sensitive data.

    Returns:
        Sanitized message with sensitive data redacted.
    """
    import re

    sanitized = message
    for pattern_name, pattern, replacement in SENSITIVE_PATTERNS:
        sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)

    return sanitized


def validate_config_permissions(
    config_path: Path,
    should_exist: bool = False,
) -> bool:
    """Validate that config file has secure permissions.

    Config files should be readable/writable only by the owner to prevent
    other users on the system from reading passwords and API keys.

    Args:
        config_path: Path to config file.
        should_exist: If True, file must exist.

    Returns:
        True if permissions are secure.

    Raises:
        FileNotFoundError: If should_exist=True and file not found.
        PermissionError: If permissions are not secure.
    """
    if should_exist and not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    if not config_path.exists():
        return True  # File doesn't exist yet, can't check

    # Get file permissions
    file_stat = config_path.stat()
    file_mode = file_stat.st_mode

    # Check if owner has read/write, group and others have none
    # Expected: 0o600 (owner: rw, group: none, others: none)
    perms = stat.S_IMODE(file_mode)

    if perms != 0o600:
        logger.warning(
            f"Config file {config_path} has insecure permissions: {oct(perms)}. "
            "Should be 0o600 (readable only by owner)."
        )
        # Try to fix it
        try:
            config_path.chmod(0o600)
            logger.info(f"Fixed config file permissions: {config_path}")
            return True
        except OSError as e:
            raise PermissionError(
                f"Config file has insecure permissions and cannot be fixed: {e}"
            ) from e

    return True


def setup_secure_files(directory: Path) -> None:
    """Create application directories with secure permissions.

    Creates config and state directories with restricted permissions
    (0o700 = owner read/write/execute only).

    Args:
        directory: Directory to create with secure permissions.
    """
    try:
        # Create directory if it doesn't exist
        directory.mkdir(parents=True, exist_ok=True)

        # Set secure permissions (only owner can access)
        directory.chmod(0o700)
        logger.info(f"Created secure directory: {directory}")
    except OSError as e:
        logger.error(f"Failed to create secure directory: {e}")
        raise


def is_credentials_safe(text: str) -> bool:
    """Check if text contains any credentials that shouldn't be logged.

    Args:
        text: Text to check for credentials.

    Returns:
        False if credentials detected, True if safe to log.
    """
    import re

    # Check for common credential patterns
    credential_patterns = [
        r"password\s*[=:]",
        r"api[_-]?key\s*[=:]",
        r"token\s*[=:]",
        r"secret\s*[=:]",
        r"auth\s*[=:]",
    ]

    for pattern in credential_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return False

    return True


def get_safe_config_path(config_override: Optional[str] = None) -> Path:
    """Get and validate config file path.

    Uses provided path or defaults to ~/.email_agent/config.yaml
    Validates that the path is within user's home directory for safety.

    Args:
        config_override: Optional override path.

    Returns:
        Validated config path.

    Raises:
        ValueError: If path is outside home directory.
    """
    if config_override:
        config_path = Path(config_override).expanduser().resolve()
    else:
        config_path = Path.home() / ".email_agent" / "config.yaml"

    # Ensure it's within home directory (basic security check)
    try:
        config_path.relative_to(Path.home())
    except ValueError:
        raise ValueError(
            f"Config path must be within home directory, got: {config_path}"
        )

    return config_path


def mask_email(email: str) -> str:
    """Mask email for display in logs.

    Args:
        email: Email address to mask.

    Returns:
        Masked email (e.g., u***@example.com)
    """
    if "@" not in email:
        return email

    local, domain = email.split("@", 1)
    if len(local) <= 2:
        masked_local = "*" * len(local)
    else:
        masked_local = local[0] + "*" * (len(local) - 2) + local[-1]

    return f"{masked_local}@{domain}"
