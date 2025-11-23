"""Custom exception classes for email agent.

Provides a hierarchy of exceptions with context and suggestions for debugging.
"""

from typing import Optional


class EmailAgentError(Exception):
    """Base exception for all email agent errors.

    Attributes:
        message: Error message describing what went wrong.
        context: Additional context about the error (e.g., email ID, folder name).
        suggestion: Suggestion for how to fix the error.
    """

    def __init__(
        self,
        message: str,
        context: Optional[dict] = None,
        suggestion: Optional[str] = None,
    ):
        """Initialize the exception.

        Args:
            message: Description of what went wrong.
            context: Dict with additional context (email_id, folder, etc.).
            suggestion: Suggestion for fixing the error.
        """
        self.message = message
        self.context = context or {}
        self.suggestion = suggestion
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the error message with context and suggestion.

        Returns:
            Formatted error message.
        """
        parts = [self.message]

        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"[Context: {context_str}]")

        if self.suggestion:
            parts.append(f"[Suggestion: {self.suggestion}]")

        return " ".join(parts)

    def __repr__(self) -> str:
        """Return string representation."""
        return f"{self.__class__.__name__}({self.message!r})"


class ConfigurationError(EmailAgentError):
    """Raised when configuration is invalid or missing.

    Example:
        raise ConfigurationError(
            "Missing IMAP email",
            suggestion="Set IMAP_EMAIL environment variable"
        )
    """

    pass


class IMAPConnectionError(EmailAgentError):
    """Raised when IMAP connection fails.

    Example:
        raise IMAPConnectionError(
            "Failed to connect to IMAP server",
            context={"server": "imap.gmail.com", "port": 993},
            suggestion="Check network connection and IMAP credentials"
        )
    """

    pass


class AuthenticationError(EmailAgentError):
    """Raised when authentication fails.

    Example:
        raise AuthenticationError(
            "IMAP authentication failed",
            context={"email": "user@example.com"},
            suggestion="Check IMAP password or use an app-specific password for Gmail"
        )
    """

    pass


class ClassificationError(EmailAgentError):
    """Raised when email classification fails.

    Example:
        raise ClassificationError(
            "Failed to classify email",
            context={"email_id": "12345"},
            suggestion="Check that the AI model is responding correctly"
        )
    """

    pass


class DecisionApplyError(EmailAgentError):
    """Raised when applying a decision to an email fails.

    Example:
        raise DecisionApplyError(
            "Failed to move email to folder",
            context={"email_id": "12345", "folder": "Archive"},
            suggestion="Check that the folder exists on the IMAP server"
        )
    """

    pass


class StateError(EmailAgentError):
    """Raised when state persistence fails.

    Example:
        raise StateError(
            "Failed to save state",
            context={"state_file": "~/.email_agent/state.json"},
            suggestion="Check that you have write permissions to the directory"
        )
    """

    pass


class ProcessingError(EmailAgentError):
    """Raised when email processing fails.

    Example:
        raise ProcessingError(
            "Email processing workflow failed",
            context={"batch_size": 10},
            suggestion="Check logs for more details"
        )
    """

    pass
