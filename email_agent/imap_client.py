"""IMAP client connection management with singleton pattern.

Provides efficient connection pooling and management for IMAP operations.
Uses a singleton pattern since IMAP connections are expensive to create.
Supports both password authentication and OAuth2 (XOAUTH2).
"""

import logging
from typing import Optional

from imap_tools import MailBox, MailboxLoginError

logger = logging.getLogger(__name__)


class IMAPClientError(Exception):
    """Base exception for IMAP client errors."""

    pass


class IMAPConnectionError(IMAPClientError):
    """Raised when IMAP connection fails."""

    pass


class IMAPClient:
    """Singleton IMAP client for email operations.

    Manages connections to IMAP servers with proper connection pooling
    and error handling. Supports both password and OAuth2 authentication.

    Example:
        client = IMAPClient()
        await client.connect(host, email, password=password)
        # or
        await client.connect(host, email, oauth_token=token)
        mailbox = client.get_mailbox()
        # ... do work ...
        await client.disconnect()
    """

    _instance: Optional["IMAPClient"] = None

    def __new__(cls) -> "IMAPClient":
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the IMAP client."""
        if self._initialized:
            return

        self._initialized = True
        self.mailbox: Optional[MailBox] = None
        self._connected = False
        self._host: Optional[str] = None
        self._email: Optional[str] = None

    async def connect(
        self,
        host: str,
        email: str,
        password: Optional[str] = None,
        oauth_token: Optional[str] = None,
        port: int = 993,
        timeout: int = 30,
    ) -> None:
        """Connect to IMAP server with password or OAuth2.

        Args:
            host: IMAP server hostname (e.g., 'imap.gmail.com').
            email: Email address.
            password: Email password or app-specific password (mutually exclusive with oauth_token).
            oauth_token: OAuth2 access token (mutually exclusive with password).
            port: IMAP port (default 993).
            timeout: Connection timeout in seconds.

        Raises:
            IMAPConnectionError: If connection fails.
            ValueError: If neither password nor oauth_token provided, or both provided.
        """
        # No lock needed - singleton pattern ensures single instance
        # and there's no concurrent access in this codebase
        if self._connected:
            logger.debug(f"Already connected to {self._host}")
            return

        try:
            # Validate authentication method
            if not password and not oauth_token:
                raise ValueError("Either password or oauth_token must be provided")
            if password and oauth_token:
                raise ValueError("Cannot provide both password and oauth_token")

            logger.info(f"Connecting to {host} as {email}")

            # Create new mailbox connection
            self.mailbox = MailBox(host, port=port)

            if oauth_token:
                # Use XOAUTH2 authentication
                self._login_oauth(email, oauth_token)
            else:
                # Use password authentication
                self.mailbox.login(email, password, initial_folder="INBOX")

            self._connected = True
            self._host = host
            self._email = email

            logger.info(f"Successfully connected to {host}")

        except MailboxLoginError as e:
            self._connected = False
            raise IMAPConnectionError(
                f"Failed to authenticate with {host}: {str(e)}"
            ) from e
        except Exception as e:
            self._connected = False
            raise IMAPConnectionError(f"Failed to connect to {host}: {str(e)}") from e

    def _login_oauth(self, email: str, access_token: str) -> None:
        """Authenticate using XOAUTH2.

        Args:
            email: Gmail address.
            access_token: OAuth2 access token.

        Raises:
            IMAPConnectionError: If authentication fails.
        """
        try:
            if not self.mailbox:
                raise IMAPClientError("Mailbox not initialized")

            # Use imap_tools built-in xoauth2 method for OAuth2 authentication
            # This handles the XOAUTH2 authentication string format internally
            # and selects the INBOX folder automatically
            self.mailbox.xoauth2(email, access_token, initial_folder="INBOX")

            logger.debug("Successfully authenticated with XOAUTH2")

        except Exception as e:
            raise IMAPClientError(f"XOAUTH2 authentication failed: {str(e)}") from e

    async def disconnect(self) -> None:
        """Disconnect from IMAP server."""
        if self.mailbox and self._connected:
            try:
                self.mailbox.logout()
                logger.info(f"Disconnected from {self._host}")
            except Exception as e:
                logger.warning(f"Error during logout: {e}")
            finally:
                self._connected = False
                self.mailbox = None

    def get_mailbox(self) -> MailBox:
        """Get the mailbox instance.

        Returns:
            The active MailBox instance.

        Raises:
            IMAPClientError: If not connected.
        """
        if not self._connected or self.mailbox is None:
            raise IMAPClientError("IMAP client not connected. Call connect() first.")
        return self.mailbox

    def is_connected(self) -> bool:
        """Check if connected to IMAP server.

        Returns:
            True if connected, False otherwise.
        """
        return self._connected

    @classmethod
    async def reset(cls) -> None:
        """Reset the singleton instance (mainly for testing).

        This will disconnect and reset the singleton, allowing a fresh
        connection to be created.
        """
        if cls._instance:
            try:
                await cls._instance.disconnect()
            except Exception as e:
                logger.warning(f"Error during reset: {e}")
            cls._instance = None
