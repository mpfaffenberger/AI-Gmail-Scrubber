"""Gmail OAuth 2.0 authentication module.

Provides secure OAuth authentication for Gmail IMAP access with automatic
token refresh and local storage.
"""

import asyncio
import json
import logging
import os
import secrets
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

from pydantic import BaseModel, Field
import httpx

logger = logging.getLogger(__name__)

# Default OAuth paths
DEFAULT_TOKEN_DIR = Path.home() / ".email_agent"
DEFAULT_TOKEN_FILE = DEFAULT_TOKEN_DIR / ".tokens.json"

# Google OAuth endpoints
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"


class GoogleOAuthConfig(BaseModel):
    """Google OAuth 2.0 configuration."""

    client_id: str = Field(description="Google OAuth client ID")
    client_secret: str = Field(description="Google OAuth client secret")
    redirect_uri: str = Field(
        default="http://localhost:8080", description="Redirect URI for OAuth callback"
    )
    scopes: list[str] = Field(
        default_factory=lambda: ["https://www.googleapis.com/auth/gmail.modify"],
        description="OAuth scopes to request",
    )


class TokenData(BaseModel):
    """OAuth token data."""

    access_token: str
    refresh_token: Optional[str] = None
    expires_at: Optional[float] = None
    token_type: str = "Bearer"

    def is_expired(self) -> bool:
        """Check if access token is expired."""
        if not self.expires_at:
            return False
        # Consider expired if within 5 minutes of expiration
        return datetime.now().timestamp() > (self.expires_at - 300)


class TokenStore:
    """Secure token storage in ~/.email_agent/.tokens.json"""

    def __init__(self, token_file: Optional[str] = None):
        """Initialize token store.

        Args:
            token_file: Path to token file. Defaults to ~/.email_agent/.tokens.json
        """
        if token_file:
            self.token_file = Path(token_file)
        else:
            self.token_file = DEFAULT_TOKEN_FILE

        # Ensure directory exists
        self.token_file.parent.mkdir(parents=True, exist_ok=True)

    def save_token(self, token_data: TokenData) -> None:
        """Save token to file.

        Args:
            token_data: Token data to save.
        """
        try:
            # Set file permissions to 0600 (read/write for owner only)
            with open(self.token_file, "w") as f:
                f.write(token_data.model_dump_json(indent=2))

            # Secure the file
            os.chmod(self.token_file, 0o600)
            logger.debug(f"Saved token to {self.token_file}")

        except Exception as e:
            logger.error(f"Failed to save token: {e}")
            raise

    def load_token(self) -> Optional[TokenData]:
        """Load token from file.

        Returns:
            TokenData if file exists, None otherwise.
        """
        try:
            if not self.token_file.exists():
                return None

            # Check file permissions
            stat_info = self.token_file.stat()
            if stat_info.st_mode & 0o077:  # Check if world/group readable
                logger.warning(
                    f"Token file has insecure permissions: {oct(stat_info.st_mode)}"
                )
                os.chmod(self.token_file, 0o600)

            with open(self.token_file, "r") as f:
                data = json.load(f)

            return TokenData(**data)

        except Exception as e:
            logger.error(f"Failed to load token: {e}")
            return None

    def remove_token(self) -> None:
        """Delete token file."""
        try:
            if self.token_file.exists():
                self.token_file.unlink()
                logger.debug(f"Removed token file {self.token_file}")
        except Exception as e:
            logger.error(f"Failed to remove token: {e}")
            raise

    def token_exists(self) -> bool:
        """Check if token file exists.

        Returns:
            True if token file exists, False otherwise.
        """
        return self.token_file.exists()


class GmailOAuth:
    """Gmail OAuth 2.0 handler."""

    def __init__(
        self,
        config: GoogleOAuthConfig,
        token_store: Optional[TokenStore] = None,
    ):
        """Initialize Gmail OAuth handler.

        Args:
            config: OAuth configuration.
            token_store: Token storage instance. Defaults to ~/.email_agent/.tokens.json
        """
        self.config = config
        self.token_store = token_store or TokenStore()
        self._current_token: Optional[TokenData] = None

        # Load existing token if available
        if self.token_store.token_exists():
            self._current_token = self.token_store.load_token()

    def get_auth_url(self, state: Optional[str] = None) -> str:
        """Generate OAuth authorization URL.

        Args:
            state: State parameter for CSRF protection (auto-generated if not provided).

        Returns:
            Authorization URL to open in browser.
        """
        if not state:
            state = secrets.token_urlsafe(32)

        params = {
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "response_type": "code",
            "scope": " ".join(self.config.scopes),
            "access_type": "offline",  # To get refresh token
            "state": state,
        }

        return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"

    async def exchange_code_for_token(self, code: str) -> TokenData:
        """Exchange authorization code for access/refresh tokens.

        Args:
            code: Authorization code from OAuth callback.

        Returns:
            TokenData with access and refresh tokens.

        Raises:
            ValueError: If token exchange fails.
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    GOOGLE_TOKEN_URL,
                    data={
                        "code": code,
                        "client_id": self.config.client_id,
                        "client_secret": self.config.client_secret,
                        "redirect_uri": self.config.redirect_uri,
                        "grant_type": "authorization_code",
                    },
                    timeout=30,
                )

            if response.status_code != 200:
                raise ValueError(f"Token exchange failed: {response.text}")

            data = response.json()

            # Calculate expiration time
            expires_in = data.get("expires_in", 3600)
            expires_at = datetime.now().timestamp() + expires_in

            token = TokenData(
                access_token=data["access_token"],
                refresh_token=data.get("refresh_token"),
                expires_at=expires_at,
                token_type=data.get("token_type", "Bearer"),
            )

            # Store token
            self._current_token = token
            self.token_store.save_token(token)

            logger.info("Successfully exchanged code for tokens")
            return token

        except Exception as e:
            logger.error(f"Failed to exchange code for token: {e}")
            raise ValueError(f"Token exchange failed: {str(e)}") from e

    async def refresh_token(self, refresh_token: str) -> TokenData:
        """Refresh access token using refresh token.

        Args:
            refresh_token: Refresh token.

        Returns:
            TokenData with new access token.

        Raises:
            ValueError: If refresh fails.
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    GOOGLE_TOKEN_URL,
                    data={
                        "client_id": self.config.client_id,
                        "client_secret": self.config.client_secret,
                        "refresh_token": refresh_token,
                        "grant_type": "refresh_token",
                    },
                    timeout=30,
                )

            if response.status_code != 200:
                raise ValueError(f"Token refresh failed: {response.text}")

            data = response.json()

            # Calculate expiration time
            expires_in = data.get("expires_in", 3600)
            expires_at = datetime.now().timestamp() + expires_in

            token = TokenData(
                access_token=data["access_token"],
                refresh_token=refresh_token,  # Keep original refresh token
                expires_at=expires_at,
                token_type=data.get("token_type", "Bearer"),
            )

            # Store updated token
            self._current_token = token
            self.token_store.save_token(token)

            logger.info("Successfully refreshed access token")
            return token

        except Exception as e:
            logger.error(f"Failed to refresh token: {e}")
            raise ValueError(f"Token refresh failed: {str(e)}") from e

    async def get_access_token(self) -> str:
        """Get current valid access token (auto-refresh if needed).

        Returns:
            Valid access token.

        Raises:
            ValueError: If no token available or refresh fails.
        """
        if not self._current_token:
            self._current_token = self.token_store.load_token()
            if not self._current_token:
                raise ValueError("No token available. Run authentication first.")

        # Refresh if expired
        if self._current_token.is_expired():
            if not self._current_token.refresh_token:
                raise ValueError(
                    "Token expired and no refresh token available. "
                    "Re-run authentication."
                )
            self._current_token = await self.refresh_token(
                self._current_token.refresh_token
            )

        return self._current_token.access_token

    def is_authenticated(self) -> bool:
        """Check if authenticated and token is valid.

        Returns:
            True if token exists and is valid.
        """
        if not self.token_store.token_exists():
            return False

        token = self.token_store.load_token()
        if not token:
            return False

        # Token is valid if not expired
        return not token.is_expired()


async def start_local_server(port: int = 8080) -> str:
    """Start local HTTP server to capture OAuth callback.

    Opens a simple HTTP server that listens for the OAuth callback,
    extracts the authorization code, and returns it.

    Args:
        port: Port to listen on (default 8080).

    Returns:
        Authorization code from callback.

    Raises:
        ValueError: If server fails or timeout occurs.
    """
    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        from threading import Thread
        from urllib.parse import urlparse, parse_qs

        auth_code = None
        server = None

        class CallbackHandler(BaseHTTPRequestHandler):
            nonlocal auth_code

            def do_GET(self):
                """Handle GET request from OAuth callback."""
                nonlocal auth_code

                parsed_url = urlparse(self.path)
                query_params = parse_qs(parsed_url.query)

                # Extract code from callback
                if "code" in query_params:
                    auth_code = query_params["code"][0]

                    # Send success response
                    self.send_response(200)
                    self.send_header("Content-type", "text/html")
                    self.end_headers()

                    html = """
                    <html>
                    <head><title>Authorization Successful</title></head>
                    <body>
                        <h1 style='color: green;'>✓ Authorization Successful!</h1>
                        <p>You can close this window and return to the terminal.</p>
                    </body>
                    </html>
                    """
                    self.wfile.write(html.encode())

                else:
                    # Error case
                    error = query_params.get("error", ["Unknown error"])[0]
                    self.send_response(400)
                    self.send_header("Content-type", "text/html")
                    self.end_headers()

                    html = f"""
                    <html>
                    <head><title>Authorization Failed</title></head>
                    <body>
                        <h1 style='color: red;'>✗ Authorization Failed</h1>
                        <p>Error: {error}</p>
                    </body>
                    </html>
                    """
                    self.wfile.write(html.encode())

            def log_message(self, format, *args):
                """Suppress default logging."""
                pass

        # Create and start server
        server = HTTPServer(("localhost", port), CallbackHandler)
        server_thread = Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        logger.debug(f"Local server listening on http://localhost:{port}")

        # Wait for callback (with timeout)
        timeout = 300  # 5 minutes
        start_time = asyncio.get_event_loop().time()

        while auth_code is None:
            if asyncio.get_event_loop().time() - start_time > timeout:
                raise ValueError("OAuth callback timeout")
            await asyncio.sleep(0.1)

        return auth_code

    except Exception as e:
        logger.error(f"Failed to start callback server: {e}")
        raise ValueError(f"Callback server error: {str(e)}") from e

    finally:
        if server:
            server.shutdown()
