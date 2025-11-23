"""Tests for OAuth 2.0 authentication module."""

import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from email_agent.auth import (
    GoogleOAuthConfig,
    TokenData,
    TokenStore,
    GmailOAuth,
)


class TestGoogleOAuthConfig:
    """Tests for GoogleOAuthConfig."""

    def test_config_initialization(self):
        """Test OAuth config initialization."""
        config = GoogleOAuthConfig(
            client_id="test_id",
            client_secret="test_secret",
        )

        assert config.client_id == "test_id"
        assert config.client_secret == "test_secret"
        assert config.redirect_uri == "http://localhost:8080"
        assert config.scopes == ["https://www.googleapis.com/auth/gmail.modify"]

    def test_config_custom_scopes(self):
        """Test OAuth config with custom scopes."""
        scopes = ["scope1", "scope2"]
        config = GoogleOAuthConfig(
            client_id="test_id",
            client_secret="test_secret",
            scopes=scopes,
        )

        assert config.scopes == scopes


class TestTokenData:
    """Tests for TokenData."""

    def test_token_initialization(self):
        """Test token data initialization."""
        token = TokenData(
            access_token="test_token",
            refresh_token="test_refresh",
            expires_at=datetime.now().timestamp() + 3600,
        )

        assert token.access_token == "test_token"
        assert token.refresh_token == "test_refresh"
        assert token.token_type == "Bearer"

    def test_token_expiration_check(self):
        """Test token expiration checking."""
        # Not expired
        future = datetime.now().timestamp() + 3600
        token = TokenData(access_token="test", expires_at=future)
        assert not token.is_expired()

        # Expired
        past = datetime.now().timestamp() - 100
        token = TokenData(access_token="test", expires_at=past)
        assert token.is_expired()

    def test_token_no_expiration(self):
        """Test token without expiration."""
        token = TokenData(access_token="test")
        assert not token.is_expired()


class TestTokenStore:
    """Tests for TokenStore."""

    def test_token_store_initialization(self):
        """Test token store initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(os.path.join(tmpdir, "tokens.json"))
            assert not store.token_exists()

    def test_save_and_load_token(self):
        """Test saving and loading tokens."""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_file = os.path.join(tmpdir, "tokens.json")
            store = TokenStore(token_file)

            # Save token
            token = TokenData(
                access_token="test_access",
                refresh_token="test_refresh",
            )
            store.save_token(token)

            # Verify file exists
            assert store.token_exists()

            # Load token
            loaded = store.load_token()
            assert loaded is not None
            assert loaded.access_token == "test_access"
            assert loaded.refresh_token == "test_refresh"

    def test_remove_token(self):
        """Test removing token."""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_file = os.path.join(tmpdir, "tokens.json")
            store = TokenStore(token_file)

            # Save token
            token = TokenData(access_token="test")
            store.save_token(token)
            assert store.token_exists()

            # Remove token
            store.remove_token()
            assert not store.token_exists()

    def test_load_nonexistent_token(self):
        """Test loading non-existent token."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TokenStore(os.path.join(tmpdir, "tokens.json"))
            assert store.load_token() is None

    def test_token_file_permissions(self):
        """Test token file has secure permissions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_file = os.path.join(tmpdir, "tokens.json")
            store = TokenStore(token_file)

            # Save token
            token = TokenData(access_token="test")
            store.save_token(token)

            # Check permissions
            stat_info = Path(token_file).stat()
            # File should only be readable/writable by owner (0o600)
            assert (stat_info.st_mode & 0o077) == 0


class TestGmailOAuth:
    """Tests for GmailOAuth."""

    def test_oauth_initialization(self):
        """Test OAuth handler initialization."""
        config = GoogleOAuthConfig(
            client_id="test_id",
            client_secret="test_secret",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            oauth = GmailOAuth(
                config,
                token_store=TokenStore(os.path.join(tmpdir, "tokens.json")),
            )

            assert oauth.config == config
            assert not oauth.is_authenticated()

    def test_get_auth_url(self):
        """Test generating OAuth URL."""
        config = GoogleOAuthConfig(
            client_id="test_id",
            client_secret="test_secret",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            oauth = GmailOAuth(
                config,
                token_store=TokenStore(os.path.join(tmpdir, "tokens.json")),
            )

            url = oauth.get_auth_url()

            # Verify URL structure
            assert "https://accounts.google.com/o/oauth2/auth" in url
            assert "client_id=test_id" in url
            assert "redirect_uri=http%3A%2F%2Flocalhost%3A8080" in url
            assert "response_type=code" in url
            assert "access_type=offline" in url

    @pytest.mark.asyncio
    async def test_exchange_code_for_token(self):
        """Test exchanging code for tokens."""
        config = GoogleOAuthConfig(
            client_id="test_id",
            client_secret="test_secret",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            oauth = GmailOAuth(
                config,
                token_store=TokenStore(os.path.join(tmpdir, "tokens.json")),
            )

            # Mock httpx.AsyncClient
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "test_access",
                "refresh_token": "test_refresh",
                "expires_in": 3600,
                "token_type": "Bearer",
            }

            with patch("email_agent.auth.httpx.AsyncClient") as mock_client:
                mock_instance = AsyncMock()
                mock_instance.__aenter__.return_value = mock_instance
                mock_instance.post.return_value = mock_response
                mock_client.return_value = mock_instance

                # Exchange code
                token = await oauth.exchange_code_for_token("auth_code")

                assert token.access_token == "test_access"
                assert token.refresh_token == "test_refresh"
                assert oauth.is_authenticated()

    @pytest.mark.asyncio
    async def test_refresh_token(self):
        """Test refreshing access token."""
        config = GoogleOAuthConfig(
            client_id="test_id",
            client_secret="test_secret",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            oauth = GmailOAuth(
                config,
                token_store=TokenStore(os.path.join(tmpdir, "tokens.json")),
            )

            # Mock httpx.AsyncClient
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "new_access",
                "expires_in": 3600,
                "token_type": "Bearer",
            }

            with patch("email_agent.auth.httpx.AsyncClient") as mock_client:
                mock_instance = AsyncMock()
                mock_instance.__aenter__.return_value = mock_instance
                mock_instance.post.return_value = mock_response
                mock_client.return_value = mock_instance

                # Refresh token
                token = await oauth.refresh_token("refresh_token")

                assert token.access_token == "new_access"

    @pytest.mark.asyncio
    async def test_get_access_token_auto_refresh(self):
        """Test getting access token with auto-refresh."""
        config = GoogleOAuthConfig(
            client_id="test_id",
            client_secret="test_secret",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            oauth = GmailOAuth(
                config,
                token_store=TokenStore(os.path.join(tmpdir, "tokens.json")),
            )

            # Create expired token
            past = datetime.now().timestamp() - 100
            oauth._current_token = TokenData(
                access_token="old_token",
                refresh_token="refresh_token",
                expires_at=past,
            )

            # Mock refresh
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "new_token",
                "expires_in": 3600,
                "token_type": "Bearer",
            }

            with patch("email_agent.auth.httpx.AsyncClient") as mock_client:
                mock_instance = AsyncMock()
                mock_instance.__aenter__.return_value = mock_instance
                mock_instance.post.return_value = mock_response
                mock_client.return_value = mock_instance

                # Get token (should auto-refresh)
                token = await oauth.get_access_token()

                assert token == "new_token"

    @pytest.mark.asyncio
    async def test_get_access_token_no_token(self):
        """Test getting access token without tokens."""
        config = GoogleOAuthConfig(
            client_id="test_id",
            client_secret="test_secret",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            oauth = GmailOAuth(
                config,
                token_store=TokenStore(os.path.join(tmpdir, "tokens.json")),
            )

            # Should raise error
            with pytest.raises(ValueError):
                await oauth.get_access_token()


class TestXOAuth2:
    """Tests for XOAUTH2 authentication."""

    def test_xoauth2_string_format(self):
        """Test XOAUTH2 authentication string format."""
        email = "user@gmail.com"
        token = "access_token_here"

        # Build XOAUTH2 string
        auth_string = f"user={email}\x01auth=Bearer {token}\x01\x01"

        # Verify format
        assert auth_string.startswith("user=")
        assert "auth=Bearer" in auth_string
        assert auth_string.endswith("\x01\x01")

        # Verify it can be encoded
        encoded = auth_string.encode()
        assert isinstance(encoded, bytes)
