"""Authentication commands for email agent CLI.

Handles OAuth 2.0 authentication setup and management.
"""

import asyncio
import logging
import os
import webbrowser
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from email_agent.auth import GmailOAuth, GoogleOAuthConfig, TokenStore
from email_agent.config import Config, DEFAULT_CONFIG_FILE

console = Console()
logger = logging.getLogger(__name__)


def show_error(message: str) -> None:
    """Display error message to user.

    Args:
        message: Error message to display.
    """
    panel = Panel(
        Text(message, style="red"),
        title="[red]Error[/red]",
        border_style="red",
        expand=False,
    )
    console.print(panel)


def show_success(message: str) -> None:
    """Display success message to user.

    Args:
        message: Success message to display.
    """
    panel = Panel(
        Text(message, style="green"),
        title="[green]Success[/green]",
        border_style="green",
        expand=False,
    )
    console.print(panel)


def show_info(message: str) -> None:
    """Display info message to user.

    Args:
        message: Info message to display.
    """
    console.print(f"\n[cyan]{message}[/cyan]\n")


async def _auth_async(
    email: str,
    config_path: Optional[str],
    verbose: bool,
) -> None:
    """Async implementation of authentication.

    Handles the OAuth 2.0 flow asynchronously.
    """
    try:
        console.print("\n[cyan bold]🔐 Gmail OAuth 2.0 Authentication[/cyan bold]\n")

        # Load config
        config_file = config_path or str(DEFAULT_CONFIG_FILE)
        if not Path(config_file).exists():
            show_error(
                "Config file not found!\n\n"
                "Please run 'email-agent init' first to create a config file."
            )
            raise typer.Exit(1)

        try:
            cfg = Config.load(config_file)
        except Exception as e:
            show_error(f"Failed to load config: {str(e)}")
            raise typer.Exit(1)

        # Validate that OAuth is enabled
        if not cfg.imap.use_oauth:
            show_error(
                "OAuth is disabled in config!\n\n"
                "Set 'use_oauth: true' in the [imap] section of your config file."
            )
            raise typer.Exit(1)

        # Check for OAuth credentials
        client_id = cfg.oauth.client_id or os.getenv("GOOGLE_CLIENT_ID", "")
        client_secret = cfg.oauth.client_secret or os.getenv("GOOGLE_CLIENT_SECRET", "")

        if not client_id or not client_secret:
            show_error(
                "Google OAuth credentials not configured!\n\n"
                "You need to create OAuth 2.0 credentials. Follow these steps:\n\n"
                "1. Go to https://console.cloud.google.com\n"
                "2. Create a new project\n"
                "3. Enable the Gmail API\n"
                "4. Create OAuth 2.0 credentials (Desktop application)\n"
                "5. Add http://localhost:8080 as an authorized redirect URI\n"
                "6. Copy the Client ID and Client Secret\n\n"
                "Then set them as environment variables:\n"
                "  export GOOGLE_CLIENT_ID=your_client_id\n"
                "  export GOOGLE_CLIENT_SECRET=your_client_secret\n\n"
                "Or add them to your config file:\n"
                "  oauth:\n"
                "    client_id: your_client_id\n"
                "    client_secret: your_client_secret\n\n"
                "See docs/OAUTH_SETUP.md for detailed instructions."
            )
            raise typer.Exit(1)

        # Create OAuth config
        oauth_config = GoogleOAuthConfig(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri="http://localhost:8080",
        )

        # Create OAuth handler
        oauth = GmailOAuth(oauth_config)

        # Generate auth URL
        auth_url = oauth.get_auth_url()

        show_info(
            "Opening browser for authentication...\n"
            "If the browser doesn't open, visit this URL manually:\n\n"
            f"[link={auth_url}]{auth_url}[/link]\n"
        )

        # Try to open browser
        try:
            webbrowser.open(auth_url, new=1, autoraise=True)
        except Exception as e:
            logger.debug(f"Failed to open browser: {e}")
            console.print(
                "[yellow]Could not open browser automatically.\n"
                "Please visit the URL above in your browser.[/yellow]\n"
            )

        # Start local server and wait for callback
        show_info("Waiting for authorization (timeout: 5 minutes)...")

        try:
            from email_agent.auth import start_local_server

            auth_code = await start_local_server(port=8080)

        except Exception as e:
            show_error(f"Failed to capture authorization code: {str(e)}")
            raise typer.Exit(1)

        # Exchange code for tokens
        show_info("Exchanging authorization code for tokens...")

        try:
            token = await oauth.exchange_code_for_token(auth_code)
        except Exception as e:
            show_error(f"Failed to get tokens: {str(e)}")
            raise typer.Exit(1)

        # Verify authentication
        if not oauth.is_authenticated():
            show_error("Failed to authenticate. Please try again.")
            raise typer.Exit(1)

        show_success(
            f"✓ Successfully authenticated as {email}!\n\n"
            f"Access token saved to: ~/.email_agent/.tokens.json\n"
            f"Tokens will automatically refresh as needed.\n\n"
            f"You can now run:\n"
            f"  email-agent process"
        )

    except typer.Exit:
        raise
    except KeyboardInterrupt:
        console.print("\n[yellow]Authentication cancelled by user[/yellow]")
        raise typer.Exit(130)
    except Exception as e:
        show_error(f"Authentication failed: {str(e)}")
        if verbose:
            logger.exception("Full traceback:")
        raise typer.Exit(1)


def auth(
    email: str = typer.Option(
        ...,
        "--email",
        "-e",
        help="Gmail address to authenticate",
        prompt="Gmail address",
    ),
    config_path: Optional[str] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to config file",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed output",
    ),
) -> None:
    """Authenticate with Google using OAuth 2.0.

    This opens a browser window for you to log in to Google and grant
    permission for the email agent to manage your emails. Tokens are
    stored securely locally in ~/.email_agent/.tokens.json
    """
    asyncio.run(_auth_async(email, config_path, verbose))


def logout(
    config_path: Optional[str] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to config file",
    ),
) -> None:
    """Remove stored OAuth tokens.

    This will delete the stored tokens from ~/.email_agent/.tokens.json
    You'll need to run 'email-agent auth' again to re-authenticate.
    """
    try:
        console.print("\n[cyan bold]🔓 Logout from Gmail[/cyan bold]\n")

        # Remove tokens
        token_store = TokenStore()

        if not token_store.token_exists():
            console.print("[yellow]No tokens found. Already logged out.[/yellow]")
            return

        # Confirm
        if not typer.confirm("Are you sure you want to remove stored tokens?"):
            console.print("[yellow]Cancelled.[/yellow]")
            return

        token_store.remove_token()
        show_success("✓ Tokens removed successfully!")
        console.print("\nRun 'email-agent auth' to authenticate again.")

    except Exception as e:
        show_error(f"Logout failed: {str(e)}")
        raise typer.Exit(1)
