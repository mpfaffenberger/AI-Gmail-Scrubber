"""CLI interface for email agent using Typer.

Provides commands for email processing, status, configuration, and testing.
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.logging import RichHandler

from email_agent.config import Config, DEFAULT_CONFIG_FILE, init_config_file
from email_agent.exceptions import (
    ConfigurationError,
    IMAPConnectionError,
)
from email_agent.imap_client import IMAPClient
from email_agent.security import (
    validate_config_permissions,
    setup_secure_files,
    mask_email,
    sanitize_error_message,
)
from email_agent.state import StateManager
from email_agent.cli_processing import process, dry_run, daemon, configure
from email_agent.cli_auth import auth, logout

# Create Typer app and console
app = typer.Typer(
    name="email-agent",
    help="AI-powered email classification and management agent",
    rich_markup_mode="rich",
)
console = Console()
logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Set up logging with rich formatting.

    Args:
        verbose: If True, set log level to DEBUG.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=True, console=console)],
    )


def show_error(message: str, context: Optional[dict] = None) -> None:
    """Display error message to user.

    Args:
        message: Error message to display.
        context: Optional context dict to display.
    """
    sanitized = sanitize_error_message(message)
    panel = Panel(
        Text(sanitized, style="red"),
        title="[red]Error[/red]",
        border_style="red",
        expand=False,
    )
    console.print(panel)

    if context:
        table = Table(title="Context", show_header=False)
        for key, value in context.items():
            # Mask emails in context
            if key == "email" and isinstance(value, str):
                value = mask_email(value)
            table.add_row(key, str(value))
        console.print(table)


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


@app.command()
def init(
    config_path: Optional[str] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to config file",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Overwrite existing config",
    ),
) -> None:
    """Initialize email agent with config and directories.

    Creates ~/.email_agent/ directory with example config file.
    """
    try:
        setup_logging()
        console.print("\n[cyan bold]🚀 Email Agent Initialization[/cyan bold]\n")

        # Create secure directories
        config_dir = Path.home() / ".email_agent"
        setup_secure_files(config_dir)

        # Create config file
        config_file = init_config_file(config_path, force=force)

        console.print("\n[yellow]Next steps:[/yellow]")
        console.print(f"1. Edit [cyan]{config_file}[/cyan]")
        console.print("2. For OAuth: Run [cyan]email-agent auth[/cyan]")
        console.print("3. Or set your IMAP password if using legacy auth")
        console.print("4. Run [cyan]email-agent test-connection[/cyan]")
        console.print("5. Start processing: [cyan]email-agent process[/cyan]\n")

    except Exception as e:
        show_error(f"Initialization failed: {str(e)}")
        raise typer.Exit(1)


@app.command()
def test_connection(
    config: Optional[str] = typer.Option(
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
    """Test IMAP connection without processing emails."""
    try:
        setup_logging(verbose)
        console.print("\n[cyan bold]🔗 Testing IMAP Connection[/cyan bold]\n")

        # Load config
        config_file = config or str(DEFAULT_CONFIG_FILE)
        cfg = Config.load(config_file)
        validate_config_permissions(Path(config_file))

        # Run async connection test
        asyncio.run(_test_connection_async(cfg))

    except ConfigurationError as e:
        show_error(str(e), context=e.context)
        raise typer.Exit(1)
    except IMAPConnectionError as e:
        show_error(str(e), context=e.context)
        raise typer.Exit(1)
    except Exception as e:
        show_error(f"Connection test failed: {str(e)}")
        raise typer.Exit(1)


async def _test_connection_async(cfg: Config) -> None:
    """Async helper to test IMAP connection.

    Args:
        cfg: Configuration instance.

    Raises:
        IMAPConnectionError: If connection fails.
    """
    from email_agent.auth import TokenStore, GmailOAuth, GoogleOAuthConfig

    # Prepare authentication
    password: Optional[str] = None
    oauth_token: Optional[str] = None

    if cfg.imap.use_oauth:
        # Load OAuth token
        token_store = TokenStore()
        token = token_store.load_token()

        if not token:
            raise IMAPConnectionError(
                "OAuth token not found. Run 'email-agent auth' to authenticate first."
            )

        # Check if token is expired and refresh if needed
        if token.is_expired():
            if not token.refresh_token:
                raise IMAPConnectionError(
                    "OAuth token expired and no refresh token available. "
                    "Run 'email-agent auth' again."
                )

            # Refresh token
            oauth_handler = GmailOAuth(
                config=GoogleOAuthConfig(
                    client_id=cfg.oauth.client_id,
                    client_secret=cfg.oauth.client_secret,
                    redirect_uri=cfg.oauth.redirect_uri,
                ),
                token_store=token_store,
            )
            token = await oauth_handler.refresh_token(token.refresh_token)

        oauth_token = token.access_token
    else:
        # Use password authentication
        password = cfg.imap.password

    # Test IMAP connection
    imap = IMAPClient()
    try:
        await imap.connect(
            host=cfg.imap.server,
            email=cfg.imap.email,
            password=password,
            oauth_token=oauth_token,
            port=cfg.imap.port,
        )

        # Just verify connection works - don't fetch all emails (can be slow)
        await imap.disconnect()

        show_success(
            f"✓ IMAP connection successful!\n\n"
            f"Email: {cfg.imap.email}\n"
            f"Server: {cfg.imap.server}:{cfg.imap.port}\n"
            f"Authentication: {'OAuth2' if cfg.imap.use_oauth else 'App Password'}\n\n"
            f"Ready to process emails! Run:\n"
            f"  email-agent process --dry-run\n"
            f"or\n"
            f"  email-agent process"
        )

    except Exception:
        # Ensure cleanup
        try:
            await imap.disconnect()
        except Exception:
            pass
        raise


@app.command()
def list_models(
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed model info",
    ),
) -> None:
    """List available AI models."""
    try:
        setup_logging(verbose)
        console.print("\n[cyan bold]🤖 Available AI Models[/cyan bold]\n")

        table = Table(title="Models", show_header=True)
        table.add_column("Provider", style="cyan")
        table.add_column("Model", style="green")
        table.add_column("Status", style="yellow")

        # Anthropic models
        table.add_row("Anthropic", "claude-3-5-sonnet", "✓ Available")
        table.add_row("Anthropic", "claude-3-opus", "✓ Available")
        table.add_row("Anthropic", "claude-3-haiku", "✓ Available")

        # OpenAI models
        table.add_row("OpenAI", "gpt-4o", "✓ Available")
        table.add_row("OpenAI", "gpt-4-turbo", "✓ Available")
        table.add_row("OpenAI", "gpt-3.5-turbo", "✓ Available")

        console.print(table)

        console.print(
            "\n[yellow]Note:[/yellow] Set API keys via environment variables:\n"
            "  - Anthropic: [cyan]ANTHROPIC_API_KEY[/cyan]\n"
            "  - OpenAI: [cyan]OPENAI_API_KEY[/cyan]\n"
        )

    except Exception as e:
        show_error(f"Failed to list models: {str(e)}")
        raise typer.Exit(1)


@app.command()
def status(
    config: Optional[str] = typer.Option(
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
    """Show processing status and statistics."""
    try:
        setup_logging(verbose)
        console.print("\n[cyan bold]📊 Email Agent Status[/cyan bold]\n")

        # Load config
        config_file = config or str(DEFAULT_CONFIG_FILE)
        cfg = Config.load(config_file)

        # Load state
        state_dir = Path.home() / ".email_agent"
        state_file = state_dir / "state.json"
        state = StateManager(str(state_file))

        # Show config
        config_table = Table(title="Configuration", show_header=False)
        config_table.add_row("Email", mask_email(cfg.imap.email))
        config_table.add_row("IMAP Server", f"{cfg.imap.server}:{cfg.imap.port}")
        config_table.add_row("Model", cfg.model.name)
        config_table.add_row("Batch Size", str(cfg.processing.batch_size))
        config_table.add_row("Dry Run", "Yes" if cfg.processing.dry_run else "No")
        auth_method = "OAuth 2.0" if cfg.imap.use_oauth else "Password"
        config_table.add_row("Auth Method", auth_method)
        console.print(config_table)

        # Show state
        console.print("\n")
        state_table = Table(title="Processing State", show_header=False)
        stats = state.get_stats()
        state_table.add_row("Processed", str(stats.get("processed_count", 0)))
        state_table.add_row("Kept", str(stats.get("kept_count", 0)))
        state_table.add_row("Archived", str(stats.get("archived_count", 0)))
        state_table.add_row("Deleted", str(stats.get("deleted_count", 0)))
        state_table.add_row("Spam", str(stats.get("spam_count", 0)))
        console.print(state_table)

    except FileNotFoundError:
        show_error(
            "Config file not found. Run 'email-agent init' first.",
            context={"config_file": str(DEFAULT_CONFIG_FILE)},
        )
        raise typer.Exit(1)
    except Exception as e:
        show_error(f"Failed to get status: {str(e)}")
        raise typer.Exit(1)


@app.command()
def reset_state(
    config: Optional[str] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to config file",
    ),
    confirm: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation prompt",
    ),
) -> None:
    """Reset processing history (careful!)"""
    try:
        setup_logging()
        console.print(
            "\n[yellow bold]⚠️  WARNING: This will clear all processing history![/yellow bold]\n"
        )

        if not confirm:
            if not typer.confirm("Are you sure you want to reset?"):
                console.print("[yellow]Cancelled.[/yellow]")
                raise typer.Exit(0)

        state_dir = Path.home() / ".email_agent"
        state_file = state_dir / "state.json"

        if state_file.exists():
            state = StateManager(str(state_file))
            state.clear()
            show_success(f"State cleared: {state_file}")
        else:
            console.print("[yellow]No state file found to reset.[/yellow]")

    except Exception as e:
        show_error(f"Failed to reset state: {str(e)}")
        raise typer.Exit(1)


# Register processing commands
app.command()(process)
app.command(name="dry-run")(dry_run)
app.command()(daemon)
app.command(name="configure")(configure)

# Register auth commands
app.command()(auth)
app.command()(logout)
