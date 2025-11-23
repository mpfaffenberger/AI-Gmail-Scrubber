"""Processing commands for email agent CLI.

Handles the process, dry-run, and configure commands.
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional

import typer
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from email_agent.config import Config, DEFAULT_CONFIG_FILE
from email_agent.exceptions import EmailAgentError, ConfigurationError
from email_agent.agent import EmailAgent
from email_agent.orchestrator import EmailOrchestrator
from email_agent.security import (
    setup_secure_files,
    validate_config_permissions,
    sanitize_error_message,
)

console = Console()
logger = logging.getLogger(__name__)


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


async def _run_processing(
    config: Config,
    batch_size: int,
    dry_run: bool,
    max_concurrent: int = 2,
) -> dict:
    """Run the email processing workflow.

    Args:
        config: Configuration object.
        batch_size: Number of emails to process.
        dry_run: If True, don't actually modify emails.
        max_concurrent: Maximum number of emails to process concurrently.

    Returns:
        Processing results dict.
    """
    # Setup secure directories
    state_dir = Path.home() / ".email_agent"
    setup_secure_files(state_dir)
    state_file = state_dir / "state.json"

    # Create agent and orchestrator
    # Note: model_config is not passed - let it load from ~/.code_puppy/models.json
    # The temperature and max_tokens from config are not used (pydantic-ai defaults are used)
    agent = EmailAgent(model_name=config.model.name)

    orchestrator = EmailOrchestrator(
        agent=agent,
        state_file=state_file,
    )

    # Connect to IMAP
    await orchestrator.imap_client.connect(
        host=config.imap.server,
        email=config.imap.email,
        password=config.imap.password,
        port=config.imap.port,
    )

    try:
        # Run processing
        results = await orchestrator.run_processing(
            batch_size=batch_size,
            dry_run=dry_run,
            continue_on_error=True,
            max_concurrent=max_concurrent,
        )
        return results
    finally:
        await orchestrator.imap_client.disconnect()


def process(
    batch_size: int = typer.Option(
        1,
        "--batch-size",
        "-b",
        help="Number of emails to process",
        min=1,
        max=100,
    ),
    max_concurrent: int = typer.Option(
        2,
        "--max-concurrent",
        "-m",
        help="Maximum number of emails to process concurrently",
        min=1,
        max=10,
    ),
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
    """Process emails using AI classification.

    Fetches unprocessed emails, classifies them, and applies decisions.
    """
    try:
        # Setup logging FIRST - suppress INFO unless verbose (do this BEFORE any imports)
        import logging

        if not verbose:
            logging.basicConfig(level=logging.WARNING, force=True)
            logging.getLogger().setLevel(logging.WARNING)

        from email_agent.cli import setup_logging

        setup_logging(verbose)

        console.print("\n[cyan bold]🚀 Starting Email Processing[/cyan bold]\n")

        # Load config
        config_file = config or str(DEFAULT_CONFIG_FILE)
        cfg = Config.load(config_file)
        validate_config_permissions(Path(config_file))
        cfg.validate_imap_credentials()

        # Run processing
        results = asyncio.run(
            _run_processing(
                config=cfg,
                batch_size=batch_size,
                dry_run=False,
                max_concurrent=max_concurrent,
            )
        )

        # Show results
        show_success(
            f"Processing completed!\n"
            f"Processed: {results.get('processed', 0)} emails\n"
            f"Kept: {results.get('kept', 0)}\n"
            f"Archived: {results.get('archived', 0)}\n"
            f"Deleted: {results.get('deleted', 0)}\n"
            f"Spam: {results.get('spam', 0)}"
        )

    except KeyboardInterrupt:
        console.print("\n[yellow]Processing interrupted by user[/yellow]")
        raise typer.Exit(130)
    except FileNotFoundError:
        show_error(
            "Config file not found. Run 'email-agent init' first.",
            context={"config_file": str(DEFAULT_CONFIG_FILE)},
        )
        raise typer.Exit(1)
    except ConfigurationError as e:
        show_error(str(e), context=e.context)
        raise typer.Exit(1)
    except EmailAgentError as e:
        show_error(str(e), context=e.context)
        raise typer.Exit(1)
    except Exception as e:
        show_error(f"Processing failed: {str(e)}")
        if verbose:
            logger.exception("Full traceback:")
        raise typer.Exit(1)


def dry_run(
    batch_size: int = typer.Option(
        1,
        "--batch-size",
        "-b",
        help="Number of emails to preview",
        min=1,
        max=100,
    ),
    max_concurrent: int = typer.Option(
        2,
        "--max-concurrent",
        "-m",
        help="Maximum number of emails to process concurrently",
        min=1,
        max=10,
    ),
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
    """Preview what would happen without modifying emails.

    Shows the decisions that would be made but doesn't actually apply them.
    """
    try:
        # Setup logging FIRST - suppress INFO unless verbose (do this BEFORE any imports)
        import logging

        if not verbose:
            logging.basicConfig(level=logging.WARNING, force=True)
            logging.getLogger().setLevel(logging.WARNING)

        from email_agent.cli import setup_logging

        setup_logging(verbose)

        console.print(
            "\n[cyan bold]👀 Preview Mode (Dry Run)[/cyan bold]\n"
            "[yellow]No emails will be modified[/yellow]\n"
        )

        # Load config
        config_file = config or str(DEFAULT_CONFIG_FILE)
        cfg = Config.load(config_file)
        validate_config_permissions(Path(config_file))
        cfg.validate_imap_credentials()

        # Run processing in dry-run mode
        results = asyncio.run(
            _run_processing(
                config=cfg,
                batch_size=batch_size,
                dry_run=True,
                max_concurrent=max_concurrent,
            )
        )

        # Show preview results
        console.print(
            "[green]✓ Dry run completed[/green]\n"
            f"Would process: {results.get('processed', 0)} emails\n"
            f"Would keep: {results.get('kept', 0)}\n"
            f"Would archive: {results.get('archived', 0)}\n"
            f"Would delete: {results.get('deleted', 0)}\n"
            f"Would mark as spam: {results.get('spam', 0)}\n"
        )
        console.print(
            "[cyan]To apply these changes, run:[/cyan] "
            "[bold]email-agent process[/bold]\n"
        )

    except KeyboardInterrupt:
        console.print("\n[yellow]Dry run interrupted by user[/yellow]")
        raise typer.Exit(130)
    except FileNotFoundError:
        show_error(
            "Config file not found. Run 'email-agent init' first.",
            context={"config_file": str(DEFAULT_CONFIG_FILE)},
        )
        raise typer.Exit(1)
    except ConfigurationError as e:
        show_error(str(e), context=e.context)
        raise typer.Exit(1)
    except EmailAgentError as e:
        show_error(str(e), context=e.context)
        raise typer.Exit(1)
    except Exception as e:
        show_error(f"Dry run failed: {str(e)}")
        if verbose:
            logger.exception("Full traceback:")
        raise typer.Exit(1)


def daemon(
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
    delay: int = typer.Option(
        30,
        "--delay",
        "-d",
        help="Seconds to wait when no emails found",
        min=1,
        max=300,
    ),
    max_concurrent: int = typer.Option(
        2,
        "--max-concurrent",
        "-m",
        help="Maximum number of emails to process concurrently",
        min=1,
        max=10,
    ),
) -> None:
    """Run in daemon mode - continuously process emails one at a time.

    Fetches and processes emails in an infinite loop. Press Ctrl+C to stop.
    """
    try:
        # Setup logging FIRST - suppress INFO unless verbose (do this BEFORE any imports)
        import logging

        if not verbose:
            logging.basicConfig(level=logging.WARNING, force=True)
            logging.getLogger().setLevel(logging.WARNING)

        from email_agent.cli import setup_logging

        setup_logging(verbose)

        console.print(
            "\n[cyan bold]🔄 Daemon Mode - Continuous Email Processing[/cyan bold]\n"
            "[yellow]Press Ctrl+C to stop[/yellow]\n"
        )

        # Load config
        config_file = config or str(DEFAULT_CONFIG_FILE)
        cfg = Config.load(config_file)
        validate_config_permissions(Path(config_file))
        cfg.validate_imap_credentials()

        # Track stats
        total_processed = 0
        successful = 0
        failed = 0

        console.print(f"[cyan]Delay when no emails: {delay}s[/cyan]\n")

        # Run processing loop
        while True:
            try:
                console.print(
                    f"[dim]📥 Checking for unprocessed emails... (Total processed: {total_processed})[/dim]"
                )

                results = asyncio.run(
                    _run_processing(
                        config=cfg,
                        batch_size=1,  # Always process 1 at a time
                        dry_run=False,
                        max_concurrent=max_concurrent,
                    )
                )

                if results.get("total_processed", 0) > 0:
                    # We processed an email
                    total_processed += results.get("successful", 0) + results.get(
                        "failed", 0
                    )
                    successful += results.get("successful", 0)
                    failed += results.get("failed", 0)

                    console.print(
                        f"\n[green]✓ Session stats: {successful} successful, {failed} failed, {total_processed} total[/green]\n"
                    )
                else:
                    # No emails found, wait before checking again
                    console.print(
                        f"[dim]💤 No emails found. Waiting {delay}s...[/dim]\n"
                    )
                    import time

                    time.sleep(delay)

            except KeyboardInterrupt:
                # Let the outer handler catch it
                raise
            except Exception as e:
                console.print(f"[red]✗ Error in daemon loop: {str(e)}[/red]")
                if verbose:
                    logger.exception("Daemon loop error:")
                # Continue the loop despite errors
                import time

                time.sleep(5)  # Brief pause before retrying

    except KeyboardInterrupt:
        console.print("\n[yellow]🛑 Daemon stopped by user[/yellow]")
        console.print("\n[cyan]Final stats:[/cyan]")
        console.print(f"  Total processed: {total_processed}")
        console.print(f"  Successful: {successful}")
        console.print(f"  Failed: {failed}\n")
        raise typer.Exit(0)
    except FileNotFoundError:
        show_error(
            "Config file not found. Run 'email-agent init' first.",
            context={"config_file": str(DEFAULT_CONFIG_FILE)},
        )
        raise typer.Exit(1)
    except ConfigurationError as e:
        show_error(str(e), context=e.context)
        raise typer.Exit(1)
    except EmailAgentError as e:
        show_error(str(e), context=e.context)
        raise typer.Exit(1)
    except Exception as e:
        show_error(f"Daemon failed: {str(e)}")
        if verbose:
            logger.exception("Full traceback:")
        raise typer.Exit(1)


def configure(
    config: Optional[str] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to config file",
    ),
) -> None:
    """Interactively configure email agent."""
    try:
        console.print(
            "\n[cyan bold]⚙️  Email Agent Configuration[/cyan bold]\n"
            "[yellow]Leave blank to keep current value[/yellow]\n"
        )

        # Load existing config or use defaults
        config_file = config or str(DEFAULT_CONFIG_FILE)
        if Path(config_file).exists():
            cfg = Config.load(config_file)
            console.print("[cyan]Loaded existing config[/cyan]\n")
        else:
            cfg = Config.load_with_defaults()
            console.print("[yellow]Using default config[/yellow]\n")

        # Get IMAP settings
        console.print("[bold cyan]IMAP Settings[/bold cyan]")
        email = typer.prompt("Email", default=cfg.imap.email)
        password = typer.prompt("Password", hide_input=True, default=cfg.imap.password)
        server = typer.prompt("IMAP Server", default=cfg.imap.server)
        port = typer.prompt("IMAP Port", default=cfg.imap.port, type=int)

        # Get model settings
        console.print("\n[bold cyan]Model Settings[/bold cyan]")
        model = typer.prompt("Model", default=cfg.model.name)
        temperature = typer.prompt(
            "Temperature",
            default=cfg.model.temperature,
            type=float,
        )

        # Get processing settings
        console.print("\n[bold cyan]Processing Settings[/bold cyan]")
        batch_size = typer.prompt(
            "Batch Size",
            default=cfg.processing.batch_size,
            type=int,
        )

        # Save config
        cfg.imap.email = email
        cfg.imap.password = password
        cfg.imap.server = server
        cfg.imap.port = port
        cfg.model.name = model
        cfg.model.temperature = temperature
        cfg.processing.batch_size = batch_size

        # Write config
        config_path = Path(config_file)
        with open(config_path, "w") as f:
            yaml.dump(cfg.model_dump(), f)

        validate_config_permissions(config_path)
        show_success(f"Configuration saved to {config_path}")

    except Exception as e:
        show_error(f"Configuration failed: {str(e)}")
        raise typer.Exit(1)
