"""Email processing orchestration system.

Manages the workflow of fetching, processing, and applying decisions to emails.
Provides dry-run mode, progress tracking, and error recovery.
Supports both password and OAuth2 authentication.
"""

import asyncio
import logging
from typing import Any, Optional

from rich.console import Console

from email_agent.agent import EmailAgent
from email_agent.imap_client import IMAPClient, IMAPClientError
from email_agent.state import StateManager

logger = logging.getLogger(__name__)
console = Console()


class OrchestratorError(Exception):
    """Base exception for orchestrator errors."""

    pass


class EmailOrchestrator:
    """Orchestrates the email processing workflow.

    Manages the lifecycle of email processing including:
    - Fetching unprocessed emails
    - Running the agent on each email
    - Applying decisions (move, delete, archive, etc.)
    - Tracking state and progress

    Supports both password and OAuth2 authentication.

    Example:
        orchestrator = EmailOrchestrator(agent, state_file='~/.email_agent/state.json')
        results = await orchestrator.run_processing(batch_size=10, dry_run=False)
    """

    def __init__(
        self,
        agent: EmailAgent,
        state_file: Optional[str] = None,
    ):
        """Initialize the orchestrator.

        Args:
            agent: The EmailAgent instance to use for processing.
            state_file: Path to state JSON file. Defaults to ~/.email_agent/state.json
        """
        self.agent = agent
        self.state = StateManager(state_file)
        self.imap_client = IMAPClient()
        self._session_id = None

    async def run_processing(
        self,
        batch_size: int = 10,
        dry_run: bool = False,
        continue_on_error: bool = True,
        max_concurrent: int = 2,
        folder: str = "INBOX",
    ) -> dict[str, Any]:
        """Main entry point for email processing workflow.

        Args:
            batch_size: Number of emails to process per batch.
            dry_run: If True, don't actually modify emails.
            continue_on_error: If True, continue processing on errors.
            max_concurrent: Maximum number of emails to process concurrently (default: 2).
            folder: IMAP folder to fetch emails from (default: "INBOX").

        Returns:
            Dict with processing statistics and results.

        Raises:
            OrchestratorError: If workflow fails critically.
        """
        try:
            if not self.imap_client.is_connected():
                raise OrchestratorError(
                    "IMAP client not connected. Call setup_imap() first."
                )

            # Simple banner
            console.print("\n[bold magenta]EMAIL PROCESSING[/bold magenta]")
            console.print("[dim]" + "─" * 60 + "[/dim]")

            # Initialize agent tools
            await self.agent.initialize_tools()

            # Get unprocessed emails from specified folder
            unprocessed = await self.get_unprocessed_emails(n=batch_size, folder=folder)

            if not unprocessed:
                console.print("\n[green]✓ No unprocessed emails found[/green]\n")
                return {
                    "success": True,
                    "total_processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "results": [],
                }

            # Process emails concurrently with semaphore
            results = []
            successful = 0
            failed = 0

            # Create a semaphore to limit concurrent processing
            semaphore = asyncio.Semaphore(max_concurrent)

            async def process_single_email(
                idx: int, email_info: dict
            ) -> dict[str, Any]:
                """Process a single email with semaphore control."""
                async with semaphore:
                    try:
                        console.print(f"\n[dim]Email {idx}/{len(unprocessed)}[/dim]")

                        result = await self.agent.process_email(
                            email_id=email_info["email_id"],
                            dry_run=dry_run,
                        )

                        # ALWAYS mark as processed to avoid infinite loops on failed emails
                        category = result.get(
                            "category", result.get("decision", "unknown")
                        )
                        
                        # Track success/failure in metadata
                        metadata = {
                            "success": result["success"],
                        }
                        if not result["success"]:
                            metadata["error"] = result.get("error", "Unknown error")
                            
                            # CRITICAL: Add IMAP keyword even when LLM fails to call tool
                            # This marks the email as classified (prevents infinite retry loops)
                            # The IMAP keyword allows get_unprocessed_emails() to skip this email
                            # in future runs using NOT(keyword=TAG) search criteria
                            if self.agent.email_tools and not dry_run:
                                try:
                                    await self.agent.email_tools.add_label_to_email(
                                        email_info["email_id"],
                                        self.agent.email_tools.agent_tag,
                                    )
                                    console.print("[yellow]⚠ Marked failed email as CLASSIFIED to avoid retry[/yellow]")
                                except Exception as tag_error:
                                    console.print(f"[red]⚠ Failed to tag email: {tag_error}[/red]")

                        await self.mark_as_processed(
                            email_info["email_id"],
                            {"category": category},
                            metadata=metadata,
                        )

                        return result

                    except Exception as e:
                        console.print(f"[red]✗ Error: {e}[/red]")
                        
                        # CRITICAL: Add IMAP keyword even on exception
                        # This marks the email as classified (prevents infinite retry loops)
                        # The IMAP keyword allows get_unprocessed_emails() to skip this email
                        # in future runs using NOT(keyword=TAG) search criteria
                        if self.agent.email_tools and not dry_run:
                            try:
                                await self.agent.email_tools.add_label_to_email(
                                    email_info["email_id"],
                                    self.agent.email_tools.agent_tag,
                                )
                                console.print("[yellow]⚠ Marked errored email as CLASSIFIED to avoid retry[/yellow]")
                            except Exception as tag_error:
                                console.print(f"[red]⚠ Failed to tag email: {tag_error}[/red]")
                        
                        # Mark as processed even on exception to avoid infinite retry
                        await self.mark_as_processed(
                            email_info["email_id"],
                            {"category": "error"},
                            metadata={"success": False, "error": str(e)},
                        )
                        
                        if not continue_on_error:
                            raise
                        return {
                            "email_id": email_info["email_id"],
                            "success": False,
                            "error": str(e),
                        }

            # Process all emails concurrently (limited by semaphore)
            tasks = [
                process_single_email(idx, email_info)
                for idx, email_info in enumerate(unprocessed, 1)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=False)

            # Count successes and failures
            for result in results:
                if isinstance(result, dict):
                    if result.get("success"):
                        successful += 1
                    else:
                        failed += 1

            # Print summary
            self._print_summary(len(unprocessed), successful, failed, dry_run)

            return {
                "success": True,
                "total_processed": len(unprocessed),
                "successful": successful,
                "failed": failed,
                "results": results,
                "dry_run": dry_run,
            }

        except Exception as e:
            raise OrchestratorError(str(e)) from e

    async def get_unprocessed_emails(self, n: int = 10, folder: str = "INBOX") -> list[dict[str, Any]]:
        """Fetch unprocessed emails from specified folder.

        Args:
            n: Number of emails to retrieve.
            folder: IMAP folder to search (default: "INBOX").

        Returns:
            List of email info dicts.
        """
        try:
            if self.agent.email_tools is None:
                raise OrchestratorError(
                    "Email tools not initialized. Run orchestrator.run_processing() first."
                )

            result = await self.agent.email_tools.get_unprocessed_emails(n=n, folder=folder)

            if result.success:
                emails = result.data["emails"]
                return emails
            else:
                return []

        except Exception:
            raise

    async def mark_as_processed(
        self,
        email_id: str,
        decisions: dict[str, Any],
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Mark an email as processed.

        Args:
            email_id: Email ID to mark.
            decisions: Decisions made about the email.
            metadata: Optional additional metadata.
        """
        self.state.mark_processed(email_id, decisions, metadata)

    async def apply_decisions(
        self,
        email_id: str,
        decisions: dict[str, Any],
    ) -> bool:
        """Apply decisions to an email.

        Args:
            email_id: Email ID to apply decisions to.
            decisions: Dict with decision info.

        Returns:
            True if successful, False otherwise.
        """
        try:
            if self.agent.email_tools is None:
                return False

            decision = decisions.get("decision", "").upper()

            if decision == "DELETE":
                result = await self.agent.email_tools.move_email_to_folder(
                    email_id,
                    "[Gmail]/Trash",
                )
                return result.success

            elif decision == "ARCHIVE":
                result = await self.agent.email_tools.move_email_to_folder(
                    email_id,
                    "[Gmail]/All Mail",
                )
                return result.success

            elif decision == "SPAM":
                result = await self.agent.email_tools.move_email_to_folder(
                    email_id,
                    "[Gmail]/Spam",
                )
                return result.success

            elif decision == "KEEP":
                result = await self.agent.email_tools.mark_email_as_read(email_id)
                return result.success
            else:
                return True

        except Exception:
            return False

    async def setup_imap(
        self,
        host: str,
        email: str,
        password: Optional[str] = None,
        oauth_token: Optional[str] = None,
        port: int = 993,
    ) -> None:
        """Set up IMAP connection with password or OAuth2.

        Args:
            host: IMAP server hostname.
            email: Email address.
            password: Email password (mutually exclusive with oauth_token).
            oauth_token: OAuth2 access token (mutually exclusive with password).
            port: IMAP port.

        Raises:
            OrchestratorError: If connection fails.
        """
        try:
            await self.imap_client.connect(
                host=host,
                email=email,
                password=password,
                oauth_token=oauth_token,
                port=port,
            )
        except IMAPClientError as e:
            raise OrchestratorError(f"Failed to set up IMAP: {str(e)}") from e

    async def cleanup(self) -> None:
        """Clean up resources."""
        try:
            await self.imap_client.disconnect()
        except Exception:
            pass

    @staticmethod
    def _print_summary(
        total: int,
        successful: int,
        failed: int,
        dry_run: bool,
    ) -> None:
        """Print a clean summary."""
        console.print("\n[bold cyan]SUMMARY[/bold cyan]")
        console.print("[dim]" + "─" * 60 + "[/dim]")
        console.print(
            f"Processed: {total} | Success: [green]{successful}[/green] | Failed: [red]{failed}[/red]"
        )
        if dry_run:
            console.print("[yellow]Mode: DRY RUN (no changes made)[/yellow]")
        console.print()
