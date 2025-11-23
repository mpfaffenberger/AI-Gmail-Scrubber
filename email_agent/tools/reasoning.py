"""Reasoning and thought tool for transparent agent decision-making.

Allows the agent to explain its reasoning as it processes emails.
Provides rich formatting for nice output.
"""

import logging
from typing import Optional

from pydantic import BaseModel, Field
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

logger = logging.getLogger(__name__)
console = Console()


class ReasoningResult(BaseModel):
    """Result of a reasoning operation.

    Attributes:
        success: Always True (reasoning always succeeds).
        message: Confirmation message.
    """

    success: bool = Field(default=True, description="Always True")
    message: str = Field(description="Confirmation message")


class ReasoningTools:
    """Tools for agent reasoning and transparency.

    Allows the agent to explain its thought process as it makes decisions,
    using rich formatting for nice output.

    Example:
        tools = ReasoningTools()
        result = await tools.reason_about_email(
            reasoning="Email from newsletter, can safely delete",
            next_steps="Mark as read and move to spam folder"
        )
    """

    def __init__(self):
        """Initialize reasoning tools."""
        pass

    async def reason_about_email(
        self,
        reasoning: str,
        next_steps: Optional[str] = None,
    ) -> ReasoningResult:
        """Share the agent's reasoning about an email.

        Displays the agent's thought process in a formatted panel.

        Args:
            reasoning: The agent's reasoning for its decision.
            next_steps: Optional description of what the agent will do next.

        Returns:
            ReasoningResult with success=True.
        """
        logger.info("[REASONING] Agent reasoning about email decision")
        logger.debug(
            f"[REASONING] Reasoning: {reasoning[:100]}{'...' if len(reasoning) > 100 else ''}"
        )
        if next_steps:
            logger.debug(
                f"[REASONING] Next steps: {next_steps[:100]}{'...' if len(next_steps) > 100 else ''}"
            )

        # Build the content
        content = Text()
        content.append("🤔 ", style="cyan")
        content.append(reasoning)

        if next_steps:
            content.append("\n\n")
            content.append("📋 Next Steps: ", style="yellow bold")
            content.append(next_steps)

        # Display in a panel
        panel = Panel(
            content,
            title="[cyan]Agent Reasoning[/cyan]",
            border_style="cyan",
            expand=False,
        )
        console.print(panel)

        logger.debug(f"[REASONING] Reasoning details: {reasoning}")
        if next_steps:
            logger.debug(f"[REASONING] Next steps details: {next_steps}")

        logger.info("[REASONING] Reasoning shared successfully")

        return ReasoningResult(
            success=True,
            message="Reasoning shared",
        )

    async def log_decision(
        self,
        email_subject: str,
        decision: str,
        reason: Optional[str] = None,
    ) -> ReasoningResult:
        """Log an email processing decision.

        Displays the decision in a formatted panel.

        Args:
            email_subject: Subject of the email being processed.
            decision: The decision made (e.g., 'archive', 'delete', 'keep').
            reason: Optional explanation for the decision.

        Returns:
            ReasoningResult with success=True.
        """
        logger.info(f"[REASONING] Logging decision for email: '{email_subject}'")
        logger.info(f"[REASONING] Decision: {decision}")
        logger.debug(
            f"[REASONING] Email subject: {email_subject[:80]}{'...' if len(email_subject) > 80 else ''}"
        )
        logger.debug(f"[REASONING] Decision type: {decision}")
        if reason:
            logger.debug(
                f"[REASONING] Reason: {reason[:100]}{'...' if len(reason) > 100 else ''}"
            )

        # Build the content
        content = Text()
        content.append("Subject: ", style="bold white")
        content.append(email_subject, style="cyan")
        content.append("\n")
        content.append("Decision: ", style="bold white")

        # Color code decisions
        decision_color = "yellow"
        if decision.lower() in ["delete", "spam"]:
            decision_color = "red"
        elif decision.lower() in ["archive", "organize"]:
            decision_color = "green"
        elif decision.lower() in ["keep", "inbox"]:
            decision_color = "blue"

        content.append(decision, style=f"{decision_color} bold")

        if reason:
            content.append("\n")
            content.append("Reason: ", style="bold white")
            content.append(reason, style="dim")

        # Display in a panel
        panel = Panel(
            content,
            title="[cyan]Decision Log[/cyan]",
            border_style="cyan",
            expand=False,
        )
        console.print(panel)

        logger.info(f"[REASONING] Decision logged: {decision} for '{email_subject}'")
        if reason:
            logger.debug(f"[REASONING] Decision reason: {reason}")

        logger.debug("[REASONING] Decision record complete")

        return ReasoningResult(
            success=True,
            message="Decision logged",
        )
