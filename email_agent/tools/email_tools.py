"""Email manipulation tools for the agent.

Provides tools for reading, moving, marking, and managing emails via IMAP.
All tools are async-compatible and properly typed for pydantic-ai.
"""

import logging
from typing import Any, Dict, Optional

from imap_tools import MailBox
from pydantic import BaseModel, Field
from pydantic_ai import RunContext

logger = logging.getLogger(__name__)


class ToolResult(BaseModel):
    """Standard result type for all email tools.

    Attributes:
        success: Whether the operation succeeded.
        message: Human-readable result message.
        data: Optional additional data from the operation.
    """

    success: bool = Field(description="Whether the operation succeeded")
    message: str = Field(description="Human-readable result message")
    data: Optional[dict] = Field(default=None, description="Optional additional data")


class EmailInfo(BaseModel):
    """Information about a single email.

    Attributes:
        email_id: Unique email identifier.
        subject: Email subject line.
        from_addr: Sender email address.
        to_addr: Recipient email address.
        date: Email date.
        preview: First 200 chars of email body.
    """

    email_id: str = Field(description="Unique email identifier")
    subject: str = Field(description="Email subject line")
    from_addr: str = Field(description="Sender email address")
    to_addr: str = Field(description="Recipient email address")
    date: str = Field(description="Email date")
    preview: str = Field(description="First 200 chars of email body")


class EmailContent(BaseModel):
    """Full content of an email.

    Attributes:
        email_id: Unique email identifier.
        subject: Email subject line.
        from_addr: Sender email address.
        to_addr: Recipient email address.
        date: Email date.
        body: Full email body text.
        labels: List of labels/tags on the email.
    """

    email_id: str = Field(description="Unique email identifier")
    subject: str = Field(description="Email subject line")
    from_addr: str = Field(description="Sender email address")
    to_addr: str = Field(description="Recipient email address")
    date: str = Field(description="Email date")
    body: str = Field(description="Full email body text")
    labels: list[str] = Field(
        default_factory=list, description="Labels/tags on the email"
    )


class EmailTools:
    """Collection of email manipulation tools.

    All methods are designed to work with pydantic-ai and return proper
    typed results. Requires an active IMAP connection via IMAPClient.

    Example:
        tools = EmailTools(mailbox)
        result = await tools.get_unprocessed_emails(n=10)
        if result.success:
            emails = result.data['emails']
    """

    def __init__(self, mailbox: MailBox, agent_tag: str = "EMAIL_AGENT_PROCESSED"):
        """Initialize email tools.

        Args:
            mailbox: Active MailBox instance from imap_tools.
            agent_tag: Tag to mark emails processed by agent.
        """
        self.mailbox = mailbox
        self.agent_tag = agent_tag

    async def get_unprocessed_emails(
        self, n: int = 10, tag: Optional[str] = None
    ) -> ToolResult:
        """Get the most recent unprocessed emails.

        Uses IMAP search to find emails that don't have the agent classification tag,
        sorted by date in descending order (newest first).

        Args:
            n: Number of emails to retrieve (default 10).
            tag: Optional specific tag to filter by. Uses agent_tag if not specified.

        Returns:
            ToolResult with list of EmailInfo objects in data['emails'].
        """
        try:
            from imap_tools import NOT, AND
            
            filter_tag = tag or self.agent_tag

            # Search for emails that DON'T have our classification keyword
            # This properly queries the IMAP server instead of client-side filtering
            # The NOT(keyword=...) search criteria finds unclassified emails
            search_criteria = NOT(keyword=filter_tag)
            
            # Fetch messages matching criteria, reverse=True for newest first
            # Limit fetch to avoid overwhelming large mailboxes
            fetch_limit = max(n * 2, 50)  # Fetch extra in case some are skipped
            
            messages = list(
                self.mailbox.fetch(
                    criteria=search_criteria,
                    mark_seen=False,
                    reverse=True,  # Newest first (descending by date)
                    limit=fetch_limit,
                )
            )

            # Take only the requested number
            recent = messages[:n]

            # Convert to EmailInfo objects
            emails = []
            for msg in recent:
                # Handle tuple/list for to_addr and from_addr (imap-tools returns tuples)
                to_addr = (
                    msg.to[0]
                    if isinstance(msg.to, (tuple, list)) and msg.to
                    else (msg.to or "(unknown)")
                )
                from_addr = (
                    msg.from_
                    if isinstance(msg.from_, str)
                    else (msg.from_ or "(unknown)")
                )

                email_info = EmailInfo(
                    email_id=msg.uid,
                    subject=msg.subject or "(no subject)",
                    from_addr=from_addr,
                    to_addr=to_addr,
                    date=str(msg.date),
                    preview=self._get_preview(msg),
                )
                emails.append(email_info.model_dump())

            return ToolResult(
                success=True,
                message=f"Retrieved {len(emails)} unprocessed emails (sorted by date descending)",
                data={"emails": emails},
            )

        except Exception as e:
            return ToolResult(
                success=False,
                message=f"Failed to get unprocessed emails: {str(e)}",
            )

    async def get_email_content(self, email_id: str) -> ToolResult:
        """Get full content of a specific email.

        Args:
            email_id: Unique email identifier (UID).

        Returns:
            ToolResult with EmailContent in data['content'].
        """
        try:
            # imap-tools fetch() expects a criteria or UID
            # When fetching by UID, we need to pass it as a string UID
            from imap_tools import AND

            msg_list = list(self.mailbox.fetch(AND(uid=email_id), mark_seen=False))
            if not msg_list:
                return ToolResult(
                    success=False,
                    message=f"Email {email_id} not found",
                )

            msg = msg_list[0]
            labels = list(msg.flags) if hasattr(msg, "flags") else []

            # Handle tuple/list for to_addr and from_addr (imap-tools returns tuples)
            to_addr = (
                msg.to[0]
                if isinstance(msg.to, (tuple, list)) and msg.to
                else (msg.to or "(unknown)")
            )
            from_addr = (
                msg.from_ if isinstance(msg.from_, str) else (msg.from_ or "(unknown)")
            )

            content = EmailContent(
                email_id=msg.uid,
                subject=msg.subject or "(no subject)",
                from_addr=from_addr,
                to_addr=to_addr,
                date=str(msg.date),
                body=msg.text or "(no body)",
                labels=labels,
            )

            return ToolResult(
                success=True,
                message=f"Retrieved content for email {email_id}",
                data={"content": content.model_dump()},
            )

        except Exception as e:
            return ToolResult(
                success=False,
                message=f"Failed to get email content: {str(e)}",
            )

    async def mark_email_as_read(self, email_id: str) -> ToolResult:
        """Mark an email as read.

        Args:
            email_id: Unique email identifier (UID).

        Returns:
            ToolResult indicating success or failure.
        """
        try:
            self.mailbox.flag(email_id, [r"\Seen"], True)
            return ToolResult(
                success=True,
                message=f"Marked email {email_id} as read",
            )
        except Exception as e:
            return ToolResult(
                success=False,
                message=f"Failed to mark email as read: {str(e)}",
            )

    async def add_label_to_email(self, email_id: str, label: str) -> ToolResult:
        """Add a label/tag to an email as an IMAP keyword.

        This adds the label as an IMAP keyword (custom flag) that can be searched
        using IMAP search criteria. The keyword will persist on the server and can
        be used to track which emails have been processed by the agent.

        Args:
            email_id: Unique email identifier (UID).
            label: Label/tag to add as an IMAP keyword.

        Returns:
            ToolResult indicating success or failure.
        """
        try:
            # Add as IMAP keyword (custom flag)
            # The mailbox.flag() method adds custom IMAP keywords that persist server-side
            # and can be searched with keyword=label in search criteria
            self.mailbox.flag(email_id, [label], True)
            return ToolResult(
                success=True,
                message=f"Added IMAP keyword '{label}' to email {email_id}",
            )
        except Exception as e:
            return ToolResult(
                success=False,
                message=f"Failed to add IMAP keyword: {str(e)}",
            )

    async def move_email_to_folder(self, email_id: str, folder_path: str) -> ToolResult:
        """Move an email to a specific folder.

        Args:
            email_id: Unique email identifier (UID).
            folder_path: Target folder path (e.g., 'Archive' or '[Gmail]/Archive').

        Returns:
            ToolResult indicating success or failure.
        """
        try:
            # Create folder if it doesn't exist
            await self.create_folder(folder_path)

            # Move the email
            self.mailbox.move(email_id, folder_path)
            return ToolResult(
                success=True,
                message=f"Moved email {email_id} to {folder_path}",
            )
        except Exception as e:
            return ToolResult(
                success=False,
                message=f"Failed to move email: {str(e)}",
            )

    async def create_folder(self, folder_path: str) -> ToolResult:
        """Create a folder if it doesn't exist.

        Args:
            folder_path: Folder path to create.

        Returns:
            ToolResult indicating success or failure.
        """
        try:
            # Check if folder exists
            folders = self.mailbox.folder.list()
            folder_names = [f.name for f in folders]

            if folder_path not in folder_names:
                self.mailbox.folder.create(folder_path)

            return ToolResult(
                success=True,
                message=f"Folder {folder_path} ready",
            )
        except Exception as e:
            return ToolResult(
                success=False,
                message=f"Failed to create folder: {str(e)}",
            )

    def _get_preview(self, msg, length: int = 200) -> str:
        """Get a preview of the email body.

        Args:
            msg: Message object from imap_tools.
            length: Length of preview (default 200 chars).

        Returns:
            Preview string.
        """
        body = msg.text or "(no body)"
        # Clean up whitespace
        body = " ".join(body.split())
        return body[:length] + ("..." if len(body) > length else "")


def register_email_tools(agent, email_tools_instance: EmailTools):
    """Register email classification and management tools with the agent.

    Args:
        agent: The pydantic-ai Agent instance to register tools with.
        email_tools_instance: An initialized EmailTools instance with active mailbox.
    """

    @agent.tool
    async def classify_and_move_email(
        ctx: RunContext, email_id: str, category: str, reasoning: str = ""
    ) -> Dict[str, Any]:
        """Classify an email and move it to a category-specific folder.

        This is the PRIMARY tool for organizing emails. After analyzing an email,
        use this tool to classify it, move it to the appropriate folder, and mark
        it as processed.

        Args:
            ctx: The PydanticAI runtime context.
            email_id: The unique identifier (UID) of the email to classify.
            category: The category name for this email. This will become the folder name.
                     Use lowercase with hyphens (e.g., "receipts", "work-urgent", "newsletters").
                     Examples: "receipts", "newsletters", "work-urgent", "social", "finance",
                              "travel", "promotions", "spam", "personal", etc.
            reasoning: A brief explanation of why you chose this category.

        Returns:
            Dict containing:
                - success (bool): True if email was successfully moved and tagged
                - message (str): Result message
                - category (str): The category the email was moved to
                - folder_path (str): The actual folder path created/used
                - tagged (bool): True if email was successfully marked as processed
                - error (str, optional): Error message if operation failed

        Examples:
            >>> # Classify a receipt email
            >>> result = classify_and_move_email(
            ...     ctx,
            ...     email_id="12345",
            ...     category="receipts",
            ...     reasoning="This is a purchase confirmation from Amazon with order details"
            ... )

            >>> # Classify a newsletter
            >>> result = classify_and_move_email(
            ...     ctx,
            ...     email_id="67890",
            ...     category="newsletters",
            ...     reasoning="Marketing email from a company newsletter subscription"
            ... )
        """
        from rich.console import Console

        console = Console()

        # Check if this is a dry run
        dry_run = ctx.deps.get("dry_run", False) if ctx.deps else False

        # Print tool call - clean banner style
        console.print("\n[bold yellow]TOOL CALL[/bold yellow]")
        console.print("[dim]" + "─" * 60 + "[/dim]")
        console.print("Tool: classify_and_move_email")
        console.print(f"Category: [cyan]{category}[/cyan]")
        if reasoning and len(reasoning) < 150:
            console.print(f"Reasoning: {reasoning}")
        elif reasoning:
            console.print(f"Reasoning: {reasoning[:150]}...")

        try:
            # Sanitize category name for folder use
            # Convert to lowercase, replace spaces with hyphens
            folder_name = category.lower().strip().replace(" ", "-")
            # Remove any special characters that might cause issues
            import re

            folder_name = re.sub(r"[^a-z0-9-]", "", folder_name)

            # Special handling for trash/spam - use Gmail special folders
            if folder_name in ["trash", "delete", "deleted"]:
                folder_name = "[Gmail]/Trash"
            elif folder_name == "spam":
                folder_name = "[Gmail]/Spam"

            if dry_run:
                # In dry-run mode, don't actually move the email or tag it
                action = "DELETE" if folder_name == "[Gmail]/Trash" else "move"
                return {
                    "success": True,
                    "message": f"[DRY RUN] Would classify as '{category}', {action} to folder '{folder_name}', and mark as processed",
                    "category": category,
                    "folder_path": folder_name,
                    "reasoning": reasoning,
                    "dry_run": True,
                }

            # Move email to the category folder
            result = await email_tools_instance.move_email_to_folder(
                email_id=email_id, folder_path=folder_name
            )

            if result.success:
                # CRITICAL: Mark email as classified by adding the agent tag as an IMAP keyword
                # This prevents re-processing the same email in future runs
                # The tag is searchable via IMAP search criteria (NOT keyword=TAG)
                tag_result = await email_tools_instance.add_label_to_email(
                    email_id=email_id, label=email_tools_instance.agent_tag
                )

                action = "DELETED" if folder_name == "[Gmail]/Trash" else "moved"
                return {
                    "success": True,
                    "message": f"Email classified as '{category}', {action} to folder '{folder_name}', and marked as CLASSIFIED",
                    "category": category,
                    "folder_path": folder_name,
                    "reasoning": reasoning,
                    "tagged": tag_result.success,
                }
            else:
                return {
                    "success": False,
                    "message": f"Failed to move email to '{folder_name}': {result.message}",
                    "category": category,
                    "folder_path": folder_name,
                    "error": result.message,
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"Error classifying email: {str(e)}",
                "category": category,
                "error": str(e),
            }

    @agent.tool
    async def mark_email_processed(ctx: RunContext, email_id: str) -> Dict[str, Any]:
        """Mark an email as processed by adding the agent tag.

        Use this after successfully classifying and moving an email to prevent
        re-processing it in future runs.

        Args:
            ctx: The PydanticAI runtime context.
            email_id: The unique identifier (UID) of the email.

        Returns:
            Dict containing success status and message.
        """
        try:
            result = await email_tools_instance.add_label_to_email(
                email_id=email_id, label=email_tools_instance.agent_tag
            )

            return result.model_dump()

        except Exception as e:
            return {
                "success": False,
                "message": f"Error marking email: {str(e)}",
                "error": str(e),
            }

    # Tools registered successfully (silent)
