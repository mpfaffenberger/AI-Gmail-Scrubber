"""Core Email Agent implementation using pydantic-ai.

Provides the main agent class that coordinates email processing using
multiple AI models and integrated tools.
"""

import logging
from typing import Any, Optional

from pydantic_ai import Agent, ModelRetry
from rich.console import Console

from email_agent.imap_client import IMAPClient
from code_puppy.model_factory import ModelFactory
from email_agent.tools.email_tools import EmailTools
from email_agent.tools.reasoning import ReasoningTools

logger = logging.getLogger(__name__)
console = Console()


class EmailAgentError(Exception):
    """Base exception for email agent errors."""

    pass


class EmailAgent:
    """Main email agent using pydantic-ai.

    Coordinates email processing across multiple AI models with integrated
    tools for email manipulation, reasoning, and decision-making.

    Example:
        agent = EmailAgent(model_name='claude-3-5-sonnet')
        result = await agent.process_email(email_id='12345')
    """

    SYSTEM_PROMPT = """You are an intelligent email management assistant. Your job is to:

1. Read and understand incoming emails
2. Analyze them based on content, sender, subject, and context
3. Classify each email into a meaningful category of YOUR choice

CATEGORY GUIDELINES:

**DELETE IMMEDIATELY (use category "trash"):**
- Promotional emails, sales offers, marketing campaigns
- Commercial advertisements and deals
- Unwanted newsletters and subscriptions
- Spam, phishing attempts, or suspicious emails
- Anything that looks like mass marketing

**ORGANIZE into specific categories:**
- "receipts" - purchase confirmations, invoices, order confirmations
- "work" - work-related emails, professional communications
- "school" - educational emails, teacher communications, school notifications
- "social" - social media notifications (LinkedIn, Facebook, etc.)
- "finance" - bank statements, financial updates, account notifications
- "travel" - flight confirmations, hotel bookings, travel itineraries
- "personal" - personal correspondence from friends/family
- Or ANY other specific category that makes sense!

4. Use the classify_and_move_email tool to move the email to the appropriate folder
5. Provide clear reasoning for your classification decision

IMPORTANT:
- BE AGGRESSIVE about deleting promotional/marketing content - most people don't need it
- If it looks like marketing or a promotion, classify as "trash"
- Use lowercase with hyphens for categories (e.g., "work-urgent", "receipts")
- Be specific and consistent with your categories
- The classify_and_move_email tool will automatically mark the email as processed
- When you classify as "trash", the email will be permanently deleted

Always use the classify_and_move_email tool to move and mark the email.
"""

    def __init__(
        self,
        model_name: str,
        model_config: Optional[dict[str, Any]] = None,
        agent_tag: str = "EMAIL_AGENT_PROCESSED",
    ):
        """Initialize the email agent.

        Args:
            model_name: Name of the model to use (e.g., 'claude-3-5-sonnet').
            model_config: Optional model configuration dict.
            agent_tag: Tag to mark emails processed by this agent.

        Raises:
            EmailAgentError: If model initialization fails.
        """
        self.model_name = model_name
        self.agent_tag = agent_tag
        self.email_tools: Optional[EmailTools] = None
        self.reasoning_tools = ReasoningTools()

        # Initialize the pydantic-ai agent with the model
        try:
            config = model_config or ModelFactory.load_config()
            model = ModelFactory.get_model(model_name, config)
            self._agent = Agent(
                model=model,
                system_prompt=self.SYSTEM_PROMPT,
                retries=2,
            )
        except Exception as e:
            logger.error(f"Failed to initialize agent: {str(e)}")
            raise EmailAgentError(f"Failed to initialize agent: {str(e)}") from e

    async def initialize_tools(self) -> None:
        """Initialize the email tools with active IMAP connection.

        Must be called after agent is created and IMAP is connected.

        Raises:
            EmailAgentError: If no active IMAP connection.
        """
        try:
            imap_client = IMAPClient()

            if not imap_client.is_connected():
                raise EmailAgentError(
                    "IMAP client not connected. Call IMAPClient.connect() first."
                )

            mailbox = imap_client.get_mailbox()
            self.email_tools = EmailTools(mailbox, agent_tag=self.agent_tag)

            # Register email tools with the agent so it can use them directly
            from email_agent.tools.email_tools import register_email_tools

            register_email_tools(self._agent, self.email_tools)

        except EmailAgentError:
            raise
        except Exception as e:
            raise EmailAgentError(f"Failed to initialize email tools: {str(e)}") from e

    async def process_email(
        self, email_id: str, dry_run: bool = False
    ) -> dict[str, Any]:
        """Process a single email.

        Args:
            email_id: Unique email identifier.
            dry_run: If True, don't actually modify emails.

        Returns:
            Dict with processing result and decisions.

        Raises:
            EmailAgentError: If email processing fails.
        """
        if self.email_tools is None:
            raise EmailAgentError(
                "Tools not initialized. Call initialize_tools() first."
            )

        try:
            # Get email content
            from rich.console import Console

            console_agent = Console()

            content_result = await self.email_tools.get_email_content(email_id)
            if not content_result.success:
                return {
                    "email_id": email_id,
                    "success": False,
                    "error": content_result.message,
                }

            email_content = content_result.data["content"]

            # Build prompt
            prompt = f"""Please analyze and classify this email:

Subject: {email_content["subject"]}
From: {email_content["from_addr"]}
To: {email_content["to_addr"]}
Date: {email_content["date"]}

Body:
{email_content["body"][:1000]}{"..." if len(email_content["body"]) > 1000 else ""}

Analyze this email and determine the best category for organizing it.
Think about what type of email this is and choose an appropriate category name.
Then use the classify_and_move_email tool to move it to the right folder.

Email ID for the tool: {email_id}"""

            # Print the email input - clean banner style
            console_agent.print("\n[bold blue]EMAIL INPUT[/bold blue]")
            console_agent.print("[dim]" + "─" * 60 + "[/dim]")
            console_agent.print(f"Subject: {email_content['subject']}")
            console_agent.print(f"From: {email_content['from_addr']}")
            console_agent.print(f"Date: {email_content['date']}")

            # Run the agent
            response = await self._agent.run(
                user_prompt=prompt,
                deps={
                    "email_id": email_id,
                    "dry_run": dry_run,
                },
            )

            # Extract category from tool calls by checking all messages
            category = "unknown"
            tool_success = False

            # pydantic-ai stores messages in all_messages (not _all_messages)
            if hasattr(response, "all_messages"):
                for msg in response.all_messages():
                    if hasattr(msg, "parts"):
                        for part in msg.parts:
                            # Check if this part is a tool call
                            if (
                                hasattr(part, "tool_name")
                                and part.tool_name == "classify_and_move_email"
                            ):
                                # Try different ways to get args
                                if hasattr(part, "args_as_dict"):
                                    args = part.args_as_dict()
                                elif hasattr(part, "args"):
                                    # args might be a Pydantic model, not a dict
                                    args = (
                                        part.args
                                        if isinstance(part.args, dict)
                                        else (
                                            part.args.model_dump()
                                            if hasattr(part.args, "model_dump")
                                            else {}
                                        )
                                    )
                                else:
                                    args = {}
                                if "category" in args:
                                    category = args.get("category", "unknown")
                                    tool_success = True  # Tool was called
                            # Check if this part is a tool return
                            elif hasattr(part, "content") and isinstance(
                                part.content, str
                            ):
                                # Sometimes the return is in content as JSON
                                try:
                                    import json

                                    tool_return = json.loads(part.content)
                                    if (
                                        isinstance(tool_return, dict)
                                        and "category" in tool_return
                                    ):
                                        category = tool_return["category"]
                                        tool_success = tool_return.get("success", False)
                                except:
                                    pass

            # Get final output
            response_data = (
                str(response.data) if hasattr(response, "data") else str(response)
            )

            # Print result - clean banner style
            console_agent.print("\n[bold green]RESULT[/bold green]")
            console_agent.print("[dim]" + "─" * 60 + "[/dim]")
            console_agent.print(f"Category: [cyan]{category}[/cyan]")
            console_agent.print(
                f"Status: {'[green]✓ Success[/green]' if tool_success else '[red]✗ Failed[/red]'}"
            )

            result = {
                "email_id": email_id,
                "success": tool_success,
                "subject": email_content["subject"],
                "decision": category,  # Keep 'decision' key for backwards compatibility
                "category": category,
                "reasoning": response_data,
            }
            return result

        except ModelRetry:
            return {
                "email_id": email_id,
                "success": False,
                "error": "Model retry exhausted",
            }
        except Exception as e:
            logger.error(f"Error processing email {email_id}: {str(e)}")
            return {
                "email_id": email_id,
                "success": False,
                "error": str(e),
            }

    async def process_batch(
        self,
        email_ids: list[str],
        dry_run: bool = False,
        show_progress: bool = True,
    ) -> list[dict[str, Any]]:
        """Process a batch of emails.

        Args:
            email_ids: List of email IDs to process.
            dry_run: If True, don't actually modify emails.
            show_progress: If True, show progress bar.

        Returns:
            List of processing results.
        """
        results = []
        successful = 0
        failed = 0

        if show_progress:
            from rich.progress import Progress

            with Progress() as progress:
                task = progress.add_task(
                    "[cyan]Processing emails...",
                    total=len(email_ids),
                )

                for email_id in email_ids:
                    result = await self.process_email(email_id, dry_run=dry_run)
                    results.append(result)

                    if result.get("success"):
                        successful += 1
                    else:
                        failed += 1

                    progress.update(task, advance=1)
        else:
            for email_id in email_ids:
                result = await self.process_email(email_id, dry_run=dry_run)
                results.append(result)

                if result.get("success"):
                    successful += 1
                else:
                    failed += 1

        return results

    @staticmethod
    def _parse_decision(response_data: Any) -> str:
        """Extract decision from agent response.

        Args:
            response_data: The response from the agent.

        Returns:
            The decision (KEEP, ARCHIVE, DELETE, or SPAM).
        """
        logger.debug(
            f"[AGENT] _parse_decision() called with response type: {type(response_data).__name__}"
        )

        if isinstance(response_data, str):
            # Look for explicit decision statements first
            import re

            # Pattern 1: "decision": "KEEP" (JSON-like)
            json_match = re.search(
                r'"decision"\s*:\s*"(KEEP|ARCHIVE|DELETE|SPAM)"',
                response_data,
                re.IGNORECASE,
            )
            if json_match:
                decision = json_match.group(1).upper()
                logger.debug(f"[AGENT] Found JSON decision: '{decision}'")
                return decision

            # Pattern 2: "I should KEEP/ARCHIVE/DELETE/SPAM this email"
            action_match = re.search(
                r"I should (KEEP|ARCHIVE|DELETE|SPAM) this",
                response_data,
                re.IGNORECASE,
            )
            if action_match:
                decision = action_match.group(1).upper()
                logger.debug(f"[AGENT] Found action statement decision: '{decision}'")
                return decision

            # Pattern 3: "Action: KEEP" or "Decision: KEEP"
            explicit_match = re.search(
                r"(?:action|decision)\s*:\s*(KEEP|ARCHIVE|DELETE|SPAM)",
                response_data,
                re.IGNORECASE,
            )
            if explicit_match:
                decision = explicit_match.group(1).upper()
                logger.debug(f"[AGENT] Found explicit decision: '{decision}'")
                return decision

            # Fallback: search for keywords (but prioritize in order: DELETE, ARCHIVE, SPAM, KEEP)
            # This order matters because "spam" might appear in context like "not spam"
            data_lower = response_data.lower()
            logger.debug(
                "[AGENT] No explicit decision found, searching for keywords..."
            )
            for decision in ["delete", "archive", "keep", "spam"]:
                if decision in data_lower:
                    logger.debug(f"[AGENT] Found decision keyword: '{decision}'")
                    return decision.upper()

        logger.debug("[AGENT] No decision keyword found, defaulting to KEEP")
        return "KEEP"  # Default to keeping when unsure
