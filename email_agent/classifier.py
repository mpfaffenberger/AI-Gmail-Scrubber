"""Email classification system with AI-powered and rule-based decisions.

Provides intelligent email classification combining AI reasoning with
custom rules and confidence scoring.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from pydantic_ai import Agent

logger = logging.getLogger(__name__)


class DecisionAction(str, Enum):
    """Possible actions for an email decision."""

    KEEP = "keep"
    DELETE = "delete"
    ARCHIVE = "archive"
    FOLDER = "folder"
    READ = "read"
    FLAG = "flag"


@dataclass
class Decision:
    """Represents a classification decision for an email."""

    action: DecisionAction
    confidence: float  # 0.0 to 1.0
    reasoning: str
    tags: List[str] = field(default_factory=list)
    target_folder: Optional[str] = None  # For FOLDER action

    def __post_init__(self) -> None:
        """Validate decision fields."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be 0.0-1.0, got {self.confidence}")
        if self.action == DecisionAction.FOLDER and not self.target_folder:
            raise ValueError("FOLDER action requires target_folder")


class Rule:
    """A rule for email classification."""

    def __init__(
        self,
        name: str,
        predicate: Callable[[Dict[str, Any]], bool],
        action: DecisionAction,
        confidence: float = 1.0,
        target_folder: Optional[str] = None,
    ):
        """Initialize a classification rule.

        Args:
            name: Human-readable rule name.
            predicate: Function that takes email dict and returns bool.
            action: What to do if rule matches.
            confidence: How confident this rule is (0.0-1.0).
            target_folder: Optional folder for FOLDER actions.
        """
        self.name = name
        self.predicate = predicate
        self.action = action
        self.confidence = confidence
        self.target_folder = target_folder

    def matches(self, email: Dict[str, Any]) -> bool:
        """Check if email matches this rule."""
        try:
            return self.predicate(email)
        except Exception as e:
            logger.error(f"Error evaluating rule {self.name}: {e}")
            return False

    def apply(self) -> Decision:
        """Create a Decision from this rule."""
        return Decision(
            action=self.action,
            confidence=self.confidence,
            reasoning=f"Matched rule: {self.name}",
            tags=[f"rule:{self.name}"],
            target_folder=self.target_folder,
        )


class EmailClassifier:
    """Classifies emails using AI reasoning and rule-based filtering.

    Combines fast rule-based decisions with AI reasoning for complex cases.
    Rules are evaluated first (cheap), then AI is used if no rule matches.
    """

    SYSTEM_PROMPT = """You are an email classification expert. Analyze emails and make decisions.

Decision types:
- KEEP: Important emails that should stay in inbox
- ARCHIVE: Emails that are important but don't need immediate action
- DELETE: Spam, unwanted promotions, or irrelevant emails
- FLAG: Mark as important/needs action
- READ: Mark as read without moving
- FOLDER: Move to a specific folder (will be specified)

For each email, consider:
1. Sender reputation and importance
2. Content relevance and urgency
3. Subject line and keywords
4. Whether it matches typical spam patterns
5. If it's transactional (receipts, confirmations, etc.)

Be conservative: KEEP when in doubt, unless clearly spam.
Provide confidence 0.0-1.0 (higher = more certain).
Always explain your reasoning concisely.
"""

    def __init__(
        self,
        agent: Optional[Agent] = None,
        rules: Optional[List[Rule]] = None,
    ):
        """Initialize the classifier.

        Args:
            agent: Optional pydantic-ai Agent for AI decisions.
                  If None, only rule-based classification will work.
            rules: Optional list of Rule objects for rule-based classification.
        """
        self._agent = agent
        self._rules: Dict[str, Rule] = {}

        if rules:
            for rule in rules:
                self.add_rule(rule)

        logger.info(
            f"Initialized EmailClassifier with {len(self._rules)} rules "
            f"and {'AI agent' if agent else 'no AI agent'}"
        )

    def add_rule(self, rule: Rule) -> None:
        """Add a rule to the classifier.

        Args:
            rule: Rule object to add.
        """
        self._rules[rule.name] = rule
        logger.debug(f"Added rule: {rule.name}")

    def remove_rule(self, rule_name: str) -> bool:
        """Remove a rule from the classifier.

        Args:
            rule_name: Name of the rule to remove.

        Returns:
            True if rule was removed, False if not found.
        """
        if rule_name in self._rules:
            del self._rules[rule_name]
            logger.debug(f"Removed rule: {rule_name}")
            return True
        return False

    def get_rules(self) -> List[Rule]:
        """Get all registered rules.

        Returns:
            List of Rule objects.
        """
        return list(self._rules.values())

    def classify(self, email: Dict[str, Any]) -> Decision:
        """Classify an email based on rules and optionally AI.

        First checks rule-based classification, then falls back to AI if available
        and no high-confidence rule matches.

        Args:
            email: Email content dict with keys like subject, from_addr, body, etc.

        Returns:
            Decision object with action, confidence, reasoning, and tags.
        """
        # Try rule-based classification first
        rule_decision = self._classify_with_rules(email)
        if rule_decision and rule_decision.confidence > 0.9:
            logger.debug(
                f"Rule matched with high confidence: {rule_decision.reasoning}"
            )
            return rule_decision

        # Fall back to AI if available
        if self._agent:
            logger.debug("No high-confidence rule match, using AI for classification")
            ai_decision = self._classify_with_ai(email)
            if ai_decision:
                # If we had a lower-confidence rule, combine them
                if rule_decision:
                    ai_decision.tags.append(
                        f"rule_confidence:{rule_decision.confidence:.2f}"
                    )
                return ai_decision

        # Default: keep the email if can't classify
        if rule_decision:
            return rule_decision

        return Decision(
            action=DecisionAction.KEEP,
            confidence=0.5,
            reasoning="No rule matched and no AI available, defaulting to KEEP",
            tags=["default"],
        )

    def _classify_with_rules(self, email: Dict[str, Any]) -> Optional[Decision]:
        """Apply rule-based classification.

        Args:
            email: Email content dict.

        Returns:
            Decision from the first matching rule, or None.
        """
        for rule in self._rules.values():
            if rule.matches(email):
                logger.debug(f"Rule matched: {rule.name}")
                return rule.apply()

        return None

    def _classify_with_ai(self, email: Dict[str, Any]) -> Optional[Decision]:
        """Apply AI-based classification.

        Args:
            email: Email content dict.

        Returns:
            Decision from AI, or None if not available.
        """
        if not self._agent:
            return None

        try:
            # Format email for the AI
            email_text = self._format_email_for_ai(email)

            # This would normally be async, but for now we'll handle it
            # In actual usage, the agent would be called from async context
            prompt = f"""Classify this email and respond with:
1. ACTION: one of KEEP, DELETE, ARCHIVE, FLAG, READ, or FOLDER
2. CONFIDENCE: 0.0-1.0
3. REASONING: brief explanation
4. TAGS: comma-separated tags

Email:
{email_text}

Respond in format:
ACTION: ...
CONFIDENCE: ...
REASONING: ...
TAGS: ...
"""

            # For now, return None - actual implementation would use async agent
            # This is a placeholder for the async integration
            logger.debug("AI classification requested but not fully implemented")
            return None

        except Exception as e:
            logger.error(f"Error in AI classification: {e}")
            return None

    @staticmethod
    def _format_email_for_ai(email: Dict[str, Any]) -> str:
        """Format email dict as text for AI processing.

        Args:
            email: Email content dict.

        Returns:
            Formatted email text.
        """
        parts = [
            f"Subject: {email.get('subject', 'N/A')}",
            f"From: {email.get('from_addr', 'N/A')}",
            f"To: {email.get('to_addr', 'N/A')}",
            f"Date: {email.get('date', 'N/A')}",
            f"\nBody:\n{email.get('body', 'N/A')[:1000]}",
        ]
        return "\n".join(parts)

    @staticmethod
    def create_simple_rules() -> List[Rule]:
        """Create a set of simple default rules.

        Returns:
            List of basic Rule objects for common cases.
        """
        rules = [
            # Delete obvious spam
            Rule(
                name="obvious_spam",
                predicate=lambda e: any(
                    kw in e.get("body", "").lower()
                    for kw in ["unsubscribe", "click here now", "limited time offer"]
                ),
                action=DecisionAction.DELETE,
                confidence=0.95,
            ),
            # Keep from trusted senders
            Rule(
                name="trusted_sender_keep",
                predicate=lambda e: e.get("from_addr", "").endswith("@company.com")
                or "mom" in e.get("from_addr", "").lower(),
                action=DecisionAction.KEEP,
                confidence=0.95,
            ),
            # Archive receipts and confirmations
            Rule(
                name="transactional_archive",
                predicate=lambda e: any(
                    kw in e.get("subject", "").lower()
                    for kw in ["receipt", "confirmation", "order", "invoice"]
                ),
                action=DecisionAction.ARCHIVE,
                confidence=0.85,
            ),
        ]
        return rules
