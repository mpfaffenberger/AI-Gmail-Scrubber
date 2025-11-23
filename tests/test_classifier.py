"""Tests for email classifier."""

import pytest

from email_agent.classifier import (
    Decision,
    DecisionAction,
    EmailClassifier,
    Rule,
)


class TestDecision:
    """Test Decision dataclass."""

    def test_create_valid_decision(self):
        """Test creating a valid decision."""
        decision = Decision(
            action=DecisionAction.KEEP,
            confidence=0.95,
            reasoning="Important email",
            tags=["work", "important"],
        )

        assert decision.action == DecisionAction.KEEP
        assert decision.confidence == 0.95
        assert decision.reasoning == "Important email"
        assert "work" in decision.tags

    def test_invalid_confidence_too_high(self):
        """Test that confidence > 1.0 raises error."""
        with pytest.raises(ValueError, match="Confidence must be 0.0-1.0"):
            Decision(
                action=DecisionAction.KEEP,
                confidence=1.5,
                reasoning="test",
            )

    def test_invalid_confidence_negative(self):
        """Test that negative confidence raises error."""
        with pytest.raises(ValueError, match="Confidence must be 0.0-1.0"):
            Decision(
                action=DecisionAction.KEEP,
                confidence=-0.1,
                reasoning="test",
            )

    def test_folder_action_requires_target_folder(self):
        """Test that FOLDER action without target raises error."""
        with pytest.raises(ValueError, match="FOLDER action requires target_folder"):
            Decision(
                action=DecisionAction.FOLDER,
                confidence=0.9,
                reasoning="test",
            )

    def test_folder_action_with_target(self):
        """Test FOLDER action with target folder."""
        decision = Decision(
            action=DecisionAction.FOLDER,
            confidence=0.9,
            reasoning="test",
            target_folder="Archives/Receipts",
        )
        assert decision.target_folder == "Archives/Receipts"


class TestRule:
    """Test Rule class."""

    def test_create_simple_rule(self):
        """Test creating a simple rule."""
        rule = Rule(
            name="test_rule",
            predicate=lambda e: True,
            action=DecisionAction.KEEP,
        )

        assert rule.name == "test_rule"
        assert rule.action == DecisionAction.KEEP

    def test_rule_matches(self):
        """Test rule matching."""
        rule = Rule(
            name="spam_check",
            predicate=lambda e: "unsubscribe" in e.get("body", "").lower(),
            action=DecisionAction.DELETE,
        )

        spam_email = {"body": "Click here to unsubscribe"}
        normal_email = {"body": "Hello there"}

        assert rule.matches(spam_email) is True
        assert rule.matches(normal_email) is False

    def test_rule_apply(self):
        """Test rule application to create decision."""
        rule = Rule(
            name="test_rule",
            predicate=lambda e: True,
            action=DecisionAction.ARCHIVE,
            confidence=0.85,
        )

        decision = rule.apply()

        assert decision.action == DecisionAction.ARCHIVE
        assert decision.confidence == 0.85
        assert "test_rule" in decision.reasoning
        assert "rule:test_rule" in decision.tags

    def test_rule_with_folder_action(self):
        """Test rule with FOLDER action."""
        rule = Rule(
            name="archive_receipts",
            predicate=lambda e: "receipt" in e.get("subject", "").lower(),
            action=DecisionAction.FOLDER,
            target_folder="Receipts",
        )

        decision = rule.apply()
        assert decision.target_folder == "Receipts"

    def test_rule_with_exception_in_predicate(self):
        """Test rule handles exceptions in predicate gracefully."""

        def bad_predicate(e):
            raise ValueError("Oops")

        rule = Rule(
            name="bad_rule",
            predicate=bad_predicate,
            action=DecisionAction.KEEP,
        )

        # Should return False instead of raising
        assert rule.matches({}) is False


class TestEmailClassifier:
    """Test EmailClassifier class."""

    def test_create_classifier_no_rules(self):
        """Test creating classifier without rules."""
        classifier = EmailClassifier()
        assert classifier.get_rules() == []

    def test_create_classifier_with_rules(self):
        """Test creating classifier with initial rules."""
        rules = [
            Rule(
                name="rule1",
                predicate=lambda e: True,
                action=DecisionAction.KEEP,
            ),
        ]
        classifier = EmailClassifier(rules=rules)
        assert len(classifier.get_rules()) == 1

    def test_add_rule(self):
        """Test adding rules to classifier."""
        classifier = EmailClassifier()

        rule = Rule(
            name="test",
            predicate=lambda e: True,
            action=DecisionAction.KEEP,
        )
        classifier.add_rule(rule)

        assert len(classifier.get_rules()) == 1

    def test_remove_rule(self):
        """Test removing rules."""
        classifier = EmailClassifier()
        rule = Rule(
            name="test",
            predicate=lambda e: True,
            action=DecisionAction.KEEP,
        )
        classifier.add_rule(rule)

        assert classifier.remove_rule("test") is True
        assert classifier.remove_rule("test") is False
        assert len(classifier.get_rules()) == 0

    def test_classify_with_matching_rule(self):
        """Test classification with matching rule."""
        classifier = EmailClassifier()
        classifier.add_rule(
            Rule(
                name="keep_trusted",
                predicate=lambda e: e.get("from_addr") == "boss@company.com",
                action=DecisionAction.KEEP,
                confidence=0.95,
            )
        )

        email = {"from_addr": "boss@company.com", "body": "Meeting tomorrow"}
        decision = classifier.classify(email)

        assert decision.action == DecisionAction.KEEP
        assert decision.confidence == 0.95

    def test_classify_no_match_defaults_to_keep(self):
        """Test that unmatched emails default to KEEP."""
        classifier = EmailClassifier()
        email = {"from_addr": "unknown@example.com", "body": "Some content"}

        decision = classifier.classify(email)

        assert decision.action == DecisionAction.KEEP
        assert "default" in decision.tags

    def test_format_email_for_ai(self):
        """Test email formatting for AI."""
        email = {
            "subject": "Test Subject",
            "from_addr": "test@example.com",
            "to_addr": "recipient@example.com",
            "date": "2025-01-01",
            "body": "This is the email body",
        }

        formatted = EmailClassifier._format_email_for_ai(email)

        assert "Test Subject" in formatted
        assert "test@example.com" in formatted
        assert "This is the email body" in formatted

    def test_create_simple_rules(self):
        """Test creating simple default rules."""
        rules = EmailClassifier.create_simple_rules()

        assert len(rules) > 0
        assert all(isinstance(r, Rule) for r in rules)

        # Test that obvious spam rule works
        spam_rule = next(r for r in rules if "spam" in r.name)
        spam_email = {"body": "Click here now for limited time offer"}

        assert spam_rule.matches(spam_email)

    def test_classify_with_high_confidence_rule(self):
        """Test that high confidence rules prevent AI fallback."""
        classifier = EmailClassifier()
        classifier.add_rule(
            Rule(
                name="obvious_spam",
                predicate=lambda e: "unsubscribe" in e.get("body", "").lower(),
                action=DecisionAction.DELETE,
                confidence=0.95,
            )
        )

        email = {"body": "Unsubscribe from our mailing list"}
        decision = classifier.classify(email)

        assert decision.action == DecisionAction.DELETE
        assert decision.confidence == 0.95
