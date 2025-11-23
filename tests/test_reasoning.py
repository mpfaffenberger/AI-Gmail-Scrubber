"""Tests for reasoning tools."""

import pytest

from email_agent.tools.reasoning import ReasoningTools, ReasoningResult


@pytest.fixture
def reasoning_tools():
    """Fixture that provides ReasoningTools."""
    return ReasoningTools()


@pytest.mark.asyncio
async def test_reason_about_email(reasoning_tools):
    """Test reasoning about an email."""
    result = await reasoning_tools.reason_about_email(
        reasoning="This is a test email",
        next_steps="Mark as read",
    )

    assert result.success
    assert result.message == "Reasoning shared"


@pytest.mark.asyncio
async def test_reason_about_email_without_next_steps(reasoning_tools):
    """Test reasoning without next steps."""
    result = await reasoning_tools.reason_about_email(
        reasoning="This email looks important",
    )

    assert result.success


@pytest.mark.asyncio
async def test_log_decision(reasoning_tools):
    """Test logging a decision."""
    result = await reasoning_tools.log_decision(
        email_subject="Test Subject",
        decision="ARCHIVE",
        reason="Newsletter, not urgent",
    )

    assert result.success
    assert result.message == "Decision logged"


@pytest.mark.asyncio
async def test_log_decision_without_reason(reasoning_tools):
    """Test logging a decision without reason."""
    result = await reasoning_tools.log_decision(
        email_subject="Test",
        decision="DELETE",
    )

    assert result.success


def test_reasoning_result_model():
    """Test ReasoningResult model validation."""
    result = ReasoningResult(
        success=True,
        message="Test message",
    )

    assert result.success
    assert result.message == "Test message"
