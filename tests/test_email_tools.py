"""Tests for email tools."""

import pytest

from email_agent.tools.email_tools import EmailTools, ToolResult


@pytest.fixture
def email_tools(mock_mailbox):
    """Fixture that provides EmailTools with mock mailbox."""
    return EmailTools(mock_mailbox)


@pytest.mark.asyncio
async def test_get_unprocessed_emails(email_tools):
    """Test getting unprocessed emails."""
    result = await email_tools.get_unprocessed_emails(n=2)

    assert result.success
    assert len(result.data["emails"]) <= 2
    assert all("email_id" in email for email in result.data["emails"])
    assert all("subject" in email for email in result.data["emails"])


@pytest.mark.asyncio
async def test_get_email_content(email_tools):
    """Test getting full email content."""
    result = await email_tools.get_email_content("1")

    assert result.success
    assert "content" in result.data
    content = result.data["content"]
    assert content["email_id"] == "1"
    assert content["subject"] == "Newsletter from Company"


@pytest.mark.asyncio
async def test_get_email_content_not_found(email_tools):
    """Test getting content for non-existent email."""
    result = await email_tools.get_email_content("999")

    assert not result.success


@pytest.mark.asyncio
async def test_mark_email_as_read(email_tools):
    """Test marking email as read."""
    result = await email_tools.mark_email_as_read("1")

    assert result.success
    assert "read" in result.message.lower()


@pytest.mark.asyncio
async def test_add_label_to_email(email_tools):
    """Test adding a label to email."""
    result = await email_tools.add_label_to_email("1", "PROCESSED")

    assert result.success
    assert "label" in result.message.lower()


@pytest.mark.asyncio
async def test_create_folder(email_tools):
    """Test creating a folder."""
    result = await email_tools.create_folder("Archive")

    assert result.success


@pytest.mark.asyncio
async def test_move_email_to_folder(email_tools):
    """Test moving an email to a folder."""
    result = await email_tools.move_email_to_folder("1", "Archive")

    assert result.success


def test_tool_result_model():
    """Test ToolResult model validation."""
    result = ToolResult(
        success=True,
        message="Operation successful",
        data={"key": "value"},
    )

    assert result.success
    assert result.message == "Operation successful"
    assert result.data["key"] == "value"
