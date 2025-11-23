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


@pytest.mark.asyncio
async def test_get_unprocessed_emails_filters_classified(email_tools, mock_mailbox):
    """Test that get_unprocessed_emails correctly filters out classified emails."""
    # Mark email 1 as classified
    await email_tools.add_label_to_email("1", email_tools.agent_tag)
    
    # Get unprocessed emails
    result = await email_tools.get_unprocessed_emails(n=10)
    
    assert result.success
    email_ids = [email["email_id"] for email in result.data["emails"]]
    
    # Email 1 should be filtered out (it's classified)
    assert "1" not in email_ids
    # Emails 2 and 3 should still be present (unclassified)
    assert "2" in email_ids
    assert "3" in email_ids


@pytest.mark.asyncio
async def test_classification_workflow(email_tools, mock_mailbox):
    """Test full classification workflow: fetch unclassified -> classify -> verify filtered."""
    # Step 1: Get initial unprocessed count
    result1 = await email_tools.get_unprocessed_emails(n=10)
    initial_count = len(result1.data["emails"])
    assert initial_count == 3  # All 3 emails are unclassified
    
    # Step 2: Classify one email
    await email_tools.add_label_to_email("2", email_tools.agent_tag)
    
    # Step 3: Get unprocessed again - should be one fewer
    result2 = await email_tools.get_unprocessed_emails(n=10)
    new_count = len(result2.data["emails"])
    assert new_count == initial_count - 1  # One email was classified
    
    # Email 2 should not be in the results
    email_ids = [email["email_id"] for email in result2.data["emails"]]
    assert "2" not in email_ids
    
    # Step 4: Classify all remaining
    for email in result2.data["emails"]:
        await email_tools.add_label_to_email(email["email_id"], email_tools.agent_tag)
    
    # Step 5: Should get zero unprocessed emails
    result3 = await email_tools.get_unprocessed_emails(n=10)
    assert len(result3.data["emails"]) == 0


@pytest.mark.asyncio
async def test_descending_order_by_date(email_tools, mock_mailbox):
    """Test that unprocessed emails are returned in descending order by date (newest first)."""
    result = await email_tools.get_unprocessed_emails(n=10)
    
    assert result.success
    emails = result.data["emails"]
    
    # Due to reverse=True in fetch, newest should be first
    # In our mock, messages are reversed, so UID 3 should come first
    assert len(emails) >= 2
    # The mock reverses the list, so last message (uid=3) should be first
    assert emails[0]["email_id"] == "3"
