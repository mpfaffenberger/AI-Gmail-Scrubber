"""Pytest configuration and shared fixtures.

Provides mock IMAP connections and test utilities.
"""

import asyncio
from unittest.mock import Mock

import pytest


class MockMessage:
    """Mock imap_tools Message object."""

    def __init__(
        self,
        uid: str = "123",
        subject: str = "Test Email",
        from_: str = "sender@example.com",
        to: str = "recipient@example.com",
        text: str = "Test body",
        flags: list = None,
    ):
        self.uid = uid
        self.subject = subject
        self.from_ = from_
        self.to = to
        self.text = text
        self.flags = flags or []
        self.date = "2024-01-15 10:00:00"


class MockMailBox:
    """Mock imap_tools MailBox object."""

    def __init__(self):
        self.messages = []
        self.folders = {}
        self.flagged = {}

    def fetch(self, criteria=None, uid=None, mark_seen=False, reverse=False, limit=None):
        """Mock fetch method with support for search criteria."""
        messages = self.messages[:]
        
        # Handle UID-based fetch
        if uid is not None:
            return [msg for msg in messages if msg.uid == uid]
        
        # Handle search criteria (simplified for testing)
        if criteria is not None:
            # Check if it's a NOT(keyword=...) criteria
            criteria_str = str(criteria)
            if "NOT" in criteria_str and "keyword" in criteria_str:
                # Extract the keyword being searched for
                # For NOT(keyword='TAG'), filter out messages with that keyword in flags
                filtered = []
                for msg in messages:
                    # Check if message has been flagged with any keywords
                    msg_keywords = self.flagged.get(msg.uid, [])
                    # If the criteria is NOT keyword, include messages WITHOUT that keyword
                    has_keyword = any(kw in msg_keywords for kw in ["EMAIL_AGENT_PROCESSED"])
                    if not has_keyword:
                        filtered.append(msg)
                messages = filtered
        
        # Apply reverse (newest first)
        if reverse:
            messages = list(reversed(messages))
        
        # Apply limit
        if limit is not None:
            messages = messages[:limit]
        
        return messages

    def flag(self, uids, flag_set, value):
        """Mock flag method for adding/removing IMAP keywords."""
        # Handle both single UID string and list of UIDs
        uid_list = uids if isinstance(uids, list) else [uids]
        
        for uid in uid_list:
            if uid not in self.flagged:
                self.flagged[uid] = []
            
            if value:
                # Add flags/keywords
                for flag in flag_set:
                    if flag not in self.flagged[uid]:
                        self.flagged[uid].append(flag)
            else:
                # Remove flags/keywords
                self.flagged[uid] = [f for f in self.flagged[uid] if f not in flag_set]

    def move(self, uid, folder):
        """Mock move method."""
        self.messages = [msg for msg in self.messages if msg.uid != uid]

    @property
    def folder(self):
        """Mock folder property."""
        return self

    def list(self):
        """Mock folder list method."""
        return [Mock(name=name) for name in self.folders.keys()]

    def create(self, folder_name):
        """Mock create folder method."""
        self.folders[folder_name] = True

    def logout(self):
        """Mock logout method."""
        pass


@pytest.fixture
def mock_mailbox():
    """Fixture that provides a mock MailBox."""
    mailbox = MockMailBox()

    # Add some test messages
    mailbox.messages = [
        MockMessage(
            uid="1",
            subject="Newsletter from Company",
            from_="newsletter@company.com",
            text="This month's news...",
        ),
        MockMessage(
            uid="2",
            subject="Special Offer!",
            from_="promo@shop.com",
            text="50% off everything!",
        ),
        MockMessage(
            uid="3",
            subject="Team Meeting Notes",
            from_="manager@work.com",
            text="Here are the meeting notes...",
        ),
    ]

    return mailbox


@pytest.fixture
def temp_state_file(tmp_path):
    """Fixture that provides a temporary state file path."""
    return tmp_path / "state.json"


@pytest.fixture
def event_loop():
    """Fixture that provides an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
