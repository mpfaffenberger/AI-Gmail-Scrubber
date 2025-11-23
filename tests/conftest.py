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

    def fetch(self, uid=None, mark_seen=False):
        """Mock fetch method."""
        if uid is None:
            return self.messages
        return [msg for msg in self.messages if msg.uid == uid]

    def flag(self, uids, flag_set, value):
        """Mock flag method."""
        for uid in uids if isinstance(uids, list) else [uids]:
            if uid not in self.flagged:
                self.flagged[uid] = []
            if value:
                if flag_set[0] not in self.flagged[uid]:
                    self.flagged[uid].extend(flag_set)
            else:
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
