"""Tests for IMAP client singleton."""

import pytest

from email_agent.imap_client import IMAPClient, IMAPClientError


def test_imap_client_singleton():
    """Test that IMAPClient is a proper singleton."""
    # Get two instances without resetting
    client1 = IMAPClient()
    client2 = IMAPClient()

    assert client1 is client2
    assert id(client1) == id(client2)


def test_imap_client_not_connected_initially():
    """Test that client starts disconnected."""
    client = IMAPClient()
    assert not client.is_connected()


def test_imap_client_get_mailbox_without_connection():
    """Test that get_mailbox raises error when not connected."""
    client = IMAPClient()
    with pytest.raises(IMAPClientError, match="not connected"):
        client.get_mailbox()
