"""Email Agent - An AI-powered email classification and management agent.

A modern pydantic-ai based agent for intelligent email management using multiple AI providers.
"""

from email_agent.agent import EmailAgent, EmailAgentError
from email_agent.classifier import Decision, DecisionAction, EmailClassifier, Rule
from email_agent.config import Config, init_config_file
from email_agent.exceptions import (
    ConfigurationError,
    IMAPConnectionError,
    AuthenticationError,
    ClassificationError,
    DecisionApplyError,
    StateError,
    ProcessingError,
)
from email_agent.imap_client import IMAPClient, IMAPClientError
from code_puppy.model_factory import ModelFactory
from email_agent.orchestrator import EmailOrchestrator, OrchestratorError
from email_agent.security import (
    sanitize_error_message,
    validate_config_permissions,
    setup_secure_files,
    is_credentials_safe,
)
from email_agent.state import StateManager
from email_agent.tools.email_tools import EmailTools, ToolResult
from email_agent.tools.reasoning import ReasoningTools

__version__ = "0.1.0"
__author__ = "Michael Pfaffenberger"
__license__ = "MIT"

__all__ = [
    # Agent and orchestration
    "EmailAgent",
    "EmailAgentError",
    "EmailOrchestrator",
    "OrchestratorError",
    # Classification
    "EmailClassifier",
    "Decision",
    "DecisionAction",
    "Rule",
    # Configuration
    "Config",
    "init_config_file",
    # Exceptions
    "ConfigurationError",
    "IMAPConnectionError",
    "AuthenticationError",
    "ClassificationError",
    "DecisionApplyError",
    "StateError",
    "ProcessingError",
    # IMAP and connectivity
    "IMAPClient",
    "IMAPClientError",
    # Models and tools
    "ModelFactory",
    "EmailTools",
    "ToolResult",
    "ReasoningTools",
    # Security
    "sanitize_error_message",
    "validate_config_permissions",
    "setup_secure_files",
    "is_credentials_safe",
    # State management
    "StateManager",
]
