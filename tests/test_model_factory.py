"""Tests for the model factory."""

import os
from unittest import mock

import pytest

from email_agent.model_factory import ModelFactory


class TestModelFactory:
    """Test ModelFactory class."""

    def test_list_models_returns_sorted_list(self):
        """Test that list_models returns a sorted list of model names."""
        config = {"model-z": {}, "model-a": {}, "model-m": {}}
        result = ModelFactory.list_models(config)
        assert result == ["model-a", "model-m", "model-z"]

    def test_validate_model_exists(self):
        """Test that validate_model returns True for existing model."""
        config = {"claude-3-opus": {}}
        assert ModelFactory.validate_model("claude-3-opus", config) is True

    def test_validate_model_not_exists(self):
        """Test that validate_model returns False for non-existent model."""
        config = {"claude-3-opus": {}}
        assert ModelFactory.validate_model("gpt-5", config) is False

    def test_get_model_anthropic(self):
        """Test creating an Anthropic model."""
        config = {
            "claude-3-opus": {
                "type": "anthropic",
                "name": "claude-3-opus-20250219",
            }
        }

        with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            model = ModelFactory.get_model("claude-3-opus", config)
            assert model is not None
            assert hasattr(model, "model_name")

    def test_get_model_openai(self):
        """Test creating an OpenAI model."""
        config = {
            "gpt-5": {
                "type": "openai",
                "name": "gpt-5",
            }
        }

        with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            model = ModelFactory.get_model("gpt-5", config)
            assert model is not None
            assert hasattr(model, "model_name")

    def test_get_model_not_found(self):
        """Test that get_model raises error for non-existent model."""
        config = {"claude-3-opus": {}}
        with pytest.raises(ValueError, match="Model 'nonexistent' not found"):
            ModelFactory.get_model("nonexistent", config)

    def test_get_model_unsupported_type(self):
        """Test that get_model raises error for unsupported model type."""
        config = {
            "unsupported-model": {
                "type": "unsupported_type",
                "name": "some-model",
            }
        }
        with pytest.raises(ValueError, match="Unsupported model type"):
            ModelFactory.get_model("unsupported-model", config)

    def test_get_model_missing_api_key_anthropic(self):
        """Test that get_model raises error when ANTHROPIC_API_KEY is missing."""
        config = {
            "claude-3-opus": {
                "type": "anthropic",
                "name": "claude-3-opus-20250219",
            }
        }

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
                ModelFactory.get_model("claude-3-opus", config)

    def test_get_model_missing_api_key_openai(self):
        """Test that get_model raises error when OPENAI_API_KEY is missing."""
        config = {
            "gpt-5": {
                "type": "openai",
                "name": "gpt-5",
            }
        }

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="OPENAI_API_KEY"):
                ModelFactory.get_model("gpt-5", config)

    def test_get_model_zai_coding(self):
        """Test creating a Zai Coding model."""
        config = {
            "zai-coding-model": {
                "type": "zai_coding",
                "name": "zai-coding-model",
            }
        }

        with mock.patch.dict(os.environ, {"ZAI_API_KEY": "test-zai-key"}):
            model = ModelFactory.get_model("zai-coding-model", config)
            assert model is not None
            assert hasattr(model, "model_name")

    def test_get_model_missing_api_key_zai_coding(self):
        """Test that get_model raises error when ZAI_API_KEY is missing."""
        config = {
            "zai-coding-model": {
                "type": "zai_coding",
                "name": "zai-coding-model",
            }
        }

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="ZAI_API_KEY"):
                ModelFactory.get_model("zai-coding-model", config)
