"""Reticulum (LXMF) platform plugin for Hermes Agent.

The adapter lives in :mod:`adapter`; this module only re-exports the plugin
surface so the Hermes plugin loader can call ``register(ctx)``.
"""

from .adapter import (
    ReticulumAdapter,
    check_requirements,
    interactive_setup,
    is_connected,
    register,
    validate_config,
)

__all__ = [
    "ReticulumAdapter",
    "check_requirements",
    "interactive_setup",
    "is_connected",
    "register",
    "validate_config",
]
