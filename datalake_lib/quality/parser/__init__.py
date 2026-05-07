"""Parser YAML → DQConfig."""

from __future__ import annotations

from .yaml_parser import parse_all_dq_configs, parse_dq_config

__all__ = ["parse_all_dq_configs", "parse_dq_config"]
