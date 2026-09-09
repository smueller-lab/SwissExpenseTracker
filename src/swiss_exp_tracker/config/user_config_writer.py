from __future__ import annotations

from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

from swiss_exp_tracker.config.user_config_schema import CustomRule
from swiss_exp_tracker.config.user_config_schema import ReferenceIdCorrection

_CONFIG_DIR = Path(__file__).parent.parent / "pipeline_agentic" / "config"
DEFAULT_USER_CONFIG_PATH = _CONFIG_DIR / "user_config.yaml"

_yaml = YAML()
_yaml.preserve_quotes = True
_yaml.indent(mapping=2, sequence=4, offset=2)


def _custom_rule_mapping(rule: CustomRule) -> dict[str, Any]:
    """Render a CustomRule as a mapping with double-quoted strings, matching the file's hand-written style."""
    return {
        "merchant": DoubleQuotedScalarString(rule.merchant),
        "category_main": DoubleQuotedScalarString(rule.category_main),
        "category_second": DoubleQuotedScalarString(rule.category_second),
        "exact_match": rule.exact_match,
    }


def _reference_id_correction_mapping(
    correction: ReferenceIdCorrection,
) -> dict[str, Any]:
    """Render a ReferenceIdCorrection as a mapping with double-quoted strings; omits city when unset."""
    mapping: dict[str, Any] = {
        "reference_id": DoubleQuotedScalarString(correction.reference_id),
        "merchant": DoubleQuotedScalarString(correction.merchant),
        "category_main": DoubleQuotedScalarString(correction.category_main),
        "category_second": DoubleQuotedScalarString(correction.category_second),
    }
    if correction.city is not None:
        mapping["city"] = DoubleQuotedScalarString(correction.city)
    return mapping


def upsert_custom_rule(rule: CustomRule, path: Path = DEFAULT_USER_CONFIG_PATH) -> None:
    """Insert or replace a `custom_rules` entry keyed by exact-match merchant name, preserving comments/formatting."""
    data = _load(path)
    entries = data.setdefault("custom_rules", [])
    merchant_key = rule.merchant.lower()
    for i, entry in enumerate(entries):
        if (
            str(entry.get("merchant", "")).lower() == merchant_key
            and entry.get("exact_match", False) is True
        ):
            entries[i] = _custom_rule_mapping(rule)
            break
    else:
        entries.append(_custom_rule_mapping(rule))
    _dump(data, path)


def upsert_reference_id_correction(
    correction: ReferenceIdCorrection, path: Path = DEFAULT_USER_CONFIG_PATH
) -> None:
    """Insert or replace a `reference_id_corrections` entry keyed by reference_id, preserving comments/formatting."""
    data = _load(path)
    entries = data.setdefault("reference_id_corrections", [])
    for i, entry in enumerate(entries):
        if entry.get("reference_id") == correction.reference_id:
            entries[i] = _reference_id_correction_mapping(correction)
            break
    else:
        entries.append(_reference_id_correction_mapping(correction))
    _dump(data, path)


def _load(path: Path) -> dict[str, Any]:
    """Load user_config.yaml preserving comments/formatting; empty mapping if absent."""
    if not path.exists():
        return _yaml.load("{}")  # type: ignore[no-any-return]
    with path.open(encoding="utf-8") as f:
        data = _yaml.load(f)
    return data if data is not None else _yaml.load("{}")  # type: ignore[no-any-return]


def _dump(data: dict[str, Any], path: Path) -> None:
    """Write the round-trip-loaded data back to path, preserving comments/formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        _yaml.dump(data, f)
