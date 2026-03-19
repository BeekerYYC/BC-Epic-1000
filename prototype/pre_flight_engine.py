#!/usr/bin/env python3
"""
Mangrove Pre-Flight Engine
==========================
Validates carbon removal project data against registry methodology rules
BEFORE submission to verifiers.

Usage:
    python3 pre_flight_engine.py --rules <rules.json> --payload <payload.json>
    python3 pre_flight_engine.py --rules <rules.json> --payload <payload.json> --verbose
    python3 pre_flight_engine.py --rules <rules.json> --payload <payload.json> --json

Zero external dependencies. Python 3.9+.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional


# ---------------------------------------------------------------------------
# ANSI colour & box-drawing helpers
# ---------------------------------------------------------------------------

class Style:
    """ANSI escape codes for terminal styling."""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"

    # Box-drawing
    H_LINE = "\u2500"
    V_LINE = "\u2502"
    TL = "\u250C"
    TR = "\u2510"
    BL = "\u2514"
    BR = "\u2518"
    T_RIGHT = "\u251C"
    T_LEFT = "\u2524"

    @classmethod
    def disable(cls) -> None:
        """Strip all ANSI codes (for --json or piped output)."""
        for attr in list(vars(cls)):
            if not attr.startswith("_") and attr != "disable":
                val = getattr(cls, attr)
                if isinstance(val, str) and val != "":
                    setattr(cls, attr, "")


def _box_line(width: int, left: str, fill: str, right: str) -> str:
    return f"{left}{fill * width}{right}"


def _padded(text: str, width: int) -> str:
    """Pad text inside box, accounting for ANSI escape sequences."""
    visible_len = len(_strip_ansi(text))
    padding = max(0, width - visible_len)
    return f"{Style.V_LINE} {text}{' ' * padding}{Style.V_LINE}"


def _strip_ansi(text: str) -> str:
    import re
    return re.sub(r"\033\[[0-9;]*m", "", text)


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------

class Severity(Enum):
    HARD_STOP = "hard_stop"
    WARNING = "warning"


class CheckResult(Enum):
    PASS = "pass"
    FAIL = "fail"
    WARNING = "warning"


@dataclass
class RuleDefinition:
    id: str
    name: str
    description: str
    category: str
    severity: Severity
    check_type: str
    raw: dict  # full rule dict for check-specific fields

    @classmethod
    def from_dict(cls, d: dict) -> RuleDefinition:
        return cls(
            id=d["id"],
            name=d["name"],
            description=d["description"],
            category=d["category"],
            severity=Severity(d["severity"]),
            check_type=d["check_type"],
            raw=d,
        )


@dataclass
class Finding:
    rule: RuleDefinition
    result: CheckResult
    message: str
    details: str = ""
    affected_items: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Field resolution helpers
# ---------------------------------------------------------------------------

def _resolve_field(payload: dict, field_path: str) -> Any:
    """
    Resolve a dotted field path, supporting [*] wildcard for arrays.

    Examples:
        "biochar_samples[*].h_corg_ratio" -> list of values
        "lca.e_biomass_tco2e"             -> single value
        "equipment.scale_calibration_expiry" -> single value
    """
    parts = field_path.replace("[*]", ".[*]").split(".")
    return _walk(payload, parts)


def _walk(obj: Any, parts: list[str]) -> Any:
    if not parts:
        return obj
    head, rest = parts[0], parts[1:]
    if head == "[*]":
        if not isinstance(obj, list):
            return None
        results = [_walk(item, rest) for item in obj]
        return results
    if isinstance(obj, dict) and head in obj:
        return _walk(obj[head], rest)
    return None


def _flatten(val: Any) -> list[Any]:
    """Flatten nested lists from wildcard resolution."""
    if val is None:
        return []
    if not isinstance(val, list):
        return [val]
    result: list[Any] = []
    for item in val:
        if isinstance(item, list):
            result.extend(_flatten(item))
        else:
            result.append(item)
    return result


def _resolve_array_items(payload: dict, field_path: str) -> list[tuple[str, Any]]:
    """
    Resolve an array field path and return (identifier, value) pairs.
    The identifier comes from common id fields in the parent objects.
    """
    parts = field_path.replace("[*]", ".[*]").split(".")
    star_idx = parts.index("[*]")
    array_path = parts[:star_idx]
    value_path = parts[star_idx + 1:]

    array_obj = payload
    for p in array_path:
        if isinstance(array_obj, dict) and p in array_obj:
            array_obj = array_obj[p]
        else:
            return []

    if not isinstance(array_obj, list):
        return []

    results: list[tuple[str, Any]] = []
    for i, item in enumerate(array_obj):
        # Find a sensible identifier
        ident = (
            item.get("sample_id")
            or item.get("batch_id")
            or item.get("site_id")
            or item.get("report_id")
            or f"item[{i}]"
        )
        val = _walk(item, value_path) if value_path else item
        results.append((ident, val))

    return results


# ---------------------------------------------------------------------------
# Rule checkers
# ---------------------------------------------------------------------------

def _check_threshold(rule: RuleDefinition, payload: dict) -> Finding:
    """Check that all values in an array field satisfy a threshold comparison."""
    field_path = rule.raw["field"]
    op = rule.raw["operator"]
    threshold = rule.raw["threshold"]

    items = _resolve_array_items(payload, field_path)
    if not items:
        return Finding(
            rule=rule,
            result=CheckResult.FAIL,
            message=f"No data found for {field_path}",
            details="Field path resolved to an empty set.",
        )

    failures: list[str] = []
    for ident, val in items:
        if val is None:
            failures.append(f"{ident}: value missing")
            continue
        if op == "less_than" and not (val < threshold):
            failures.append(f"{ident}: {val} >= {threshold}")

    if failures:
        sev = CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING
        return Finding(
            rule=rule,
            result=sev,
            message=f"{len(failures)} item(s) exceed threshold {op} {threshold}",
            details="; ".join(failures),
            affected_items=[f.split(":")[0] for f in failures],
        )

    return Finding(rule=rule, result=CheckResult.PASS, message="All values within threshold")


def _check_field_present(rule: RuleDefinition, payload: dict) -> Finding:
    """Check that a field exists and is not null/empty."""
    field_path = rule.raw["field"]
    is_array = "[*]" in field_path

    if is_array:
        items = _resolve_array_items(payload, field_path)
        if not items:
            return Finding(
                rule=rule,
                result=CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING,
                message=f"No data found for {field_path}",
            )
        missing: list[str] = []
        for ident, val in items:
            if val is None or val == "" or val == []:
                missing.append(ident)
        if missing:
            sev = CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING
            return Finding(
                rule=rule,
                result=sev,
                message=f"{len(missing)} item(s) missing required field",
                details=f"Missing in: {', '.join(missing)}",
                affected_items=missing,
            )
        return Finding(rule=rule, result=CheckResult.PASS, message="All required values present")
    else:
        val = _resolve_field(payload, field_path)
        if val is None or val == "" or val == []:
            sev = CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING
            return Finding(
                rule=rule,
                result=sev,
                message=f"Required field '{field_path}' is missing or null",
                details=f"Expected a value at '{field_path}' but found: {val!r}",
            )
        return Finding(rule=rule, result=CheckResult.PASS, message="Field present")


def _check_range(rule: RuleDefinition, payload: dict) -> Finding:
    """Check that all values fall within [min_value, max_value]."""
    field_path = rule.raw["field"]
    min_val = rule.raw["min_value"]
    max_val = rule.raw["max_value"]

    items = _resolve_array_items(payload, field_path)
    if not items:
        return Finding(
            rule=rule,
            result=CheckResult.FAIL,
            message=f"No data found for {field_path}",
        )

    failures: list[str] = []
    for ident, val in items:
        if val is None:
            failures.append(f"{ident}: value missing")
            continue
        if val < min_val or val > max_val:
            failures.append(f"{ident}: {val} outside [{min_val}, {max_val}]")

    if failures:
        sev = CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING
        return Finding(
            rule=rule,
            result=sev,
            message=f"{len(failures)} item(s) outside allowed range [{min_val}, {max_val}]",
            details="; ".join(failures),
            affected_items=[f.split(":")[0] for f in failures],
        )

    return Finding(rule=rule, result=CheckResult.PASS, message="All values within range")


def _check_date_not_expired(rule: RuleDefinition, payload: dict) -> Finding:
    """Check that a date field has not expired relative to a reference date."""
    expiry_str = _resolve_field(payload, rule.raw["field"])
    ref_str = _resolve_field(payload, rule.raw["reference_field"])

    if expiry_str is None:
        return Finding(
            rule=rule,
            result=CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING,
            message="Expiry date field missing",
        )

    try:
        expiry = date.fromisoformat(str(expiry_str))
        ref = date.fromisoformat(str(ref_str))
    except (ValueError, TypeError) as e:
        return Finding(rule=rule, result=CheckResult.FAIL, message=f"Date parse error: {e}")

    if expiry < ref:
        return Finding(
            rule=rule,
            result=CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING,
            message=f"Expired on {expiry} before reporting period end {ref}",
            details=f"Calibration/certificate expired {(ref - expiry).days} days before period end.",
        )

    return Finding(rule=rule, result=CheckResult.PASS, message=f"Valid through {expiry}")


def _check_unit_check(rule: RuleDefinition, payload: dict) -> Finding:
    """Check that a field matches an expected unit/value."""
    field_path = rule.raw["field"]
    expected = rule.raw["expected_value"]
    is_array = "[*]" in field_path

    if is_array:
        items = _resolve_array_items(payload, field_path)
        failures: list[str] = []
        for ident, val in items:
            if val is None:
                failures.append(f"{ident}: missing")
            elif str(val) != str(expected):
                failures.append(f"{ident}: '{val}' (expected '{expected}')")
        if failures:
            sev = CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING
            return Finding(
                rule=rule,
                result=sev,
                message=f"{len(failures)} item(s) have incorrect unit/value",
                details="; ".join(failures),
                affected_items=[f.split(":")[0] for f in failures],
            )
        return Finding(rule=rule, result=CheckResult.PASS, message=f"All values match '{expected}'")
    else:
        val = _resolve_field(payload, field_path)
        if val is None:
            return Finding(
                rule=rule,
                result=CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING,
                message=f"Field '{field_path}' missing",
            )
        if str(val) != str(expected):
            return Finding(
                rule=rule,
                result=CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING,
                message=f"Expected '{expected}', found '{val}'",
                details=f"Field '{field_path}' = '{val}' does not match expected '{expected}'.",
            )
        return Finding(rule=rule, result=CheckResult.PASS, message=f"Value matches '{expected}'")


def _check_no_uniform_default(rule: RuleDefinition, payload: dict) -> Finding:
    """Check that values across array items are not all the same (detecting blanket defaults)."""
    field_path = rule.raw["field"]
    min_unique = rule.raw.get("min_unique_values", 2)

    items = _resolve_array_items(payload, field_path)
    if not items:
        return Finding(rule=rule, result=CheckResult.FAIL, message=f"No data found for {field_path}")

    values = [val for _, val in items if val is not None]
    unique_values = set(values)

    if len(values) > 1 and len(unique_values) < min_unique:
        sites = [ident for ident, _ in items]
        return Finding(
            rule=rule,
            result=CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING,
            message=f"All {len(values)} sites use identical value {values[0]} — likely a blanket default",
            details=(
                f"Sites {', '.join(sites)} all report {values[0]}. "
                f"Soil temperature should vary by geographic region."
            ),
            affected_items=sites,
        )

    return Finding(
        rule=rule,
        result=CheckResult.PASS,
        message=f"{len(unique_values)} distinct value(s) across {len(values)} site(s)",
    )


def _check_formula(rule: RuleDefinition, payload: dict) -> Finding:
    """Validate a calculation formula against a reported result."""
    lca = _resolve_field(payload, "lca") or {}
    e_stored = lca.get("e_stored_tco2e")
    e_biomass = lca.get("e_biomass_tco2e")
    e_production = lca.get("e_production_tco2e")
    e_use = lca.get("e_use_tco2e")

    result_field = rule.raw["result_field"]
    reported = _resolve_field(payload, result_field)
    tolerance_pct = rule.raw.get("tolerance_pct", 1.0)

    if any(v is None for v in [e_stored, e_biomass, e_production, e_use, reported]):
        return Finding(
            rule=rule,
            result=CheckResult.FAIL,
            message="One or more formula inputs missing",
            details=f"e_stored={e_stored}, e_biomass={e_biomass}, e_production={e_production}, e_use={e_use}, reported={reported}",
        )

    expected = e_stored - e_biomass - e_production - e_use
    diff_pct = abs(reported - expected) / max(abs(expected), 0.001) * 100

    if diff_pct > tolerance_pct:
        return Finding(
            rule=rule,
            result=CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING,
            message=f"CORC formula mismatch: reported {reported:.2f}, expected {expected:.2f} (diff {diff_pct:.1f}%)",
            details=(
                f"Formula: E_stored({e_stored}) - E_biomass({e_biomass}) - "
                f"E_production({e_production}) - E_use({e_use}) = {expected:.2f}. "
                f"Reported net CORCs = {reported:.2f}. "
                f"Deviation = {diff_pct:.1f}% (tolerance: {tolerance_pct}%)."
            ),
        )

    return Finding(
        rule=rule,
        result=CheckResult.PASS,
        message=f"CORC formula validated: {expected:.2f} tCO2e (within {tolerance_pct}% tolerance)",
    )


def _check_date_within_period(rule: RuleDefinition, payload: dict) -> Finding:
    """Check that dates in an array field fall within the reporting period."""
    field_path = rule.raw["field"]
    start_str = _resolve_field(payload, rule.raw["period_start_field"])
    end_str = _resolve_field(payload, rule.raw["period_end_field"])

    try:
        period_start = date.fromisoformat(str(start_str))
        period_end = date.fromisoformat(str(end_str))
    except (ValueError, TypeError) as e:
        return Finding(rule=rule, result=CheckResult.FAIL, message=f"Period date parse error: {e}")

    items = _resolve_array_items(payload, field_path)
    if not items:
        return Finding(
            rule=rule,
            result=CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING,
            message="No items found to check",
        )

    failures: list[str] = []
    for ident, val in items:
        if val is None:
            failures.append(f"{ident}: date missing")
            continue
        try:
            d = date.fromisoformat(str(val))
        except (ValueError, TypeError):
            failures.append(f"{ident}: invalid date '{val}'")
            continue
        if d < period_start or d > period_end:
            failures.append(f"{ident}: {val} outside [{start_str}, {end_str}]")

    if failures:
        sev = CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING
        return Finding(
            rule=rule,
            result=sev,
            message=f"{len(failures)} report(s) outside reporting period",
            details="; ".join(failures),
            affected_items=[f.split(":")[0] for f in failures],
        )

    return Finding(
        rule=rule,
        result=CheckResult.PASS,
        message=f"All reports dated within {start_str} to {end_str}",
    )


def _check_minimum_count(rule: RuleDefinition, payload: dict) -> Finding:
    """Check that an array has at least N items."""
    field_path = rule.raw["field"]
    min_count = rule.raw["min_count"]

    val = _resolve_field(payload, field_path)
    count = len(val) if isinstance(val, list) else 0

    if count < min_count:
        sev = CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING
        return Finding(
            rule=rule,
            result=sev,
            message=f"Found {count} item(s), minimum required is {min_count}",
            details=f"Field '{field_path}' has {count} entries. Methodology requires at least {min_count}.",
        )

    return Finding(
        rule=rule,
        result=CheckResult.PASS,
        message=f"{count} item(s) present (minimum: {min_count})",
    )


def _check_field_match(rule: RuleDefinition, payload: dict) -> Finding:
    """Check that two fields have matching values."""
    val_a = _resolve_field(payload, rule.raw["field"])
    val_b = _resolve_field(payload, rule.raw["match_field"])

    if val_a is None or val_b is None:
        return Finding(
            rule=rule,
            result=CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING,
            message=f"Cannot compare — one or both fields missing",
            details=f"{rule.raw['field']} = {val_a!r}, {rule.raw['match_field']} = {val_b!r}",
        )

    if val_a != val_b:
        return Finding(
            rule=rule,
            result=CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING,
            message=f"Mismatch: {val_a} != {val_b}",
            details=(
                f"Opening stock ({val_a} t) does not match prior period closing stock ({val_b} t). "
                f"Discrepancy of {abs(val_a - val_b):.1f} t."
            ),
        )

    return Finding(
        rule=rule,
        result=CheckResult.PASS,
        message=f"Values match: {val_a}",
    )


def _check_conditional_field_present(rule: RuleDefinition, payload: dict) -> Finding:
    """Check that a field is present IF a condition field is true."""
    condition = _resolve_field(payload, rule.raw["condition_field"])

    if not condition:
        return Finding(
            rule=rule,
            result=CheckResult.PASS,
            message="Condition not triggered — check not applicable",
        )

    val = _resolve_field(payload, rule.raw["field"])
    if val is None or val == "" or val == []:
        sev = CheckResult.FAIL if rule.severity == Severity.HARD_STOP else CheckResult.WARNING
        return Finding(
            rule=rule,
            result=sev,
            message=f"Condition met ({rule.raw['condition_field']} = {condition}) but field missing",
            details=f"'{rule.raw['field']}' is required when '{rule.raw['condition_field']}' is true.",
        )

    return Finding(rule=rule, result=CheckResult.PASS, message="Conditional field present")


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

CHECKERS = {
    "threshold": _check_threshold,
    "field_present": _check_field_present,
    "range": _check_range,
    "date_not_expired": _check_date_not_expired,
    "unit_check": _check_unit_check,
    "no_uniform_default": _check_no_uniform_default,
    "formula": _check_formula,
    "date_within_period": _check_date_within_period,
    "minimum_count": _check_minimum_count,
    "field_match": _check_field_match,
    "conditional_field_present": _check_conditional_field_present,
}


def run_checks(rules: list[RuleDefinition], payload: dict) -> list[Finding]:
    """Execute all rule checks against a payload and return findings."""
    findings: list[Finding] = []
    for rule in rules:
        checker = CHECKERS.get(rule.check_type)
        if checker is None:
            findings.append(Finding(
                rule=rule,
                result=CheckResult.WARNING,
                message=f"Unknown check type '{rule.check_type}' — skipped",
            ))
            continue
        try:
            finding = checker(rule, payload)
        except Exception as e:
            finding = Finding(
                rule=rule,
                result=CheckResult.FAIL,
                message=f"Check raised exception: {e}",
            )
        findings.append(finding)
    return findings


# ---------------------------------------------------------------------------
# Risk level calculation
# ---------------------------------------------------------------------------

def compute_risk_level(findings: list[Finding]) -> str:
    critical_fails = sum(
        1 for f in findings
        if f.result == CheckResult.FAIL and f.rule.severity == Severity.HARD_STOP
    )
    warnings = sum(1 for f in findings if f.result == CheckResult.WARNING)
    total = len(findings)

    if critical_fails >= 5:
        return "CRITICAL"
    elif critical_fails >= 3:
        return "HIGH"
    elif critical_fails >= 1 or warnings >= 3:
        return "MEDIUM"
    else:
        return "LOW"


# ---------------------------------------------------------------------------
# Terminal output rendering
# ---------------------------------------------------------------------------

BOX_WIDTH = 78


def _render_header(payload: dict, metadata: dict) -> str:
    """Render the styled header box."""
    project = payload.get("project", {})
    period = payload.get("reporting_period", {})
    s = Style

    lines: list[str] = []
    lines.append("")
    lines.append(f"{s.CYAN}{_box_line(BOX_WIDTH, s.TL, s.H_LINE, s.TR)}{s.RESET}")
    lines.append(f"{s.CYAN}{_padded('', BOX_WIDTH)}{s.RESET}")

    title = f"{s.BOLD}{s.WHITE}MANGROVE PRE-FLIGHT ENGINE{s.RESET}"
    lines.append(f"{s.CYAN}{_padded(title, BOX_WIDTH)}{s.RESET}")
    subtitle = f"{s.DIM}Data Quality Check Report{s.RESET}"
    lines.append(f"{s.CYAN}{_padded(subtitle, BOX_WIDTH)}{s.RESET}")
    lines.append(f"{s.CYAN}{_padded('', BOX_WIDTH)}{s.RESET}")
    lines.append(f"{s.CYAN}{_padded(f'{s.RESET}{s.CYAN}{s.H_LINE * (BOX_WIDTH - 2)}', BOX_WIDTH)}{s.RESET}")

    info_lines = [
        (f"Project", project.get("name", "Unknown")),
        (f"Project ID", project.get("project_id", "Unknown")),
        (f"Operator", project.get("operator", "Unknown")),
        (f"Methodology", metadata.get("methodology", "Unknown")),
        (f"Period", f"{period.get('start_date', '?')} to {period.get('end_date', '?')}"),
        (f"Rules Version", f"{metadata.get('version', '?')} ({metadata.get('generated_date', '?')})"),
    ]

    for label, value in info_lines:
        entry = f"{s.BOLD}{s.WHITE}{label}:{s.RESET} {value}"
        lines.append(f"{s.CYAN}{_padded(entry, BOX_WIDTH)}{s.RESET}")

    lines.append(f"{s.CYAN}{_padded('', BOX_WIDTH)}{s.RESET}")
    lines.append(f"{s.CYAN}{_box_line(BOX_WIDTH, s.BL, s.H_LINE, s.BR)}{s.RESET}")
    lines.append("")

    return "\n".join(lines)


def _category_label(category: str) -> str:
    return category.replace("_", " ").upper()


def _render_findings(findings: list[Finding], verbose: bool) -> str:
    """Render check results grouped by category."""
    s = Style
    lines: list[str] = []

    # Group by category preserving order
    categories: dict[str, list[Finding]] = {}
    for f in findings:
        cat = f.rule.category
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(f)

    for cat, cat_findings in categories.items():
        lines.append(f"  {s.BOLD}{s.BLUE}{_category_label(cat)}{s.RESET}")
        lines.append(f"  {s.DIM}{s.H_LINE * 60}{s.RESET}")

        for f in cat_findings:
            if f.result == CheckResult.PASS:
                icon = f"{s.GREEN}\u2713{s.RESET}"
                status = f"{s.GREEN}PASS{s.RESET}"
            elif f.result == CheckResult.WARNING:
                icon = f"{s.YELLOW}\u26A0{s.RESET}"
                status = f"{s.YELLOW}WARN{s.RESET}"
            else:
                icon = f"{s.RED}\u2717{s.RESET}"
                status = f"{s.RED}FAIL{s.RESET}"

            sev_tag = ""
            if f.result != CheckResult.PASS:
                if f.rule.severity == Severity.HARD_STOP:
                    sev_tag = f" {s.BG_RED}{s.WHITE} HARD STOP {s.RESET}"
                else:
                    sev_tag = f" {s.BG_YELLOW}{s.WHITE} WARNING {s.RESET}"

            lines.append(
                f"  {icon} [{status}] {s.BOLD}{f.rule.id}{s.RESET} "
                f"{f.rule.name}{sev_tag}"
            )
            lines.append(f"         {s.DIM}{f.message}{s.RESET}")

            if verbose and f.result != CheckResult.PASS:
                if f.details:
                    lines.append(f"         {s.YELLOW}Detail: {f.details}{s.RESET}")
                if f.affected_items:
                    lines.append(f"         {s.YELLOW}Affected: {', '.join(f.affected_items)}{s.RESET}")
                lines.append(f"         {s.DIM}Ref: {f.rule.raw.get('methodology_reference', 'N/A')}{s.RESET}")
                lines.append(f"         {s.DIM}Source: {f.rule.raw.get('audit_finding_source', 'N/A')}{s.RESET}")

            lines.append("")

    return "\n".join(lines)


def _render_summary(findings: list[Finding]) -> str:
    """Render the summary box."""
    s = Style

    total = len(findings)
    passed = sum(1 for f in findings if f.result == CheckResult.PASS)
    critical_fails = sum(
        1 for f in findings
        if f.result == CheckResult.FAIL and f.rule.severity == Severity.HARD_STOP
    )
    warnings = sum(1 for f in findings if f.result == CheckResult.WARNING)
    non_critical_fails = sum(
        1 for f in findings
        if f.result == CheckResult.FAIL and f.rule.severity == Severity.WARNING
    )
    all_warnings = warnings + non_critical_fails

    risk = compute_risk_level(findings)

    risk_colors = {
        "LOW": s.GREEN,
        "MEDIUM": s.YELLOW,
        "HIGH": s.RED,
        "CRITICAL": f"{s.BG_RED}{s.WHITE}{s.BOLD}",
    }
    risk_color = risk_colors.get(risk, s.WHITE)

    lines: list[str] = []
    lines.append(f"  {s.CYAN}{_box_line(60, s.TL, s.H_LINE, s.TR)}{s.RESET}")
    lines.append(f"  {s.CYAN}{_padded(f'{s.BOLD}{s.WHITE}SUMMARY{s.RESET}', 60)}{s.RESET}")
    lines.append(f"  {s.CYAN}{_padded('', 60)}{s.RESET}")

    pass_pct = (passed / total * 100) if total > 0 else 0

    # Progress bar
    bar_width = 40
    filled = int(bar_width * passed / total) if total > 0 else 0
    bar = f"{s.GREEN}{'█' * filled}{s.DIM}{'░' * (bar_width - filled)}{s.RESET}"
    lines.append(f"  {s.CYAN}{_padded(f'  {bar} {passed}/{total} ({pass_pct:.0f}%)', 60)}{s.RESET}")
    lines.append(f"  {s.CYAN}{_padded('', 60)}{s.RESET}")

    lines.append(f"  {s.CYAN}{_padded(f'  {s.GREEN}Passed:     {passed}{s.RESET}', 60)}{s.RESET}")
    lines.append(f"  {s.CYAN}{_padded(f'  {s.RED}Critical:   {critical_fails}{s.RESET}', 60)}{s.RESET}")
    lines.append(f"  {s.CYAN}{_padded(f'  {s.YELLOW}Warnings:   {all_warnings}{s.RESET}', 60)}{s.RESET}")
    lines.append(f"  {s.CYAN}{_padded('', 60)}{s.RESET}")

    risk_line = f"  Audit Risk: {risk_color} {risk} {s.RESET}"
    lines.append(f"  {s.CYAN}{_padded(risk_line, 60)}{s.RESET}")

    if risk in ("HIGH", "CRITICAL"):
        lines.append(f"  {s.CYAN}{_padded(f'  {s.RED}{s.BOLD}Do NOT submit to verifier without remediation.{s.RESET}', 60)}{s.RESET}")
    elif risk == "MEDIUM":
        lines.append(f"  {s.CYAN}{_padded(f'  {s.YELLOW}Review flagged items before submission.{s.RESET}', 60)}{s.RESET}")
    else:
        lines.append(f"  {s.CYAN}{_padded(f'  {s.GREEN}Payload appears ready for verifier submission.{s.RESET}', 60)}{s.RESET}")

    lines.append(f"  {s.CYAN}{_padded('', 60)}{s.RESET}")
    lines.append(f"  {s.CYAN}{_box_line(60, s.BL, s.H_LINE, s.BR)}{s.RESET}")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

def findings_to_json(
    findings: list[Finding],
    payload: dict,
    metadata: dict,
) -> str:
    """Serialize findings to structured JSON."""
    total = len(findings)
    passed = sum(1 for f in findings if f.result == CheckResult.PASS)
    critical_fails = sum(
        1 for f in findings
        if f.result == CheckResult.FAIL and f.rule.severity == Severity.HARD_STOP
    )
    warnings = sum(1 for f in findings if f.result in (CheckResult.WARNING,))

    output = {
        "engine": "Mangrove Pre-Flight Engine v0.1",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "project": payload.get("project", {}),
        "reporting_period": payload.get("reporting_period", {}),
        "rules_metadata": metadata,
        "summary": {
            "total_checks": total,
            "passed": passed,
            "critical_failures": critical_fails,
            "warnings": warnings,
            "pass_rate_pct": round(passed / total * 100, 1) if total > 0 else 0,
            "risk_level": compute_risk_level(findings),
        },
        "findings": [
            {
                "rule_id": f.rule.id,
                "rule_name": f.rule.name,
                "category": f.rule.category,
                "severity": f.rule.severity.value,
                "result": f.result.value,
                "message": f.message,
                "details": f.details or None,
                "affected_items": f.affected_items or None,
                "methodology_reference": f.rule.raw.get("methodology_reference"),
            }
            for f in findings
        ],
    }

    return json.dumps(output, indent=2)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mangrove Pre-Flight Engine — Data quality checks for carbon removal projects",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 pre_flight_engine.py --rules puro_biochar_rules_v2025.json --payload sample_payload_with_issues.json\n"
            "  python3 pre_flight_engine.py --rules puro_biochar_rules_v2025.json --payload sample_payload_clean.json --verbose\n"
            "  python3 pre_flight_engine.py --rules puro_biochar_rules_v2025.json --payload sample_payload_with_issues.json --json\n"
        ),
    )
    parser.add_argument("--rules", required=True, help="Path to rules JSON file")
    parser.add_argument("--payload", required=True, help="Path to project payload JSON file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed failure explanations")
    parser.add_argument("--json", dest="json_output", action="store_true", help="Output machine-readable JSON")

    args = parser.parse_args()

    # Load files
    rules_path = Path(args.rules)
    payload_path = Path(args.payload)

    if not rules_path.exists():
        print(f"Error: Rules file not found: {rules_path}", file=sys.stderr)
        sys.exit(1)
    if not payload_path.exists():
        print(f"Error: Payload file not found: {payload_path}", file=sys.stderr)
        sys.exit(1)

    with open(rules_path) as f:
        rules_data = json.load(f)
    with open(payload_path) as f:
        payload = json.load(f)

    metadata = rules_data.get("metadata", {})
    rules = [RuleDefinition.from_dict(r) for r in rules_data.get("rules", [])]

    # Run checks
    findings = run_checks(rules, payload)

    # Output
    if args.json_output:
        Style.disable()
        print(findings_to_json(findings, payload, metadata))
    else:
        print(_render_header(payload, metadata))
        print(_render_findings(findings, verbose=args.verbose))
        print(_render_summary(findings))


if __name__ == "__main__":
    main()
