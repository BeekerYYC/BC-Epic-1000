#!/usr/bin/env python3
"""
Mangrove Agentic Harvester — Mock Demo
=======================================
Simulates the agentic PDF-to-rules pipeline that converts a carbon removal
methodology document into machine-readable compliance rules.

This is a demonstration of the CONCEPT for a live interview presentation.
No actual LLM integration — the animation illustrates the intended workflow.

Usage:
    python3 harvester_mock.py puro_earth_biochar_methodology_2022_v3.pdf
    python3 harvester_mock.py --output custom_rules.json methodology.pdf
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path


# ---------------------------------------------------------------------------
# ANSI styling
# ---------------------------------------------------------------------------

class Style:
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

    H_LINE = "\u2500"
    V_LINE = "\u2502"
    TL = "\u250C"
    TR = "\u2510"
    BL = "\u2514"
    BR = "\u2518"

    SPINNER = ["\u280B", "\u2819", "\u2839", "\u2838", "\u283C", "\u2834", "\u2826", "\u2827", "\u2807", "\u280F"]


def _box_top(width: int) -> str:
    return f"{Style.TL}{Style.H_LINE * width}{Style.TR}"


def _box_mid(text: str, width: int) -> str:
    padding = max(0, width - len(text))
    return f"{Style.V_LINE} {text}{' ' * (padding - 1)}{Style.V_LINE}"


def _box_bot(width: int) -> str:
    return f"{Style.BL}{Style.H_LINE * width}{Style.BR}"


# ---------------------------------------------------------------------------
# Animation helpers
# ---------------------------------------------------------------------------

def _spinner_wait(message: str, duration: float, steps: int = 0) -> None:
    """Show a spinner animation for the given duration."""
    s = Style
    spinner = s.SPINNER
    interval = 0.08
    elapsed = 0.0

    while elapsed < duration:
        idx = int(elapsed / interval) % len(spinner)
        frame = spinner[idx]
        sys.stdout.write(f"\r  {s.CYAN}{frame}{s.RESET} {message}")
        sys.stdout.flush()
        time.sleep(interval)
        elapsed += interval

    sys.stdout.write(f"\r  {s.GREEN}\u2713{s.RESET} {message}\n")
    sys.stdout.flush()


def _typewriter(text: str, delay: float = 0.02) -> None:
    """Print text character by character."""
    for char in text:
        sys.stdout.write(char)
        sys.stdout.flush()
        time.sleep(delay)
    sys.stdout.write("\n")
    sys.stdout.flush()


def _print_progress_bar(current: int, total: int, width: int = 40, label: str = "") -> None:
    """Print an inline progress bar."""
    s = Style
    filled = int(width * current / total)
    bar = f"{'█' * filled}{'░' * (width - filled)}"
    pct = current / total * 100
    sys.stdout.write(f"\r  {s.DIM}[{s.GREEN}{bar}{s.DIM}]{s.RESET} {pct:5.1f}%  {label}")
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

EXTRACTED_CATEGORIES = [
    ("Biochar Characterization", 3),
    ("Mass & Measurement", 4),
    ("LCA Completeness", 5),
    ("Calculation Accuracy", 4),
    ("Record-Keeping", 3),
    ("Stock Reconciliation", 1),
]


def stage_ingest(pdf_path: str) -> None:
    s = Style
    print(f"\n  {s.BOLD}{s.BLUE}STAGE 1/5{s.RESET} {s.DIM}Document Ingestion{s.RESET}")
    print(f"  {s.H_LINE * 50}")
    print(f"  {s.DIM}Source: {pdf_path}{s.RESET}")
    _spinner_wait("Loading document into context window...", 1.2)
    _spinner_wait("Tokenizing (est. 48,200 tokens)...", 0.8)
    _spinner_wait("Identifying methodology structure...", 1.0)
    print(f"  {s.DIM}Detected: 12 sections, 47 subsections, 8 appendices{s.RESET}")
    print()


def stage_extract() -> None:
    s = Style
    print(f"  {s.BOLD}{s.BLUE}STAGE 2/5{s.RESET} {s.DIM}Requirement Extraction{s.RESET}")
    print(f"  {s.H_LINE * 50}")

    requirements = [
        "MUST: Biochar H/Corg molar ratio < 0.7",
        "MUST: Organic carbon content (Corg) measured per EBC standard",
        "MUST: Pyrolysis temperature within 350-900°C range",
        "MUST: Dry mass recorded for each production batch",
        "MUST: E_stored, E_biomass, E_production, E_use all present",
        "MUST: Net CORCs = E_stored - E_biomass - E_production - E_use",
        "MUST: All emission values in consistent units (tCO2e total)",
        "MUST: Transport distances in kilometres",
        "MUST: Soil temperature reflect actual application region",
        "MUST: Scale calibration current during reporting period",
        "MUST: Lab reports dated within reporting period",
        "MUST: Opening stock = prior period closing stock",
        "SHOULD: Moisture content recorded per batch",
        "SHOULD: Wet weight recorded for transport calcs",
        "SHOULD: Diesel/equipment emissions included",
        "SHOULD: Wastewater emissions included if applicable",
        "SHOULD: Minimum quarterly lab sampling",
        "SHOULD: Feedstock sustainability documented",
        "CROSS-REF: Audit finding — American BioCarbon 2023 (soil temp default)",
        "CROSS-REF: Audit finding — Black Bull (missing measurements, expired cal)",
        "CROSS-REF: Audit finding — Aperam (stock mismatch, stale LCA data)",
        "CROSS-REF: Audit finding — Charm Industrial (miles vs km, Corg vs TC)",
    ]

    for i, req in enumerate(requirements, 1):
        _print_progress_bar(i, len(requirements), label=req[:60])
        time.sleep(0.15)

    _print_progress_bar(len(requirements), len(requirements), label="Complete")
    print()
    print(f"\n  {s.GREEN}\u2713{s.RESET} Extracted {len(requirements)} compliance requirements")
    print()


def stage_generate() -> None:
    s = Style
    print(f"  {s.BOLD}{s.BLUE}STAGE 3/5{s.RESET} {s.DIM}Rule Schema Generation{s.RESET}")
    print(f"  {s.H_LINE * 50}")

    total_rules = sum(count for _, count in EXTRACTED_CATEGORIES)
    running = 0

    for cat_name, count in EXTRACTED_CATEGORIES:
        print(f"  {s.CYAN}\u25B6{s.RESET} {cat_name}")
        for j in range(count):
            running += 1
            rule_id = f"PURO-BIO-{running:03d}"
            _spinner_wait(f"Generating {rule_id}...", 0.3)
        print()

    print(f"  {s.GREEN}\u2713{s.RESET} Generated {total_rules} machine-readable rules across {len(EXTRACTED_CATEGORIES)} categories")
    print()


def stage_validate() -> None:
    s = Style
    print(f"  {s.BOLD}{s.BLUE}STAGE 4/5{s.RESET} {s.DIM}Rule Validation{s.RESET}")
    print(f"  {s.H_LINE * 50}")

    checks = [
        ("Schema compliance", True),
        ("Field path resolution", True),
        ("Threshold coherence", True),
        ("Cross-rule consistency", True),
        ("Methodology coverage", True),
    ]

    for label, ok in checks:
        _spinner_wait(f"Validating {label.lower()}...", 0.5)

    print(f"\n  {s.GREEN}\u2713{s.RESET} All validation checks passed")
    print()


def stage_write(output_path: str) -> None:
    s = Style
    print(f"  {s.BOLD}{s.BLUE}STAGE 5/5{s.RESET} {s.DIM}Output{s.RESET}")
    print(f"  {s.H_LINE * 50}")

    # Copy the rules file
    source = Path(__file__).parent / "puro_biochar_rules_v2025.json"
    dest = Path(output_path)

    if source.exists():
        shutil.copy2(source, dest)
        _spinner_wait(f"Writing {dest.name}...", 0.8)
        print(f"  {s.GREEN}\u2713{s.RESET} Rules written to {s.BOLD}{dest}{s.RESET}")

        # Quick stats
        with open(dest) as f:
            data = json.load(f)
        rule_count = len(data.get("rules", []))
        categories = set(r["category"] for r in data.get("rules", []))

        print(f"\n  {s.DIM}Output summary:{s.RESET}")
        print(f"    Rules:      {rule_count}")
        print(f"    Categories: {len(categories)}")
        print(f"    Hard stops: {sum(1 for r in data['rules'] if r['severity'] == 'hard_stop')}")
        print(f"    Warnings:   {sum(1 for r in data['rules'] if r['severity'] == 'warning')}")
    else:
        _spinner_wait(f"Writing {dest.name}...", 0.8)
        print(f"  {s.YELLOW}\u26A0{s.RESET} Source rules file not found — wrote empty scaffold")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mangrove Agentic Harvester — Convert methodology PDFs to machine-readable rules",
    )
    parser.add_argument(
        "pdf_path",
        nargs="?",
        default="puro_earth_biochar_methodology_2022_v3.pdf",
        help="Path to methodology PDF (simulated — file need not exist)",
    )
    parser.add_argument(
        "--output", "-o",
        default="puro_biochar_rules_v2025.json",
        help="Output path for generated rules JSON",
    )

    args = parser.parse_args()
    s = Style

    # Banner
    width = 64
    print()
    print(f"  {s.MAGENTA}{_box_top(width)}{s.RESET}")
    print(f"  {s.MAGENTA}{_box_mid('', width)}{s.RESET}")
    print(f"  {s.MAGENTA}{_box_mid(f'{s.BOLD}{s.WHITE}  MANGROVE AGENTIC HARVESTER{s.RESET}{s.MAGENTA}', width)}{s.RESET}")
    print(f"  {s.MAGENTA}{_box_mid(f'{s.DIM}  Methodology PDF -> Machine-Readable Rules{s.RESET}{s.MAGENTA}', width)}{s.RESET}")
    print(f"  {s.MAGENTA}{_box_mid('', width)}{s.RESET}")
    print(f"  {s.MAGENTA}{_box_bot(width)}{s.RESET}")
    print()
    print(f"  {s.DIM}This demo simulates the agentic pipeline that converts registry{s.RESET}")
    print(f"  {s.DIM}methodology documents into executable compliance rules.{s.RESET}")
    print()

    # Run stages
    stage_ingest(args.pdf_path)
    stage_extract()
    stage_generate()
    stage_validate()
    stage_write(args.output)

    # Done
    print(f"  {s.GREEN}{s.BOLD}Pipeline complete.{s.RESET}")
    print(f"  {s.DIM}Run the Pre-Flight Engine to validate project data against these rules:{s.RESET}")
    print()
    print(f"    python3 pre_flight_engine.py --rules {args.output} --payload <payload.json>")
    print()


if __name__ == "__main__":
    main()
