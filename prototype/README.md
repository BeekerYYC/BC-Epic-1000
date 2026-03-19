# Mangrove Pre-Flight Engine — Prototype

A pre-flight data quality check system that validates carbon removal project data against registry methodology rules **before** submission to verifiers.

Built as a demonstration prototype for a Product Lead interview at Mangrove Systems.

## What it demonstrates

1. **Agentic Harvester** — An AI pipeline that converts methodology PDFs (e.g., Puro.earth Biochar Methodology) into machine-readable compliance rules.
2. **Pre-Flight Engine** — A validation engine that checks project data payloads against those rules, catching the same classes of errors that real verifiers find during audits.

The rules and sample data are calibrated against **real audit findings** from Puro Biochar verifications (American BioCarbon, Black Bull Biochar, Aperam Bioenergia, Charm Industrial).

## Files

| File | Purpose |
|------|---------|
| `puro_biochar_rules_v2025.json` | 20 compliance rules extracted from Puro.earth Biochar Methodology |
| `sample_payload_with_issues.json` | Mock project data with 10+ deliberate issues |
| `sample_payload_clean.json` | Same project data with all issues resolved |
| `pre_flight_engine.py` | Validation engine (CLI) |
| `harvester_mock.py` | Animated demo of the agentic PDF-to-rules pipeline |

## Quick start

**Requirements:** Python 3.9+ (zero external dependencies)

### Run the harvester demo
```bash
python3 harvester_mock.py
```

### Run pre-flight checks against the problematic payload
```bash
python3 pre_flight_engine.py \
  --rules puro_biochar_rules_v2025.json \
  --payload sample_payload_with_issues.json \
  --verbose
```

### Run against the clean payload
```bash
python3 pre_flight_engine.py \
  --rules puro_biochar_rules_v2025.json \
  --payload sample_payload_clean.json
```

### Machine-readable JSON output
```bash
python3 pre_flight_engine.py \
  --rules puro_biochar_rules_v2025.json \
  --payload sample_payload_with_issues.json \
  --json
```

## Issues encoded in the problematic payload

| Issue | Rule | Real-world source |
|-------|------|--------------------|
| H/Corg ratio exceeds 0.7 | PURO-BIO-001 | Charm Industrial |
| Pyrolysis temp below 350°C | PURO-BIO-003 | Black Bull Biochar |
| Missing dry mass | PURO-BIO-004 | Black Bull Biochar |
| Missing moisture content | PURO-BIO-005 | Black Bull Biochar |
| Missing wet weight | PURO-BIO-006 | American BioCarbon 2023 |
| Scale calibration expired | PURO-BIO-007 | Black Bull Biochar |
| Diesel equipment not tracked | PURO-BIO-011 | Black Bull Biochar |
| Wastewater emissions missing | PURO-BIO-012 | Black Bull Biochar |
| Uniform soil temp (blanket 15°C) | PURO-BIO-013 | American BioCarbon 2023 |
| Transport in miles not km | PURO-BIO-014 | Charm Industrial |
| Per-tonne units instead of total | PURO-BIO-015 | American BioCarbon 2025 |
| CORC formula mismatch | PURO-BIO-016 | American BioCarbon 2025 |
| Lab report outside period | PURO-BIO-017 | Aperam Bioenergia |
| Only 1 lab report (need 4) | PURO-BIO-018 | Black Bull Biochar |
| Stock opening/closing mismatch | PURO-BIO-019 | Aperam Bioenergia |
