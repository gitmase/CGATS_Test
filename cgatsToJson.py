from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
import uuid
import re

# Converts a CGATS Color Data file to JSON format
# JSON file will be used to load data into Postgres database
#
# Version 2.0.0 - 10 Aug 2025
#   Refactored for clarity and maintainability
#   Added --compact option for compact JSON output
#   Improved logging configuration
#   Added default values for PROJECT and TEMPLATE fields
#   Enhanced error handling and reporting
#   Improved parsing logic for robustness
#   Eliminated descriptive data that is included in the template table in the database

CGATS_KV_RE = re.compile(r"^\s*([A-Za-z0-9_./-]+)\s*[:=]\s*(.*?)\s*$")
COMMENT_RE = re.compile(r"^\s*(?:#|//|;|\*)")
WS_ONLY_RE = re.compile(r"^\s*$")


@dataclass(frozen=True)
class ParseResult:
    descriptive: Dict[str, str]
    keys: List[str]
    rows: List[Dict[str, str]]


def iter_nonempty_lines(text: Iterable[str]) -> Iterable[str]:
    for raw in text:
        line = raw.rstrip("\r\n")
        if not line:
            yield ""
        else:
            yield line


def parse_descriptive_and_formats(lines: Iterable[str], logger: logging.Logger) -> Tuple[Dict[str, str], List[str], List[str]]:
    descriptive: Dict[str, str] = {}
    keys: List[str] = []
    buffer_after_begin_data: List[str] = []

    in_df = False
    in_data = False

    for line in iter_nonempty_lines(lines):
        u = line.strip()

        if not in_df and not in_data and (COMMENT_RE.match(u) or WS_ONLY_RE.match(u)):
            continue

        if not in_df and not in_data:
            if u.upper().startswith("BEGIN_DATA_FORMAT"):
                in_df = True
                continue
            if u.upper().startswith("BEGIN_DATA"):
                in_data = True
                continue

            m = CGATS_KV_RE.match(line)
            if m:
                k, v = m.group(1), m.group(2)
                descriptive[k.strip().upper()] = v.strip()
            else:
                logger.debug("Ignoring header line: %r", line)
            continue

        if in_df and not in_data:
            if u.upper().startswith("END_DATA_FORMAT"):
                in_df = False
                continue
            if u.upper().startswith("BEGIN_DATA"):
                in_df = False
                in_data = True
                continue

            if u and not COMMENT_RE.match(u):
                parts = u.split()
                keys.extend([p.strip().upper() for p in parts if p.strip()])
            continue

        if in_data:
            buffer_after_begin_data.append(line)

    return descriptive, keys, buffer_after_begin_data


def parse_rows(after_begin_data_lines: List[str], keys: List[str], logger: logging.Logger) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for line in after_begin_data_lines:
        u = line.strip()
        if not u:
            continue
        up = u.upper()
        if up.startswith("END_DATA"):
            break
        if COMMENT_RE.match(u):
            continue

        cols = u.split()
        if len(cols) != len(keys):
            logger.warning("Row has %d cols but %d keys. Row: %r", len(cols), len(keys), u)
        if len(cols) < len(keys):
            cols.extend([""] * (len(keys) - len(cols)))
        elif len(cols) > len(keys):
            cols = cols[: len(keys)]

        row = {k: v for k, v in zip(keys, cols)}
        rows.append(row)
    return rows


def parse_cgats_text(text: str, logger: logging.Logger) -> ParseResult:
    descriptive, keys, after_begin_data = parse_descriptive_and_formats(text.splitlines(), logger)

    if not keys:
        logger.debug("No keys found in BEGIN_DATA_FORMAT; attempting to infer from first data line.")
        for line in after_begin_data:
            s = line.strip()
            if s and not COMMENT_RE.match(s) and not s.upper().startswith("END_DATA"):
                n = len(s.split())
                keys = [f"C{{i+1}}" for i in range(n)]
                logger.debug("Inferred %d columns: %s", n, keys)
                break

    rows = parse_rows(after_begin_data, keys, logger)
    return ParseResult(descriptive=descriptive, keys=keys, rows=rows)


def to_json(descriptive: Dict[str, str], rows: List[Dict[str, str]], measurement_uuid: str) -> Dict[str, object]:
    enriched_rows: List[Dict[str, str]] = []
    for r in rows:
        rr = dict(r)
        rr["MEASUREMENT_ID"] = measurement_uuid
        enriched_rows.append(rr)

    return {
        "descriptive_data": descriptive,
        "measurement_data": enriched_rows,
    }


def configure_logging(verbose: bool, log_file: Path | None) -> logging.Logger:
    logger = logging.getLogger("cgats2json")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG if verbose else logging.INFO)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="cgats_to_json",
        description="Convert a CGATS color data file to JSON (pretty-printed by default).",
    )
    ap.add_argument("input", type=Path, help="Path to CGATS file")
    ap.add_argument("-o", "--output", type=Path, help="Output JSON path (default: <input>.json)")
    ap.add_argument("--log-file", type=Path, default=None, help="Optional log file path")
    ap.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    ap.add_argument("--compact", action="store_true", help="Output compact JSON instead of pretty-printed")  # NEW

    args = ap.parse_args(argv)

    logger = configure_logging(args.verbose, args.log_file)

    input_path: Path = args.input
    output_path: Path = args.output if args.output else input_path.with_suffix(".json")

    logger.info("Reading CGATS file: %s", input_path)
    text = input_path.read_text(encoding="utf-8", errors="replace")

    measurement_uuid = str(uuid.uuid4())
    logger.info("Generated MEASUREMENT_ID: %s", measurement_uuid)

    result = parse_cgats_text(text, logger=logger)

    # Enrich header with defaults
    result.descriptive.setdefault("MEASUREMENT_ID", measurement_uuid)
    result.descriptive.setdefault("PARSED_DATE", date.today().isoformat())
    result.descriptive.setdefault("PROJECT", "NONE")     # NEW default
    result.descriptive.setdefault("TEMPLATE", "NONE")    # NEW default

    payload = to_json(result.descriptive, result.rows, measurement_uuid)

    # Pretty by default; compact if --
    indent = None if args.compact else 2
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=indent, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Wrote JSON to: %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
