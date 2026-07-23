"""Small command-line entry points that expose reproducible pipeline operations."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline_quality.manifest import canonical_filter_hash
from pipeline_quality.models import FilterDefinition


def build_parser() -> argparse.ArgumentParser:
    """Define the small public CLI separately so parser behavior is unit-testable."""
    parser = argparse.ArgumentParser(prog="pipeline-quality")
    subparsers = parser.add_subparsers(dest="command", required=True)
    hash_parser = subparsers.add_parser("filter-hash", help="hash a filter definition")
    hash_parser.add_argument("definition", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse a filter definition and print its reproducible identity."""
    args = build_parser().parse_args(argv)
    if args.command == "filter-hash":
        # Validation happens before hashing so unknown or malformed fields cannot
        # accidentally become part of a seemingly valid filter identity.
        payload = json.loads(args.definition.read_text(encoding="utf-8"))
        print(canonical_filter_hash(FilterDefinition.model_validate(payload)))
        return 0
    return 2


if __name__ == "__main__":
    # Raising SystemExit passes the integer return value to the calling shell.
    raise SystemExit(main())
