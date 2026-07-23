from __future__ import annotations

import json

import pytest

from pipeline_quality.exceptions import SchemaContractError
from pipeline_quality.spark_transform import validate_csvw_contract


def test_csvw_fixture_matches_source_contract(fixture_dir) -> None:
    validate_csvw_contract(fixture_dir / "ons-population.csvw")


def test_csvw_rejects_reordered_columns(fixture_dir, tmp_path) -> None:
    payload = json.loads((fixture_dir / "ons-population.csvw").read_text())
    payload["tableSchema"]["columns"].reverse()
    changed = tmp_path / "changed.csvw"
    changed.write_text(json.dumps(payload))

    with pytest.raises(SchemaContractError, match="do not match"):
        validate_csvw_contract(changed)


def test_csvw_rejects_non_integer_observation(fixture_dir, tmp_path) -> None:
    payload = json.loads((fixture_dir / "ons-population.csvw").read_text())
    payload["tableSchema"]["columns"][-1]["datatype"] = "string"
    changed = tmp_path / "changed.csvw"
    changed.write_text(json.dumps(payload))

    with pytest.raises(SchemaContractError, match="must be an integer"):
        validate_csvw_contract(changed)
