import json

import pytest

from pipeline_quality.cli import main


def test_filter_hash_command_is_stable(tmp_path, capsys) -> None:
    """The public CLI must expose the same canonical identity as library code."""
    definition = tmp_path / "filter.json"
    definition.write_text(
        json.dumps(
            {
                "dataset": {"id": "TS009", "edition": "2021", "version": 1},
                "population_type": "UR",
                "dimensions": [{"name": "sex"}],
            }
        )
    )

    assert main(["filter-hash", str(definition)]) == 0
    assert len(capsys.readouterr().out.strip()) == 64


def test_cli_requires_a_command() -> None:
    """Missing operator intent should produce a parser error, not hidden work."""
    with pytest.raises(SystemExit):
        main([])
