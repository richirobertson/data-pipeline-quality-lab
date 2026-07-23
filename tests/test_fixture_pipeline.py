from __future__ import annotations

import json

from pipeline_quality.fixture_pipeline import main, run_fixture_pipeline


def test_fixture_pipeline_is_repeatable(fixture_dir, tmp_path) -> None:
    landing = tmp_path / "landing"
    manifest_path = tmp_path / "manifest.json"

    first = run_fixture_pipeline(
        fixtures=fixture_dir,
        landing=landing,
        manifest_path=manifest_path,
        run_id="test-run",
    )
    second = run_fixture_pipeline(
        fixtures=fixture_dir,
        landing=landing,
        manifest_path=manifest_path,
        run_id="test-run",
    )

    written = json.loads(manifest_path.read_text())
    assert first.filter_hash == second.filter_hash
    assert len(list(landing.rglob("*" + first.artifacts[0].sha256))) == 1
    assert written["run_id"] == "test-run"
    assert len(written["artifacts"]) == 2


def test_fixture_pipeline_command(tmp_path, fixture_dir) -> None:
    result = main(
        [
            "--fixtures",
            str(fixture_dir),
            "--landing",
            str(tmp_path / "landing"),
            "--manifest",
            str(tmp_path / "manifest.json"),
            "--run-id",
            "cli-run",
        ]
    )

    assert result == 0
    assert (tmp_path / "manifest.json").exists()
