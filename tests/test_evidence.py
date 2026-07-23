import json

from pipeline_quality.evidence import dbt_summary, load_json, main, render_report


def test_dbt_summary_classifies_results() -> None:
    """Model successes and test passes both count as healthy dbt outcomes."""
    total, passed, failed = dbt_summary(
        {
            "results": [
                {"status": "success"},
                {"status": "pass"},
                {"status": "fail"},
            ]
        }
    )
    assert (total, passed, failed) == (3, 2, 1)


def test_report_connects_source_spark_and_dbt_evidence() -> None:
    """One report should trace source identity through processing and modeling."""
    report = render_report(
        manifest={
            "run_id": "run-1",
            "filter_hash": "abc",
            "source": {"id": "TS009", "edition": "2021", "version": 1},
            "artifacts": [{"kind": "csv", "sha256": "123"}],
        },
        spark={"input_rows": 8, "accepted_rows": 7, "quarantined_rows": 1},
        dbt_results={"results": [{"status": "pass"}]},
    )

    assert "Pipeline run: `run-1`" in report
    assert "| Input | 8 |" in report
    assert "| Quarantined | 1 |" in report
    assert "| Passed | 1 |" in report


def test_load_json_handles_missing_and_non_object_files(tmp_path) -> None:
    """Incomplete diagnostic inputs should degrade predictably."""
    assert load_json(tmp_path / "missing.json") == {}
    path = tmp_path / "list.json"
    path.write_text("[]")
    assert load_json(path) == {}


def test_evidence_command_writes_report(tmp_path) -> None:
    """The operator-facing command must create its promised Markdown artifact."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"run_id": "cli-run"}))
    output = tmp_path / "report.md"

    result = main(
        [
            "--manifest",
            str(manifest),
            "--spark",
            str(tmp_path / "missing-spark.json"),
            "--dbt-results",
            str(tmp_path / "missing-dbt.json"),
            "--output",
            str(output),
        ]
    )

    assert result == 0
    assert "cli-run" in output.read_text()
