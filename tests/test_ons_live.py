from __future__ import annotations

import os

import pytest

from pipeline_quality.ons_client import OnsClient

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.getenv("RUN_LIVE_ONS") != "1", reason="live ONS check is opt-in"),
]


def test_ts009_version_contract_is_still_usable() -> None:
    with OnsClient() as client:
        version = client.get_dataset_version("TS009", "2021", 1)

    dimensions = {dimension["id"] for dimension in version["dimensions"]}
    assert version["state"] == "published"
    assert version["edition"] == "2021"
    assert version["version"] == 1
    assert {"ltla", "sex", "resident_age_91a"} <= dimensions
    assert {"csv", "csvw"} <= version["downloads"].keys()
