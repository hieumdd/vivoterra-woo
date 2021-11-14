import json
from unittest.mock import Mock

import pytest

from main import main

with open("creds.json") as f:
    creds = json.load(f)


def run(data: dict) -> dict:
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    return res


@pytest.mark.parametrize(
    "auth",
    [
        creds["EN"],
        creds["DE"],
    ],
    ids=[
        "EN",
        "DE",
    ],
)
@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        ("2018-01-01", "2021-12-01"),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def test_pipelines(auth, start, end):
    res = run(
        {
            "auth": auth,
            "start": start,
            "end": end,
        }
    )
    assert res["output_rows"] >= 0
