"""D11: typed-jsonl embeds PUA type markers for decimal/datetime/etc (A13) -- these must be
stripped at the string level before a record ships, or the marker characters land in the
log as literal, uninferrable bytes. None of the other end-to-end tests exercise a typed
value, so this is the one place that path is actually proven."""

import datetime
import decimal
import json


def _all_records(output_dir, recordtype_id):
    import os

    out = []
    path = os.path.join(output_dir, f"{recordtype_id}.jsonl")
    if not os.path.exists(path):
        return out
    with open(path, "rb") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append((line, json.loads(line)))
    return out


def test_decimal_and_datetime_survive_with_no_pua_markers(pipeline_factory):
    import dlt

    pipeline = pipeline_factory(dataset_name="ds")

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield {
            "id": 1,
            "amount": decimal.Decimal("12.50"),
            "ts": datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        }

    pipeline.run(rows())

    records = _all_records(pipeline.destination.config_params["output_dir"], "ds.rows")
    assert len(records) == 1
    raw, rec = records[0]

    # PUA_START_UTF8_MAGIC and friends are non-printable private-use codepoints; if any
    # survived, they'd show up as literal bytes in the encoded JSON.
    assert all(0xE000 > ord(c) or ord(c) > 0xF8FF for c in raw.decode("utf8"))
    assert rec["amount"] == "12.50"
    assert rec["ts"] == "2026-01-02T03:04:05+00:00"
