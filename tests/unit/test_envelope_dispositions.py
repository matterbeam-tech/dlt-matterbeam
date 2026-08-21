"""Disposition translation as pure functions, no dlt pipeline required."""

import pytest
from dlt_matterbeam import envelope


def _table(name="t", write_disposition=None, strategy=None, columns=None):
    return {
        "name": name,
        "write_disposition": write_disposition,
        "x-merge-strategy": strategy,
        "columns": columns or {},
    }


def test_key_fields_prefers_primary_key_over_merge_key():
    table = _table(
        columns={
            "id": {"name": "id", "primary_key": True},
            "legacy_id": {"name": "legacy_id", "merge_key": True},
        }
    )
    assert envelope.key_fields(table) == ["id"]


def test_key_fields_falls_back_to_merge_key():
    table = _table(columns={"legacy_id": {"name": "legacy_id", "merge_key": True}})
    assert envelope.key_fields(table) == ["legacy_id"]


def test_key_fields_empty_for_unkeyed_table():
    table = _table(columns={"a": {"name": "a"}})
    assert envelope.key_fields(table) == []


def test_hard_delete_fields():
    table = _table(columns={"deleted": {"name": "deleted", "hard_delete": True}, "a": {"name": "a"}})
    assert envelope.hard_delete_fields(table) == ["deleted"]


def test_dedup_sort():
    table = _table(columns={"ts": {"name": "ts", "dedup_sort": "desc"}})
    assert envelope.dedup_sort(table) == ("ts", "desc")
    assert envelope.dedup_sort(_table(columns={"a": {"name": "a"}})) is None


def test_scd2_is_refused():
    table = _table(write_disposition="merge", strategy="scd2")
    with pytest.raises(envelope.ScdRefused):
        envelope.check_disposition(table, warn=lambda *_: None)


def test_replace_warns_and_names_keyed_vs_unkeyed():
    warnings = []
    keyed = _table(write_disposition="replace", columns={"id": {"name": "id", "primary_key": True}})
    envelope.check_disposition(keyed, warn=warnings.append)
    assert "degraded to `append`" in warnings[0]
    assert "keyed" in warnings[0]

    warnings.clear()
    unkeyed = _table(write_disposition="replace", columns={"a": {"name": "a"}})
    envelope.check_disposition(unkeyed, warn=warnings.append)
    assert "UNKEYED" in warnings[0]


def test_plain_merge_and_append_do_not_warn_or_raise():
    calls = []
    envelope.check_disposition(_table(write_disposition="merge"), warn=calls.append)
    envelope.check_disposition(_table(write_disposition="append"), warn=calls.append)
    assert calls == []


def test_sort_rows_respects_direction_and_nulls_last():
    rows = [{"k": 1, "ts": 2}, {"k": 1, "ts": None}, {"k": 1, "ts": 1}]
    asc = envelope.sort_rows(rows, ("ts", "asc"))
    assert [r["ts"] for r in asc] == [1, 2, None]
    desc = envelope.sort_rows(rows, ("ts", "desc"))
    assert [r["ts"] for r in desc] == [2, 1, None]


def test_build_record_plain_row_has_no_tombstone():
    row = {"id": 1, "name": "ada", "_dlt_id": "abc123", "_dlt_load_id": "999"}
    rec = envelope.build_record(row, record_type_id="ds.t", keys=["id"], hard_delete=["deleted"], load_id="999")
    assert rec["id"] == 1
    assert rec["name"] == "ada"
    assert "_dlt_id" not in rec
    assert "_dlt_load_id" not in rec
    meta = rec["mb.metadata"]
    assert meta["version"] == 1
    assert meta["record_type_id"] == "ds.t"
    assert "is_tombstone" not in meta
    assert meta["source_record_id"] == "abc123"
    assert meta["source_load_id"] == "999"


def test_build_record_tombstone_strips_body_to_key_fields():
    row = {"id": 1, "name": "ada", "deleted": True, "_dlt_id": "abc123", "_dlt_load_id": "999"}
    rec = envelope.build_record(row, record_type_id="ds.t", keys=["id"], hard_delete=["deleted"], load_id="999")
    assert set(rec) == {"id", "mb.metadata"}
    assert rec["mb.metadata"]["is_tombstone"] is True


def test_build_record_keyless_tombstone_keeps_full_body():
    # no key fields declared -- nothing to strip to, so the row survives whole
    # (unkeyed hard_delete is not the modeled case)
    row = {"a": 1, "deleted": True, "_dlt_id": "x", "_dlt_load_id": "1"}
    rec = envelope.build_record(row, record_type_id="ds.t", keys=[], hard_delete=["deleted"], load_id="1")
    assert rec["a"] == 1


def test_build_record_stamps_schema_id_when_given():
    """Mirrors the server's own `new_metadata` (`matterbeam_shared.metadata`): a
    client-side transport writing directly (no server to stamp it) must produce the
    identical `mb.metadata` shape, or downstream consumers (an EventBridge handler, a
    stats recorder) can't find the schema a record claims."""
    row = {"id": 1, "_dlt_id": "abc123", "_dlt_load_id": "999"}
    rec = envelope.build_record(
        row, record_type_id="ds.t", schema_id="schema-1", keys=["id"], hard_delete=[], load_id="999"
    )
    assert rec["mb.metadata"]["schema_id"] == "schema-1"


def test_build_record_omits_schema_id_when_not_given():
    """`FileTransport` has no schema-registry concept of its own and never passes one --
    must stay backward compatible (omitted, not `None` written into the dict)."""
    row = {"id": 1, "_dlt_id": "abc123", "_dlt_load_id": "999"}
    rec = envelope.build_record(row, record_type_id="ds.t", keys=["id"], hard_delete=[], load_id="999")
    assert "schema_id" not in rec["mb.metadata"]
