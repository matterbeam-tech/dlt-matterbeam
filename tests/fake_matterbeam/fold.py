"""Reader + fold, playing the emitter's role -- evolved from
`spikes/dlt-matterbeam/fake_matterbeam/fold.py`, reusing `dlt_matterbeam.crf` for framing
instead of a vendored copy.

Reader reproduces B7 exactly: segments are listed in lexicographic key order and consumed
under a strictly monotonic cursor, so a segment whose last_record_id sorts below the
cursor is never listed and never read. That silent drop is the failure mode the whole
ordering argument exists to prevent, so the reader must keep it rather than sort segments
into the right order.

Fold is last-value-wins by replay order plus tombstones (B4/B5a).
"""

from __future__ import annotations

import json
import os

from dlt_matterbeam import crf


def scan(root: str, recordtype_id: str, cursor: str = "0") -> tuple[list[tuple[int, dict]], list[str]]:
    """Returns (facts, dropped_keys). facts: [(record_id, dict)] in replay order."""
    directory = os.path.join(root, "crf_v2", recordtype_id)
    if not os.path.isdir(directory):
        return [], []

    facts: list[tuple[int, dict]] = []
    dropped: list[str] = []
    for key in sorted(os.listdir(directory)):
        if key.endswith(".tmp"):
            continue
        last_id = key.split(".")[0]
        if last_id <= cursor:  # start_after: never listed, never read (B7)
            dropped.append(key)
            continue
        for rid, _rt, data in crf.read_segment(os.path.join(directory, key)):
            facts.append((rid, json.loads(data)))
        cursor = last_id
    return facts, dropped


def fold(facts: list[tuple[int, dict]], key_fields: list[str]):
    """Replay facts into current state. Returns {key: record} (or a list if keyless)."""
    if not key_fields:
        return [record for _, record in facts]
    state: dict[str, dict] = {}
    for _rid, record in facts:
        meta = record.get("mb.metadata", {})
        key = ",".join(str(record.get(f)) for f in key_fields)
        if meta.get("is_tombstone"):
            state.pop(key, None)
        else:
            state[key] = record
    return state


def body(record: dict) -> dict:
    """The record as the customer's warehouse would see it: mb.* metadata stripped."""
    return {k: v for k, v in record.items() if k not in ("mb.metadata", "mb.schema_id")}
