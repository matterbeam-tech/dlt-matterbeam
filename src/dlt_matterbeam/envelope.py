"""Disposition translation: dlt write dispositions -> Matterbeam facts.

Pure functions, no dlt runtime and no I/O, so the translation logic is unit-testable on
plain dicts without spinning up a pipeline. ``load_job.py`` is the only caller.
"""

from __future__ import annotations

import datetime
import json
from typing import Any, Iterable, Mapping, Sequence

from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.json import custom_pua_remove

# MetadataV1 has exactly {version, record_type_id, schema_id, is_tombstone,
# collected_at_utc} today (matterbeam_shared.metadata.MetadataV1). `source_record_id` /
# `source_load_id` are proposed additive fields, not yet real on the server side. We
# stamp them anyway on our own local segments (there's no server to disagree with when
# writing locally), so a segment written today already carries the shape a future
# server will need.
METADATA_VERSION = 1


class ScdRefused(DestinationTerminalException):
    """merge + scd2 is refused: Matterbeam stores full history natively."""


def key_fields(table: Mapping[str, Any]) -> list[str]:
    """`primary_key` columns, else `merge_key` columns -- per-column hints, never top-level."""
    columns = table["columns"].values()
    fields = [c["name"] for c in columns if c.get("primary_key")]
    if fields:
        return fields
    return [c["name"] for c in columns if c.get("merge_key")]


def hard_delete_fields(table: Mapping[str, Any]) -> list[str]:
    return [c["name"] for c in table["columns"].values() if c.get("hard_delete")]


def dedup_sort(table: Mapping[str, Any]) -> tuple[str, str] | None:
    """(column, "asc"|"desc") for within-chunk same-key ordering, or None."""
    for c in table["columns"].values():
        direction = c.get("dedup_sort")
        if direction:
            return c["name"], direction
    return None


def check_disposition(table: Mapping[str, Any], warn: Any) -> None:
    """Raise on scd2 (refused); warn once on replace (degraded to append)."""
    disposition = table.get("write_disposition")
    strategy = table.get("x-merge-strategy")
    name = table["name"]
    if disposition == "merge" and strategy == "scd2":
        raise ScdRefused(
            "scd2 is refused: Matterbeam stores full history natively in the log; "
            "use `merge` (upsert/insert-only/delete-insert) and query the log instead."
        )
    if disposition == "replace":
        keyed = bool(key_fields(table))
        warn(
            f"table `{name}`: `replace` is degraded to `append` -- Matterbeam has no "
            "truncation marker, so the previous load's rows remain in the log. "
            + (
                "The table is keyed, so every key in the new load overwrites; only " "source-side deletions linger."
                if keyed
                else "The table is UNKEYED: the previous load's rows are indistinguishable "
                "from the new ones and will remain."
            )
        )


def sort_rows(rows: list[dict], sort: tuple[str, str] | None) -> list[dict]:
    """Within-chunk same-key ordering. Rows missing the sort column always sort last,
    in either direction -- there's no meaningful position for "unknown" otherwise."""
    if not sort:
        return rows
    col, direction = sort
    present = [r for r in rows if r.get(col) is not None]
    missing = [r for r in rows if r.get(col) is None]
    present.sort(key=lambda r: r[col], reverse=(direction == "desc"))
    return present + missing


def strip_pua(value: Any) -> Any:
    """Strip typed-jsonl's PUA type markers at the string level, no object round-trip."""
    if isinstance(value, str):
        return custom_pua_remove(value)
    if isinstance(value, list):
        return [strip_pua(v) for v in value]
    if isinstance(value, dict):
        return {k: strip_pua(v) for k, v in value.items()}
    return value


def _tombstone_and_body(row: dict, keys: Sequence[str], hard_delete: Sequence[str]) -> tuple[bool, dict]:
    """Shared by `build_record` (`FileTransport`) and `build_wire_record`
    (`HttpTransport`): tombstone detection and key-stripping are transport-agnostic --
    only who stamps `mb.metadata` and where the bytes go differs (client-side/local vs.
    server-side)."""
    is_tombstone = any(row.get(c) for c in hard_delete)
    body = {f: row[f] for f in keys} if (is_tombstone and keys) else row
    return is_tombstone, body


def build_record(
    row: dict,
    *,
    record_type_id: str,
    keys: Sequence[str],
    hard_delete: Sequence[str],
    load_id: str,
    schema_id: str | None = None,
) -> dict:
    """One dlt row -> one Matterbeam fact, `mb.metadata` inline.

    Used by transports with no server in the loop -- `FileTransport` and the
    internal-only `DirectColdlogTransport` -- which stamp their own `mb.metadata` and
    allocate their own record_id, because there is no server to do either.
    `HttpTransport` uses `build_wire_record` instead -- the server stamps metadata and
    allocates record_ids there.

    `schema_id` is optional, omitted from `mb.metadata` entirely when `None` -- unlike
    `record_type_id`, which is always stamped, `schema_id` is only stamped when the
    caller actually has a real one to give. `FileTransport` has no schema-registry
    concept of its own and never passes one, so this stays backward compatible with it.
    Mirrors the server's own `new_metadata` (`matterbeam_shared.metadata`)
    field-by-field -- a client-side transport writing directly must produce the
    identical `mb.metadata` shape the server would have stamped, or downstream
    consumers (an EventBridge handler's `translate_crf_v2`, a stats recorder) can't
    find the recordtype/schema this record claims.

    `_dlt_id` / `_dlt_load_id` are promoted into metadata, never left in the body --
    the emitter filter on `mb.*` field names is exact-name, not prefix, so anything
    else stays in the body verbatim.
    """
    dlt_id = row.pop("_dlt_id", None)
    row.pop("_dlt_load_id", None)

    is_tombstone, body = _tombstone_and_body(row, keys, hard_delete)

    metadata: dict[str, Any] = {
        "version": METADATA_VERSION,
        "record_type_id": record_type_id,
        "collected_at_utc": datetime.datetime.now(datetime.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
    }
    if schema_id:
        metadata["schema_id"] = schema_id
    if is_tombstone:
        metadata["is_tombstone"] = True
    if dlt_id:
        metadata["source_record_id"] = dlt_id
    if load_id:
        metadata["source_load_id"] = load_id

    body = dict(body)
    body["mb.metadata"] = metadata
    return body


def build_wire_record(row: dict, *, keys: Sequence[str], hard_delete: Sequence[str]) -> dict:
    """One dlt row -> one minimal client->server wire envelope (`HttpTransport` only).

    `{"t": bool, "i": str|None, "v": {...}}` -- the fold key is dropped, not replaced:
    the fold-key declaration path is out of scope here, so nothing server-side reads it
    either way. `mb.metadata` is not built here at all -- the server stamps it and
    allocates the record_id, overwriting anything a caller sent.
    """
    dlt_id = row.pop("_dlt_id", None)
    row.pop("_dlt_load_id", None)

    is_tombstone, body = _tombstone_and_body(row, keys, hard_delete)

    envelope_record: dict[str, Any] = {"v": body}
    if is_tombstone:
        envelope_record["t"] = True
    if dlt_id:
        envelope_record["i"] = dlt_id
    return envelope_record


def encode_record(record: dict) -> bytes:
    return json.dumps(record, separators=(",", ":")).encode("utf8")


def iter_stripped_rows(lines: Iterable[bytes], columns: set[str]) -> Iterable[dict]:
    """Parse typed-jsonl lines (one JSON array per line), strip PUA, project columns."""
    for raw in lines:
        if not raw.strip():
            continue
        recs = json.loads(raw)
        if isinstance(recs, dict):
            recs = [recs]
        for rec in recs:
            rec = strip_pua(rec)
            yield {k: v for k, v in rec.items() if k in columns}
