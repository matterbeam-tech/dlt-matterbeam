"""Invented, human-readable test-double segment storage for the fake Matterbeam server.

This is deliberately NOT a reimplementation of the real internal coldlog wire format
(CRF v2) -- there used to be a clean-room copy of that format here
(`dlt_matterbeam.crf`, later relocated to this test tree), but this public package's
test suite should not carry a dependency on that format's specific shape at all, even
in test-only code. Byte parity with the real writer is not a goal here and never should
be again.

What the fake server (`server.py`) and the fold helper (`fold.py`) actually need from a
storage mechanism, and all this module provides:

  * one append-only "segment" file per commit, under a per-recordtype directory, so
    `fold.scan` can list segments in write order (B7's ordering argument) and detect
    inversions the same way a real reader's monotonic cursor would
  * a record id that sorts the same way lexicographically as it does numerically, built
    from (ms_since_epoch, sequence_number, writer_id, noise) -- enough structure to
    exercise the C3 high-water clamp and per-request writer_id allocation tests, nothing
    more
  * plain newline-delimited JSON, uncompressed -- readable with a text editor if a test
    ever needs to be debugged by hand

None of this is meant to resemble any real system's on-disk format, and nothing in this
test suite should assume it does.
"""

from __future__ import annotations

import os
import random
import time

NOISE_BITS = 32


def pack_record_id(ms_since_epoch: int, sequence_number: int, writer_id: int, noise: int) -> int:
    """A synthetic, monotonically-sortable id: zero-padded decimal digits, so
    lexicographic string order matches numeric order. Purely invented for this fake
    server's own ordering tests -- not modeled on any real id layout."""
    return int(f"{ms_since_epoch:013d}{sequence_number:010d}{writer_id:05d}{noise:010d}")


def write_segment(
    root: str,
    recordtype_id: str,
    records: list[bytes],
    begin_ms: int,
    writer_id: int = 0,
    noise: int | None = None,
) -> tuple[str, int, int, int]:
    """Write one segment under ``root`` as plain newline-delimited JSON. records: list of
    UTF-8 JSON bytes, one per row.

    Returns (relative_key, first_record_id, last_record_id, byte_count).
    """
    if noise is None:
        noise = random.getrandbits(NOISE_BITS)
    ids = [pack_record_id(begin_ms, seq, writer_id, noise) for seq in range(len(records))]
    first_id, last_id = ids[0], ids[-1]
    key = f"segments/{recordtype_id}/{last_id}.{first_id}.jsonl"
    path = os.path.join(root, key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{random.getrandbits(32)}.tmp"
    with open(tmp, "wb") as f:
        for rid, rec in zip(ids, records):
            f.write(str(rid).encode("ascii"))
            f.write(b"\t")
            f.write(rec)
            f.write(b"\n")
    os.replace(tmp, path)  # the write is the commit; atomic within one filesystem
    return key, first_id, last_id, os.path.getsize(path)


def read_segment(path: str):
    """Yield (record_id, json_bytes) for every record in one segment."""
    with open(path, "rb") as f:
        for line in f:
            line = line.rstrip(b"\n")
            if not line:
                continue
            rid_bytes, _, data = line.partition(b"\t")
            yield int(rid_bytes), data


def now_ms() -> int:
    return int(time.time() * 1000)
