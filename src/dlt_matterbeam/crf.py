"""Clean-room CRF v2 segment writer + reader.

No Matterbeam imports (BRIEF §4.2). Framing and record_id packing are reproduced from
the design doc's Appendix A/B, which condense the real implementation at
``matterbeam_shared.coldlog_writer`` (``common_record_format.py``, ``coldlog_writer.py``):
byte-for-byte framing, zstd level 3 with checksums off, and the 128-bit record_id layout
(``ms_since_epoch`` MSB -> ``version`` LSB), rendered as a 56-digit decimal with a leading
``5`` so CRF v2 ids sort after legacy Kinesis ones. ``tests/vendor_parity`` checks this
module against the real writer directly.
"""

from __future__ import annotations

import io
import random
import struct
import time

import zstandard

VERSION = 2
NOISE_BITS = 32


def pack_record_id(
    ms_since_epoch: int,
    sequence_number: int,
    writer_id: int,
    noise: int,
    version: int = VERSION,
) -> int:
    """Pack the 128-bit record_id and render it as CRF v2's 56-digit decimal string-as-int."""
    packed = version
    packed |= noise << 4
    packed |= writer_id << 36
    packed |= sequence_number << 52
    packed |= ms_since_epoch << 84
    return int(f"5{packed:0>55}")


def frame(recordtype_id: str, record_id: int, utf_record: bytes) -> bytes:
    """One length-delimited CRF v2 record: total_len | record_id | recordtype | data."""
    rt = recordtype_id.encode("utf8")
    total = 4 + 24 + 4 + len(rt) + 4 + len(utf_record)
    buf = bytearray(total)
    struct.pack_into("!I", buf, 0, total)
    buf[4:28] = record_id.to_bytes(24, byteorder="big")
    struct.pack_into("!I", buf, 28, len(rt))
    buf[32 : 32 + len(rt)] = rt
    struct.pack_into("!I", buf, 32 + len(rt), len(utf_record))
    buf[36 + len(rt) :] = utf_record
    return bytes(buf)


def compress(body: bytes) -> bytes:
    """Streamed, not one-shot: the real writer streams frames into an open zstd writer as
    they're built (`zstandard.open`'s `ZstdCompressionWriter`), which omits the
    decompressed-content-size field that one-shot `ZstdCompressor.compress()` writes into
    the frame header -- same settings, different bytes otherwise. tests/vendor_parity
    checks this against the real writer directly."""
    buf = io.BytesIO()
    with zstandard.ZstdCompressor(level=3, write_checksum=False, threads=0).stream_writer(buf, closefd=False) as w:
        w.write(body)
    return buf.getvalue()


def write_segment(
    root: str,
    recordtype_id: str,
    records: list[bytes],
    begin_ms: int,
    writer_id: int = 0,
    noise: int | None = None,
) -> tuple[str, int, int, int]:
    """Write one segment under ``root``. records: list of UTF-8 JSON bytes, one per row.

    Returns (relative_key, first_record_id, last_record_id, compressed_byte_count).
    """
    import os

    if noise is None:
        noise = random.getrandbits(NOISE_BITS)
    ids = [pack_record_id(begin_ms, seq, writer_id, noise) for seq in range(len(records))]
    body = b"".join(frame(recordtype_id, rid, rec) for rid, rec in zip(ids, records))
    blob = compress(body)
    first_id, last_id = ids[0], ids[-1]
    key = f"crf_v2/{recordtype_id}/{last_id}.{first_id}.zst"
    path = os.path.join(root, key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{random.getrandbits(32)}.tmp"
    with open(tmp, "wb") as f:
        f.write(blob)
    os.replace(tmp, path)  # the write is the commit (B3); atomic within one filesystem
    return key, first_id, last_id, len(blob)


def read_segment(path: str):
    """Yield (record_id, recordtype_id, json_bytes) for every record in one segment."""
    with open(path, "rb") as f:
        body = zstandard.ZstdDecompressor().decompress(f.read(), max_output_size=1 << 30)
    off = 0
    while off < len(body):
        (total,) = struct.unpack_from("!I", body, off)
        rid = int.from_bytes(body[off + 4 : off + 28], "big")
        (rt_len,) = struct.unpack_from("!I", body, off + 28)
        rt = body[off + 32 : off + 32 + rt_len].decode("utf8")
        (d_len,) = struct.unpack_from("!I", body, off + 32 + rt_len)
        data = body[off + 36 + rt_len : off + 36 + rt_len + d_len]
        yield rid, rt, data
        off += total


def now_ms() -> int:
    return int(time.time() * 1000)
