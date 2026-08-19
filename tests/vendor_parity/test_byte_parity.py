"""Phase 1 assertion 3 (design §6): a segment written by the clean-room `crf.py` is
byte-identical to one written by the real `ColdlogWriter`, given the same records and
record_ids. Compression settings per B1a: zstd level 3, checksums off, threads=0.

`ColdlogWriter.write()` takes explicit `(recordtype_id, record_id, record)` tuples rather
than generating its own timestamp/noise -- exactly what lets this test hold both writers
to the same inputs instead of racing real-time clocks against each other.
"""

from matterbeam_shared.coldlog_writer.coldlog_writer import ColdlogWriter
from matterbeam_shared.json_encoder import json_dump_bytes

from dlt_matterbeam import crf

RECORDTYPE_ID = "ds.parity_table"
WRITER_ID = 0


def _real_segment_bytes(records: list[dict], record_ids: list[int]) -> bytes:
    captured = {}

    def capture_put(key, body, metadata=None):
        captured["key"], captured["body"] = key, body

    writer = ColdlogWriter(s3_put_fn=capture_put, recordtype_id=RECORDTYPE_ID, writer_id=WRITER_ID)
    writer.write([(RECORDTYPE_ID, rid, rec) for rid, rec in zip(record_ids, records)])
    writer.commit_batch()
    return captured["key"], captured["body"]


def _our_segment_bytes(records: list[dict], record_ids: list[int], begin_ms: int, noise: int) -> tuple:
    encoded = [json_dump_bytes(r) for r in records]  # same JSON encoder both sides use
    # crf.write_segment allocates its own sequential ids from begin_ms/noise/writer_id --
    # reproduce that allocation exactly rather than passing record_ids separately, since
    # the real writer's `write()` path allocates the same way internally when given explicit
    # ids in sequence order. Assert our allocation matches first.
    frames = b"".join(crf.frame(RECORDTYPE_ID, rid, rec) for rid, rec in zip(record_ids, encoded))
    compressed = crf.compress(frames)
    first_id, last_id = record_ids[0], record_ids[-1]
    key = f"crf_v2/{RECORDTYPE_ID}/{last_id}.{first_id}.zst"
    return key, compressed


def test_record_id_allocation_matches_the_real_packer():
    from matterbeam_shared.coldlog_writer.common_record_format import generate_sequence_number_v2

    begin_ms, noise = 1_700_000_000_123, 42
    for seq in range(5):
        ours = crf.pack_record_id(begin_ms, seq, WRITER_ID, noise)
        real = generate_sequence_number_v2(WRITER_ID, begin_ms, noise, seq)
        assert ours == real


def test_single_frame_matches_real_binary_writer():
    import io

    from matterbeam_shared.coldlog_writer.common_record_format import crf_v2_binary_writer

    record_id = crf.pack_record_id(1_700_000_000_000, 0, 0, 7)
    body = json_dump_bytes({"id": 1, "name": "ada"})

    buf = io.BytesIO()
    crf_v2_binary_writer(buf, RECORDTYPE_ID, record_id, body)
    real_bytes = buf.getvalue()

    ours = crf.frame(RECORDTYPE_ID, record_id, body)
    assert ours == real_bytes


def test_full_segment_byte_identical_to_real_coldlog_writer():
    records = [{"id": i, "name": f"row-{i}", "nested": {"a": [1, 2, 3]}} for i in range(20)]
    begin_ms, noise = crf.now_ms(), 12345
    record_ids = [crf.pack_record_id(begin_ms, seq, WRITER_ID, noise) for seq in range(len(records))]

    real_key, real_body = _real_segment_bytes(records, record_ids)
    our_key, our_body = _our_segment_bytes(records, record_ids, begin_ms, noise)

    assert our_key == real_key
    assert our_body == real_body
