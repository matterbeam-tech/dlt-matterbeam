"""Appendix B: CRF v2 framing and record_id packing, independent of dlt entirely."""

from dlt_matterbeam import crf


def test_pack_record_id_bit_layout():
    # B2: ms_since_epoch MSB(44) | sequence_number(32) | writer_id(16) | noise(32) | version(4)
    rid = crf.pack_record_id(ms_since_epoch=1, sequence_number=0, writer_id=0, noise=0, version=2)
    # version=2 is the last 4 bits of the 128-bit value, i.e. the last hex nibble of the
    # un-prefixed decimal's binary form. Round-trip through the packing math directly:
    packed = 2 | (0 << 4) | (0 << 36) | (0 << 52) | (1 << 84)
    assert rid == int(f"5{packed:0>55}")


def test_record_ids_sort_by_timestamp_then_sequence():
    a = crf.pack_record_id(1000, 0, 0, 0)
    b = crf.pack_record_id(1000, 1, 0, 0)
    c = crf.pack_record_id(1001, 0, 0, 0)
    assert a < b < c


def test_write_read_segment_round_trip(tmp_path):
    records = [b'{"id":1}', b'{"id":2}', b'{"id":3}']
    key, first_id, last_id, nbytes = crf.write_segment(str(tmp_path), "ds.t", records, begin_ms=crf.now_ms())
    path = tmp_path / key
    assert path.exists()
    assert nbytes == path.stat().st_size

    read_back = list(crf.read_segment(str(path)))
    assert [data for _, _, data in read_back] == records
    assert [rt for _, rt, _ in read_back] == ["ds.t"] * 3
    assert read_back[0][0] == first_id
    assert read_back[-1][0] == last_id
    assert first_id < last_id


def test_segment_key_matches_appendix_b_layout(tmp_path):
    key, first_id, last_id, _ = crf.write_segment(str(tmp_path), "ds.t", [b"{}"], begin_ms=crf.now_ms())
    assert key == f"crf_v2/ds.t/{last_id}.{first_id}.zst"
