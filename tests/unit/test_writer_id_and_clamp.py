"""Phase 3 (design doc C3): per-request `writer_id` allocation, and the high-water clamp
on `begin_timestamp` that gives intra-pid monotonicity insurance against the server's own
clock regressing between two requests the entry lock has already ordered. Neither is the
ordering guarantee itself (that's the entry lock, D6) -- this only proves the two small
mechanisms C3 actually asks for.
"""

import _direct_http as http


def test_writer_id_is_allocated_per_request(fake_server):
    base_url, state = fake_server
    pid = http.register(base_url, "pl", "ds")

    r1 = http.post_chunk(base_url, pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}])
    r2 = http.post_chunk(base_url, pid, "t", "L1", "J2", 0, [{"i": "b", "v": {"id": 2}}])

    assert r1.json()["data"]["writer_id"] != r2.json()["data"]["writer_id"]


def test_high_water_clamp_survives_a_server_clock_regression(fake_server, monkeypatch):
    """Without the clamp, a backward clock step produces a segment that sorts *below*
    the previous one -- B7's exact hazard: silently and permanently unreadable. With the
    clamp active, record_id stays monotonic even though the clock did not."""
    import fake_matterbeam.server as server_mod

    base_url, state = fake_server
    pid = http.register(base_url, "pl", "ds")

    first = http.post_chunk(base_url, pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}])
    last_id_1 = first.json()["data"]["last_record_id"]
    assert last_id_1 is not None

    real_now_ms = server_mod.segment_store.now_ms
    monkeypatch.setattr(server_mod.segment_store, "now_ms", lambda: real_now_ms() - 60_000)

    second = http.post_chunk(base_url, pid, "t", "L1", "J2", 0, [{"i": "b", "v": {"id": 2}}])
    last_id_2 = second.json()["data"]["last_record_id"]
    assert last_id_2 > last_id_1  # lexicographic == numeric here: both are fixed-width, zero-padded


def test_clamp_is_load_bearing(fake_server, monkeypatch):
    """The negative case: with the clamp off, the same clock regression *does* produce an
    inverted (silently unreadable, B7) segment."""
    import fake_matterbeam.server as server_mod

    monkeypatch.setattr(server_mod, "CLAMP_ON", False)

    base_url, state = fake_server
    pid = http.register(base_url, "pl", "ds")

    first = http.post_chunk(base_url, pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}])
    last_id_1 = first.json()["data"]["last_record_id"]

    real_now_ms = server_mod.segment_store.now_ms
    monkeypatch.setattr(server_mod.segment_store, "now_ms", lambda: real_now_ms() - 60_000)

    second = http.post_chunk(base_url, pid, "t", "L1", "J2", 0, [{"i": "b", "v": {"id": 2}}])
    last_id_2 = second.json()["data"]["last_record_id"]
    assert last_id_2 < last_id_1  # the documented hazard, reproduced with the clamp off
