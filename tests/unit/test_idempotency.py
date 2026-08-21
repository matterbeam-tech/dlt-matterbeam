"""The chunk ledger, the record-level `_dlt_id` backstop that must run on *every* chunk
(not only once a ledger entry has expired), and the `complete_load` bound.

Driven directly over HTTP against the fake server (`_direct_http.py`) rather than through
a whole dlt pipeline: these mechanics are server-internal, and constructing an exact
`_dlt_id`/job_id/seq by hand is far more direct than hoping dlt's own retry timing lines
up with a particular scenario.
"""

import _direct_http as http
from fake_matterbeam import fold


def test_duplicate_chunk_resend_is_a_noop(fake_server):
    base_url, state = fake_server
    pid = http.register(base_url, "pl", "ds")
    rows = [{"i": "a", "v": {"id": 1}}, {"i": "b", "v": {"id": 2}}]

    first = http.post_chunk(base_url, pid, "t", "L1", "J1", 0, rows)
    assert first.status_code == 200
    assert first.json()["data"]["record_count"] == 2
    assert first.json()["data"]["duplicate"] is False

    second = http.post_chunk(base_url, pid, "t", "L1", "J1", 0, rows)
    assert second.status_code == 200
    assert second.json()["data"]["duplicate"] is True

    facts, dropped = fold.scan(state.coldlog_root, f"{pid}.t")
    assert not dropped
    assert len(facts) == 2  # not 4 -- the resend wrote nothing


def test_record_level_dedup_catches_a_replay_after_the_ledger_entry_is_gone(fake_server):
    """A chunk commits, then (for any reason -- TTL expiry in production, simulated here
    by just deleting the entry) its ledger entry is gone. A naive "ledger only"
    implementation would duplicate the chunk on retry; the record-level `_dlt_id`
    backstop must catch it anyway."""
    base_url, state = fake_server
    pid = http.register(base_url, "pl", "ds")
    rows = [{"i": "a", "v": {"id": 1}}, {"i": "b", "v": {"id": 2}}]

    first = http.post_chunk(base_url, pid, "t", "L1", "J1", 0, rows)
    assert first.json()["data"]["record_count"] == 2

    # Simulate the ledger entry having expired/been lost -- the chunk ledger alone can no
    # longer recognise this as a replay.
    state.ledger.discard(f"{pid}|L1|J1|0")
    state.ledger_results.pop(f"{pid}|L1|J1|0", None)

    retry = http.post_chunk(base_url, pid, "t", "L1", "J1", 0, rows)
    assert retry.status_code == 200
    assert retry.json()["data"]["record_count"] == 0  # both rows deduped by _dlt_id
    assert state.counters["record_dedup_hits"] == 2

    facts, dropped = fold.scan(state.coldlog_root, f"{pid}.t")
    assert not dropped
    assert len(facts) == 2  # still exactly the first write -- no duplicate landed


def test_record_level_dedup_is_load_bearing(fake_server, monkeypatch):
    """The negative case: with the backstop turned off, the exact same ledger-loss replay
    *does* duplicate -- proving the dedup, not something else, is what prevents it above."""
    import fake_matterbeam.server as server_mod

    monkeypatch.setattr(server_mod, "DEDUP_ON", False)

    base_url, state = fake_server
    pid = http.register(base_url, "pl", "ds")
    rows = [{"i": "a", "v": {"id": 1}}, {"i": "b", "v": {"id": 2}}]

    http.post_chunk(base_url, pid, "t", "L1", "J1", 0, rows)
    state.ledger.discard(f"{pid}|L1|J1|0")
    state.ledger_results.pop(f"{pid}|L1|J1|0", None)
    retry = http.post_chunk(base_url, pid, "t", "L1", "J1", 0, rows)
    assert retry.json()["data"]["record_count"] == 2  # duplicated, dedup was off

    facts, dropped = fold.scan(state.coldlog_root, f"{pid}.t")
    assert not dropped
    assert len(facts) == 4  # the documented, accepted failure mode with the backstop off


def test_complete_load_closes_the_load(fake_server):
    base_url, state = fake_server
    pid = http.register(base_url, "pl", "ds")

    ok = http.post_chunk(base_url, pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}])
    assert ok.status_code == 200

    http.complete_load(base_url, pid, "L1")

    # A replay of an already-recorded chunk is still just a harmless ack, even after close.
    replay = http.post_chunk(base_url, pid, "t", "L1", "J1", 0, [{"i": "a", "v": {"id": 1}}])
    assert replay.status_code == 200
    assert replay.json()["data"]["duplicate"] is True

    # Genuinely new data for the closed load is rejected, not silently accepted.
    late = http.post_chunk(base_url, pid, "t", "L1", "J2", 0, [{"i": "b", "v": {"id": 2}}])
    assert late.status_code == 409
    assert late.json()["error"]["context"]["reason"] == "load_closed"
