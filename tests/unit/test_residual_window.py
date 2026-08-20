"""D10's residual stale-resurrection window, reproduced deterministically and measured
rather than guessed at (design doc Phase 3 requirement).

The exact sequence from D10's own text: job J1 (key K, version 1) writes one chunk, then
fails before sending a second chunk that also touches key K. Job J2 (key K, version 2)
runs to completion, superseding K. J1 retries: its first chunk is now a harmless ledger
replay, but the second chunk -- never sent before the crash -- is genuinely new data,
lands *after* J2's write, and wins the last-value-wins fold even though it carries the
older value. Ordering (the entry lock) does not prevent this: it is order across time,
not concurrency, and D10 says so explicitly.

This is the case the design deliberately didn't build around because it expected it to
be rare. This test proves the server-side instrumentation added in Phase 3 catches it and
also demonstrates the underlying data hazard it's counting.
"""

import _direct_http as http
from fake_matterbeam import fold


def test_residual_window_is_detected_and_the_underlying_hazard_reproduces(fake_server):
    base_url, state = fake_server
    pid = http.register(base_url, "pl", "ds")

    # J1's first chunk: unrelated row, succeeds. (J1 then "crashes" before sending its
    # second chunk -- nothing more is sent for J1 until the retry, below.)
    http.post_chunk(base_url, pid, "t", "L1", "J1", 0, [{"i": "j1c0", "v": {"id": 1, "val": "other"}}])

    # J2 runs to completion, superseding key 42 with version 2.
    http.post_chunk(base_url, pid, "t", "L1", "J2", 0, [{"i": "j2c0", "v": {"id": 42, "val": "v2"}}])

    # J1 retries: its file is re-read from the start, so chunk 0 is resent first (a
    # harmless ledger replay) and then its never-before-sent chunk 1 -- carrying the
    # *older* value for key 42 -- goes out for the first time, landing after J2's write.
    http.post_chunk(base_url, pid, "t", "L1", "J1", 0, [{"i": "j1c0", "v": {"id": 1, "val": "other"}}])
    resurrecting = http.post_chunk(
        base_url, pid, "t", "L1", "J1", 1, [{"i": "j1c1", "v": {"id": 42, "val": "v1_stale"}}]
    )
    assert resurrecting.status_code == 200

    assert state.counters["residual_stale_resurrection"] == 1

    facts, dropped = fold.scan(state.coldlog_root, f"{pid}.t")
    assert not dropped
    folded = fold.fold(facts, key_fields=["id"])
    # The documented residual: the *older* value wins, because it landed later in
    # physical arrival order. This is the bug D10 describes, not a bug in this test.
    assert folded["42"]["val"] == "v1_stale"
