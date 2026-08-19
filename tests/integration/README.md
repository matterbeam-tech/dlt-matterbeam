# Integration tests

Real-account tier for `HttpTransport` (Phase 2). `conftest.py` wires the skip condition:
tests in this directory only run when both `MATTERBEAM_API_TOKEN` and
`MATTERBEAM_BASE_URL` are set in the environment; otherwise they're excluded from
collection entirely, so `pytest` (which defaults to `tests/unit`, see `pyproject.toml`)
never touches this tier and CI never needs the secrets.

Run explicitly, against a real Matterbeam dev account, once the Phase 2 server-side
routes are deployed:

```bash
MATTERBEAM_API_TOKEN=... MATTERBEAM_BASE_URL=https://api.<customer>.matterbeam.com \
  pytest tests/integration
```

`test_live_ingest.py` is deliberately thin: merge semantics, ordering, the two
distinguishable 409s, and state round-tripping are all covered against the fake server in
`tests/unit/` already. What only a real account can prove — and what these tests check —
is that the same code paths actually reach it: auth, routing, a real segment landing in a
real coldlog, and a real registration producing a real, canvas-visible pid.

**Not yet exercised here** (no reader exists in this package to check it): whether the
segment a real run produces is byte-parity-correct against what a Matterbeam collector
would write (that's `tests/vendor_parity`, against the real `ColdlogWriter` directly, no
live account needed), and whether the rows are actually visible/foldable in the
Matterbeam UI — they are not, by design this phase (no fold-key declaration path, P3
deferred to a separate project; see the design doc's Phase 2 report).
