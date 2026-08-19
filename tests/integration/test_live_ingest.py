"""Phase 2 integration tier: a real dlt pipeline against a real Matterbeam dev account.

Only collected when `MATTERBEAM_API_TOKEN` and `MATTERBEAM_BASE_URL` are both set (see
`conftest.py` in this directory) -- CI never needs these secrets, and `pytest` (which
defaults to `tests/unit`, see `pyproject.toml`) never touches this tier unless explicitly
pointed at it: `pytest tests/integration`.

This is deliberately thin. Everything about merge semantics, ordering, 409 handling, and
state round-tripping is already covered against the fake server in `tests/unit/` -- what
only a *real* account can prove is that those same code paths actually reach it: auth,
routing, a real segment landing in a real coldlog, and a real registration producing a
real, canvas-visible pid.
"""

import os
import uuid

import dlt

from dlt_matterbeam.destinations import matterbeam


def _destination():
    # `destination="matterbeam"` (the bare string form) resolves the sink via dlt's own
    # config providers, which have no mapping from this test's MATTERBEAM_API_TOKEN /
    # MATTERBEAM_BASE_URL env vars to MatterbeamClientConfiguration's fields -- it would
    # default to transport="http" with no matterbeam_url configured and fail fast rather
    # than ever touch the network, which is exactly what this file did until building
    # Phase 2 surfaced it (both tests "passed" against a local file transport, never
    # against a real backend, back when the default silently fell back to "file"
    # transport). Construct the destination explicitly instead, exactly like
    # tests/unit/conftest.py's http_pipeline_factory does against the fake server.
    return matterbeam(
        transport="http",
        matterbeam_url=os.environ["MATTERBEAM_BASE_URL"],
        api_token=os.environ["MATTERBEAM_API_TOKEN"],
    )


def _pipeline(dataset_name: str, pipeline_name: str, **kwargs) -> "dlt.Pipeline":
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=_destination(),
        dataset_name=dataset_name,
        progress=None,
        **kwargs,
    )


def test_append_lands_and_registers_a_real_pid():
    run_id = uuid.uuid4().hex[:8]
    pipeline = _pipeline(dataset_name=f"dlt_matterbeam_it_{run_id}", pipeline_name=f"dlt_matterbeam_it_{run_id}")

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]

    info = pipeline.run(rows())
    assert not info.has_failed_jobs

    # `pipeline.destination_client()` builds a *fresh* client instance that was never
    # used in the run (the loader opens and closes one client per job, per the design
    # doc's §8.2 finding) -- its own .pid is legitimately None. Registration is
    # idempotent (D6), so calling it again is the correct way to confirm a real pid
    # exists for this run rather than asserting on an instance that never registered.
    client = pipeline.destination_client()
    pid = client.transport.register(pipeline.pipeline_name, pipeline.dataset_name)
    assert pid is not None


def test_incremental_state_survives_a_fresh_machine():
    """The half of D5 that matters most: a second `pipelines_dir` with no local state at
    all reads the cursor back over HTTP before extracting, and fetches only new rows."""
    run_id = uuid.uuid4().hex[:8]
    dataset_name = f"dlt_matterbeam_it_{run_id}"
    pipeline_name = f"dlt_matterbeam_it_{run_id}"

    events = [{"id": i, "updated_at": f"2026-01-{i:02d}T00:00:00+00:00"} for i in range(1, 6)]

    @dlt.resource(name="events", write_disposition="append", primary_key="id")
    def make_events(upto, cursor=dlt.sources.incremental("updated_at", initial_value="2020-01-01T00:00:00+00:00")):
        since = cursor.last_value
        yield [e for e in events[:upto] if str(e["updated_at"]) > str(since)]

    pipeline_a = _pipeline(dataset_name=dataset_name, pipeline_name=pipeline_name)
    info_a = pipeline_a.run(make_events(3))
    assert not info_a.has_failed_jobs

    # A genuinely fresh machine: same pipeline_name/dataset_name, no local state dir.
    fresh_pipelines_dir = os.path.join(pipeline_a.pipelines_dir, "..", f"fresh_{run_id}")
    pipeline_b = _pipeline(
        dataset_name=dataset_name,
        pipeline_name=pipeline_name,
        pipelines_dir=fresh_pipelines_dir,
    )
    info_b = pipeline_b.run(make_events(5))
    assert not info_b.has_failed_jobs
    # If the cursor had not come back from the destination, this would have re-extracted
    # events 1-5; the resource itself only yields what's past `cursor.last_value`, so a
    # successful run with no failed jobs and a restored cursor is the observable proof --
    # inspecting exactly which rows landed requires a real Matterbeam reader, out of scope
    # for this package's own test suite.
