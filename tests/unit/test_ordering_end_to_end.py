"""Phase 2's own stated demo (design doc §6): run two pipelines at once and watch them
proceed concurrently; run the same pipeline twice at once and watch the second get a 409
that it survives by retrying. Both exercised directly against the fake server's FSM lock.
"""

import threading

import dlt
from fake_matterbeam import fold


def _run(pipeline, resource_factory, results, index):
    try:
        pipeline.run(resource_factory())
        results[index] = "ok"
    except Exception as e:  # capture for the main thread to assert on
        results[index] = e


def test_two_different_pipelines_proceed_concurrently(http_pipeline_factory):
    make_pipeline, state = http_pipeline_factory
    pipeline_a = make_pipeline(dataset_name="ds_a")
    pipeline_b = make_pipeline(dataset_name="ds_b")

    def make_rows(n):
        @dlt.resource(name="rows", write_disposition="append")
        def rows():
            yield [{"id": i} for i in range(n)]

        return rows

    results = [None, None]
    threads = [
        threading.Thread(target=_run, args=(pipeline_a, lambda: make_rows(50), results, 0)),
        threading.Thread(target=_run, args=(pipeline_b, lambda: make_rows(50), results, 1)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert results == ["ok", "ok"]
    assert len(state.pids) == 2  # two different pipelines, two different pids

    # recordtype_id is (pid, table) now, not (dataset_name, table) -- a pid is already
    # 1:1-bound to one dataset_name at registration, so dataset_name would be a second,
    # redundant axis of identity (design doc Phase 2 report). Look the real pids up from
    # the fake server's registry rather than guessing the id shape.
    pid_a = state.registry[f"{pipeline_a.pipeline_name}|ds_a"]
    pid_b = state.registry[f"{pipeline_b.pipeline_name}|ds_b"]
    facts_a, _ = fold.scan(state.coldlog_root, f"{pid_a}.rows")
    facts_b, _ = fold.scan(state.coldlog_root, f"{pid_b}.rows")
    assert len(facts_a) == 50
    assert len(facts_b) == 50


def test_same_pipeline_run_twice_at_once_gets_a_409_and_still_lands_correctly(http_pipeline_factory):
    """ "Two laptops running the same pipeline name" (design doc D5/D6): the second run
    must not silently drop data or duplicate it -- the entry lock serialises the two, one
    of them retries through `lock_contention`, and the fold ends up identical to what a
    clean sequential run would have produced."""
    make_pipeline, state = http_pipeline_factory
    # Widen the entry lock's hold time so two genuinely concurrent requests are certain to
    # contend, rather than racing real request latency (matching the spike's MB_JITTER_MS).
    state.ingest_jitter_ms = 200
    # Same pipeline_name, same dataset_name -> same registration key -> same pid, exactly
    # the "two laptops" scenario -- distinct pipeline objects/dirs, not one object reused.
    pipeline_a = make_pipeline(dataset_name="ds", pipeline_name="shared_pipeline")
    pipeline_b = make_pipeline(dataset_name="ds", pipeline_name="shared_pipeline")

    def make_rows(offset):
        @dlt.resource(name="rows", write_disposition="append")
        def rows():
            for i in range(50):
                yield {"id": offset + i}

        return rows

    results = [None, None]
    threads = [
        threading.Thread(target=_run, args=(pipeline_a, lambda: make_rows(0), results, 0)),
        threading.Thread(target=_run, args=(pipeline_b, lambda: make_rows(1000), results, 1)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert results == ["ok", "ok"], results
    assert len(state.pids) == 1  # both pipelines resolved to the same pid
    assert state.counters["lock_contention"] > 0  # the lock actually contended at least once

    pid = state.registry["shared_pipeline|ds"]
    facts, dropped = fold.scan(state.coldlog_root, f"{pid}.rows")
    assert not dropped
    ids = {r["id"] for _, r in facts}
    assert ids == set(range(50)) | set(range(1000, 1050))  # both runs' rows all present, none lost
