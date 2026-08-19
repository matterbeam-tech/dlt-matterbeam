"""Phase 1 assertion 1 (design §6): the declared `sequential` parallelism strategy is
honoured at *runtime*, not just round-tripped through `capabilities()` (open question #2,
D2/D4's shared falsifier). Instruments job start/stop by wrapping `MatterbeamLoadJob.run` --
a test-only wrapper, not production tracing code -- and asserts no two job spans overlap.

Also confirms the negative: dlt's default parallelism DOES overlap jobs on the same
workload, so `sequential` is load-bearing rather than decorative (matching the spike's
finding in the design doc's §8.1 ordering row).
"""

import threading
import time

from dlt_matterbeam.load_job import MatterbeamLoadJob


def _instrument(monkeypatch):
    spans = []
    lock = threading.Lock()
    original_run = MatterbeamLoadJob.run

    def wrapped(self):
        start = time.monotonic()
        try:
            return original_run(self)
        finally:
            end = time.monotonic()
            with lock:
                spans.append((start, end))

    monkeypatch.setattr(MatterbeamLoadJob, "run", wrapped)
    return spans


def _overlaps(spans):
    ordered = sorted(spans)
    return sum(1 for i in range(1, len(ordered)) if ordered[i][0] < ordered[i - 1][1])


def _wide_source(tables=3, rows_per_table=60):
    import dlt

    resources = []
    for t in range(tables):

        @dlt.resource(name=f"t{t}", write_disposition="append", primary_key="id")
        def r(t=t):
            time.sleep(0.01)  # give overlapping jobs a real window to collide in
            yield [{"id": f"{t}-{i}", "n": i} for i in range(rows_per_table)]

        resources.append(r)
    return resources


def test_sequential_strategy_serialises_jobs_at_runtime(pipeline_factory, monkeypatch):
    spans = _instrument(monkeypatch)
    monkeypatch.setenv("NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS", "15")

    pipeline = pipeline_factory(dataset_name="wide")  # sequential is the declared default
    pipeline.run(_wide_source())

    assert len(spans) >= 4, f"expected >=4 job files across >=2 tables, got {len(spans)}"
    assert _overlaps(spans) == 0


def test_default_parallelism_does_overlap_on_the_same_workload(pipeline_factory, monkeypatch):
    spans = _instrument(monkeypatch)
    monkeypatch.setenv("NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS", "15")

    # override the declared strategy to prove `sequential` is load-bearing, not decorative
    pipeline = pipeline_factory(dataset_name="wide", loader_parallelism_strategy="parallel")
    pipeline.run(_wide_source())

    assert len(spans) >= 4
    assert _overlaps(spans) > 0
