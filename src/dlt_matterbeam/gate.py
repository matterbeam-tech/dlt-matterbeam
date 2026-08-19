"""The upload gate (BRIEF §1 step 2; findings-dlt-deployment.md A11; cli-and-upload-
options.md §3): decide whether a pipeline script's destination is `matterbeam` without
ever contacting a real destination.

This is pass 1 only -- the local, pre-upload check that runs inside `dlt matterbeam
deploy` itself, in the same environment the customer used for their pipeline's local
run (so the dependency is already importable, per A11). Pass 2 (the authoritative,
server-side re-check inside the build step) does not exist yet -- there is no build
step yet (BRIEF §1 steps 5-6 are later tasks).

Source-side detection (BRIEF §3: "source is deferred to a later project") is not
implemented here. A pipeline whose destination isn't matterbeam is rejected outright,
even if it might one day resolve as an emitter (`source=matterbeam`).
"""

import os
import runpy
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator, Optional

MATTERBEAM_DESTINATION_TYPE = "dlt_matterbeam.destinations.matterbeam"
"""`Pipeline.destination.destination_type` for the real factory (A3, A11) -- `<plugin
module>.destinations.<name>`."""


class _RunIntercepted(Exception):
    """Raised by the patched `Pipeline.run` so a candidate script never reaches a real
    destination or source while we inspect it (A11, "load-bearing correction": `.run()`
    round-trips the destination before `.extract()` ever starts, so the patch has to sit
    at `.run()`'s own entry, not any method it calls)."""


class MatterbeamGateError(Exception):
    """Raised when a pipeline script fails the upload gate."""


@dataclass
class GateResult:
    outcome: str
    """One of: "collector" (destination=matterbeam), "not_matterbeam" (resolved to a
    different destination), "no_destination" (pipeline constructed, no destination
    resolved), "unresolved" (destination name didn't resolve -- e.g. a missing
    dependency; A11 is explicit this must stay distinct from "not matterbeam"), or
    "no_pipeline" (the script never constructed a dlt pipeline, or doesn't exist)."""
    destination_type: Optional[str] = None
    detail: Optional[str] = None


@contextmanager
def _run_intercepted(pipeline_script_path: str) -> Iterator["Container"]:  # type: ignore[name-defined]
    """Runs `pipeline_script_path` far enough to resolve its dlt objects, per A11's
    demonstrated harness: an ephemeral `DLT_DATA_DIR` for this invocation only (so a
    prior local run's leftover state can't leak a destination into this check), and a
    patch on `Pipeline.run` at entry (never `.extract()`) so the script is stopped
    before it can contact anything real. Yields the dlt `Container` so a caller can
    inspect `PipelineContext` afterward; any exception raised inside the `with` block
    (e.g. `UnknownDestinationModule`) is left for the caller to handle, not swallowed
    here -- this helper only owns setup/teardown.

    Shared by `inspect_destination` (pass-1 gate classification) and
    `open_collector_pipeline` (deploy.py's route into the same live `Pipeline` object,
    once the gate has already confirmed it's a collector) so the interception mechanics
    exist in exactly one place.
    """
    # imported lazily: this module must stay importable even when a customer's own
    # pipeline dependencies (installed for the script we're about to inspect) aren't
    # present yet in whatever environment merely imports dlt_matterbeam.
    from dlt.common.configuration.container import Container
    from dlt.common.pipeline import PipelineContext
    from dlt.pipeline.pipeline import Pipeline

    container = Container()
    if PipelineContext in container:
        container[PipelineContext].deactivate()

    original_run = Pipeline.run

    def _intercepted_run(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise _RunIntercepted()

    Pipeline.run = _intercepted_run  # type: ignore[method-assign]
    old_data_dir = os.environ.get("DLT_DATA_DIR")
    try:
        with tempfile.TemporaryDirectory(prefix="dlt-matterbeam-gate-") as tmp_dir:
            os.environ["DLT_DATA_DIR"] = tmp_dir
            try:
                runpy.run_path(pipeline_script_path, run_name="__main__")
            except _RunIntercepted:
                pass
            yield container
    finally:
        Pipeline.run = original_run
        if old_data_dir is None:
            os.environ.pop("DLT_DATA_DIR", None)
        else:
            os.environ["DLT_DATA_DIR"] = old_data_dir
        if PipelineContext in container:
            container[PipelineContext].deactivate()


def inspect_destination(pipeline_script_path: str) -> GateResult:
    """Pass-1 gate classification -- see `_run_intercepted` for the mechanics."""
    from dlt.common.destination.exceptions import UnknownDestinationModule
    from dlt.common.pipeline import PipelineContext

    if not os.path.isfile(pipeline_script_path):
        return GateResult(outcome="no_pipeline", detail=f"no such file: {pipeline_script_path!r}")

    try:
        with _run_intercepted(pipeline_script_path) as container:
            # NOTE: `PipelineContext.pipeline()` lazily builds and activates a *default*
            # pipeline if none is active (dlt's own convenience for e.g. `dlt.run(data)`
            # at module level) -- calling it unconditionally would misreport a script
            # that built no pipeline at all as one that built a destination-less
            # pipeline. `is_active()` checks `_pipeline is not None` directly, with no
            # such side effect.
            if PipelineContext not in container or not container[PipelineContext].is_active():
                return GateResult(
                    outcome="no_pipeline",
                    detail="script ran without constructing a dlt pipeline",
                )

            pipeline = container[PipelineContext].pipeline()
            destination = pipeline.destination
            if destination is None:
                return GateResult(outcome="no_destination")

            destination_type = destination.destination_type
            if destination_type == MATTERBEAM_DESTINATION_TYPE:
                return GateResult(outcome="collector", destination_type=destination_type)
            return GateResult(outcome="not_matterbeam", destination_type=destination_type)
    except UnknownDestinationModule as ex:
        return GateResult(outcome="unresolved", detail=str(ex))


def run_gate(pipeline_script_path: str) -> GateResult:
    """Pass-1 entry point for `dlt matterbeam deploy`. Returns the `GateResult` for a
    recognized collector; raises `MatterbeamGateError` with an actionable message for
    everything else."""
    result = inspect_destination(pipeline_script_path)

    if result.outcome == "no_pipeline":
        raise MatterbeamGateError(
            f"{pipeline_script_path!r} did not construct a dlt pipeline "
            f"({result.detail}) -- nothing to deploy."
        )
    if result.outcome == "unresolved":
        raise MatterbeamGateError(
            "could not resolve the pipeline's destination "
            f"({result.detail}). Is every dependency the pipeline needs installed in "
            "this environment? Run the pipeline locally first."
        )
    if result.outcome in ("no_destination", "not_matterbeam"):
        seen = result.destination_type or "no destination"
        raise MatterbeamGateError(
            "dlt matterbeam deploy requires the pipeline's destination to be "
            f'"matterbeam" -- found {seen} instead. (A pipeline with matterbeam as '
            "its *source* -- an emitter -- will also be accepted once that support "
            "ships; it is not yet built.) Set destination=\"matterbeam\" (or a "
            "matterbeam() factory instance) and try again."
        )
    assert result.outcome == "collector"
    return result


@contextmanager
def open_collector_pipeline(pipeline_script_path: str) -> Iterator["Pipeline"]:  # type: ignore[name-defined]
    """Deploy-time entry point (`deploy.py`'s pid handoff / secrets / packaging steps): raises
    the same `MatterbeamGateError` `run_gate` does for anything that isn't a recognized
    collector, then re-runs the identical harness (`_run_intercepted`) a second time so the
    caller gets the live, still-constructed `Pipeline` object -- `pipeline_name`/`dataset_name`
    are literal constructor arguments the script passed, so they're correct regardless of the
    ephemeral `DLT_DATA_DIR` this runs under; `pipeline.destination_client()` similarly resolves
    real config with no network I/O in `__init__`. Running the harness twice (once inside
    `run_gate`, once here) costs one extra local import -- cheap, and it keeps gate
    classification and deploy-info extraction as two separately testable concerns instead of
    threading a live object out of `run_gate`'s own return contract.

    A fresh local run's *trace* (needed for secret-name recovery, R10) is deliberately NOT
    available from this pipeline object -- the ephemeral `DLT_DATA_DIR` here means `.run()`
    never actually executes, so `last_trace` is `None`. Secret recovery instead attaches to the
    customer's real, previously-run pipeline state (`deploy.py`'s `recover_secrets`), mirroring
    `dlt deploy`'s own `dlt.attach` + `get_state_and_trace` (R08 §2).
    """
    run_gate(pipeline_script_path)  # raises MatterbeamGateError for anything but a collector

    from dlt.common.pipeline import PipelineContext

    with _run_intercepted(pipeline_script_path) as container:
        yield container[PipelineContext].pipeline()
