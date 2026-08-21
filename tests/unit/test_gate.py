"""The upload gate: pass-1 destination detection."""

import pytest
from dlt_matterbeam.gate import GateResult, MatterbeamGateError, inspect_destination, run_gate

MATTERBEAM_SCRIPT = """
import dlt

pipeline = dlt.pipeline(pipeline_name="{name}", destination="matterbeam", dataset_name="ds")

if __name__ == "__main__":
    pipeline.run([{{"a": 1}}], table_name="items")
"""

DUCKDB_SCRIPT = """
import dlt

pipeline = dlt.pipeline(pipeline_name="{name}", destination="duckdb", dataset_name="ds")

if __name__ == "__main__":
    pipeline.run([{{"a": 1}}], table_name="items")
"""

NO_DESTINATION_SCRIPT = """
import dlt

pipeline = dlt.pipeline(pipeline_name="{name}", dataset_name="ds")
"""

UNRESOLVED_SCRIPT = """
import dlt

pipeline = dlt.pipeline(pipeline_name="{name}", destination="not_a_real_destination_xyz", dataset_name="ds")
"""

NO_PIPELINE_SCRIPT = """
x = 1 + 1
"""


def _write(tmp_path, name, template):
    path = tmp_path / f"{name}.py"
    path.write_text(template.format(name=name))
    return str(path)


def test_collector_recognized(tmp_path):
    script = _write(tmp_path, "mb_case", MATTERBEAM_SCRIPT)
    result = inspect_destination(script)
    assert result == GateResult(outcome="collector", destination_type="dlt_matterbeam.destinations.matterbeam")


def test_run_gate_returns_for_collector(tmp_path):
    script = _write(tmp_path, "mb_case2", MATTERBEAM_SCRIPT)
    result = run_gate(script)
    assert result.outcome == "collector"


def test_non_matterbeam_destination_rejected(tmp_path):
    script = _write(tmp_path, "duck_case", DUCKDB_SCRIPT)
    result = inspect_destination(script)
    assert result.outcome == "not_matterbeam"
    assert "duckdb" in result.destination_type

    with pytest.raises(MatterbeamGateError, match="duckdb"):
        run_gate(script)


def test_no_destination_rejected(tmp_path):
    script = _write(tmp_path, "no_dest_case", NO_DESTINATION_SCRIPT)
    result = inspect_destination(script)
    assert result.outcome == "no_destination"

    with pytest.raises(MatterbeamGateError, match="matterbeam"):
        run_gate(script)


def test_unresolved_destination_is_a_distinct_outcome(tmp_path):
    """ "failed to resolve" must not be folded into "not matterbeam"."""
    script = _write(tmp_path, "unresolved_case", UNRESOLVED_SCRIPT)
    result = inspect_destination(script)
    assert result.outcome == "unresolved"

    with pytest.raises(MatterbeamGateError, match="resolve"):
        run_gate(script)


def test_script_without_a_pipeline_rejected(tmp_path):
    script = _write(tmp_path, "no_pipeline_case", NO_PIPELINE_SCRIPT)
    result = inspect_destination(script)
    assert result.outcome == "no_pipeline"

    with pytest.raises(MatterbeamGateError, match="did not construct"):
        run_gate(script)


def test_missing_file_rejected(tmp_path):
    missing = str(tmp_path / "does_not_exist.py")
    result = inspect_destination(missing)
    assert result.outcome == "no_pipeline"

    with pytest.raises(MatterbeamGateError):
        run_gate(missing)


def test_stale_local_state_does_not_leak_across_checks(tmp_path):
    """Local working-directory state leaking a destination into an unrelated later
    check is a real, demonstrated failure mode -- the ephemeral DLT_DATA_DIR per
    invocation is what prevents it. Run the same pipeline_name once with matterbeam and
    once with duckdb; the second check must not see the first run's destination."""
    same_name_matterbeam = tmp_path / "leak_case.py"
    same_name_matterbeam.write_text(MATTERBEAM_SCRIPT.format(name="leak_case"))
    same_name_duckdb = tmp_path / "leak_case_2.py"
    same_name_duckdb.write_text(DUCKDB_SCRIPT.format(name="leak_case"))

    first = inspect_destination(str(same_name_matterbeam))
    assert first.outcome == "collector"

    second = inspect_destination(str(same_name_duckdb))
    assert second.outcome == "not_matterbeam"


def test_run_is_never_actually_called(tmp_path, monkeypatch):
    """The gate must never let the script's `.run()` actually execute."""
    from dlt.pipeline.pipeline import Pipeline

    called = []
    original_run = Pipeline.run

    def spy_run(self, *args, **kwargs):
        called.append(True)
        return original_run(self, *args, **kwargs)

    monkeypatch.setattr(Pipeline, "run", spy_run)
    script = _write(tmp_path, "no_actual_run_case", MATTERBEAM_SCRIPT)
    inspect_destination(script)
    assert called == []
