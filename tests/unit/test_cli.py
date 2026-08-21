"""`dlt matterbeam` CLI registration and commands: pid/secrets/upload wiring in `_deploy`."""

import argparse
import runpy

import pytest
from dlt_matterbeam.cli import MatterbeamCommand
from dlt_matterbeam.gate import MatterbeamGateError

MATTERBEAM_SCRIPT = """
import dlt

pipeline = dlt.pipeline(pipeline_name="{name}", destination="matterbeam", dataset_name="ds")
"""

DUCKDB_SCRIPT = """
import dlt

pipeline = dlt.pipeline(pipeline_name="{name}", destination="duckdb", dataset_name="ds")
"""

HOSTED_MATTERBEAM_SCRIPT = """
import dlt

from dlt_matterbeam.destinations import matterbeam

pipeline = dlt.pipeline(
    pipeline_name="{name}",
    destination=matterbeam(transport="http", matterbeam_url="{base_url}", api_token="fake-token"),
    dataset_name="ds",
    pipelines_dir="{pipelines_dir}",
    progress=None,
)

if __name__ == "__main__":
    pipeline.run([{{"id": 1}}], table_name="items")
"""


def test_plug_cli_registers_matterbeam_command():
    from dlt_matterbeam.__plugins__ import plug_cli_matterbeam

    assert plug_cli_matterbeam("dlt") is MatterbeamCommand


def test_configure_parser_exposes_deploy_and_status():
    command = MatterbeamCommand()
    parser = argparse.ArgumentParser()
    command.configure_parser(parser)

    deploy_args = parser.parse_args(["deploy", "some_script.py"])
    assert deploy_args.matterbeam_command == "deploy"
    assert deploy_args.pipeline_script_path == "some_script.py"

    status_args = parser.parse_args(["status", "pid-123"])
    assert status_args.matterbeam_command == "status"
    assert status_args.pid == "pid-123"


def test_execute_deploy_accepts_a_collector(tmp_path, capsys):
    """Gate-only check (no destination credentials configured): recognized as a collector, then
    fails cleanly at the pid-handoff step since there's no matterbeam_url to call out to."""
    script = tmp_path / "mb_case.py"
    script.write_text(MATTERBEAM_SCRIPT.format(name="cli_mb_case"))

    command = MatterbeamCommand()
    command.configure_parser(argparse.ArgumentParser())
    args = argparse.Namespace(matterbeam_command="deploy", pipeline_script_path=str(script))

    with pytest.raises(MatterbeamGateError, match="matterbeam_url"):
        command.execute(args)

    out = capsys.readouterr().out
    assert "recognized as a collector" in out


def test_execute_deploy_runs_pid_secrets_and_upload(tmp_path, capsys, fake_server, monkeypatch):
    """End to end through the CLI -- pid handoff, secret submission, package upload, and one
    build-status poll (the fake server's `trigger_build` leaves it at "building" forever, so a
    real poll loop would never resolve -- shrink both the attempt count and interval so this
    test doesn't pay real wall-clock time for that)."""
    import dlt_matterbeam.cli as cli_module

    monkeypatch.setattr(cli_module, "_DEPLOY_POLL_MAX_ATTEMPTS", 1)
    monkeypatch.setattr(cli_module, "_DEPLOY_POLL_INTERVAL_SECONDS", 0)

    base_url, state = fake_server
    script = tmp_path / "hosted_case.py"
    pipelines_dir = tmp_path / "pipelines"
    script.write_text(
        HOSTED_MATTERBEAM_SCRIPT.format(name="cli_hosted_case", base_url=base_url, pipelines_dir=str(pipelines_dir))
    )
    runpy.run_path(str(script), run_name="__main__")  # the customer's prior local run

    command = MatterbeamCommand()
    command.configure_parser(argparse.ArgumentParser())
    args = argparse.Namespace(matterbeam_command="deploy", pipeline_script_path=str(script))

    command.execute(args)

    out = capsys.readouterr().out
    assert "recognized as a collector" in out
    assert "registered pid" in out
    assert "uploaded package" in out
    assert "building environment" in out
    assert "still building" in out  # the fake server's trigger_build never resolves to ready
    pid = next(iter(state.pids))
    assert state.pids[pid]["collector_type"] == "hosted_dlt"
    assert pid in state.uploaded_packages


def test_execute_deploy_rejects_non_matterbeam(tmp_path):
    script = tmp_path / "duck_case.py"
    script.write_text(DUCKDB_SCRIPT.format(name="cli_duck_case"))

    command = MatterbeamCommand()
    command.configure_parser(argparse.ArgumentParser())
    args = argparse.Namespace(matterbeam_command="deploy", pipeline_script_path=str(script))

    with pytest.raises(MatterbeamGateError, match="duckdb"):
        command.execute(args)


def test_execute_status_polls_build_status_using_env_credentials(capsys, fake_server, monkeypatch):
    """`status <pid>` has no pipeline script to recover credentials from, so it
    resolves them the same way any other dlt config value would -- env vars, here standing in
    for a customer's `.dlt/secrets.toml` (`resolve_standalone_credentials`)."""
    base_url, state = fake_server
    pid, _ = state.register("cli_status_case", "ds", collector_type="hosted_dlt")
    state.pids[pid]["build_status"] = "ready"
    state.pids[pid]["current_package_hash"] = "abc123"
    state.pids[pid]["installed_packages"] = {"dlt": "1.30.0"}

    monkeypatch.setenv("DESTINATION__MATTERBEAM__MATTERBEAM_URL", base_url)
    monkeypatch.setenv("DESTINATION__MATTERBEAM__API_TOKEN", "fake-token")

    command = MatterbeamCommand()
    command.configure_parser(argparse.ArgumentParser())
    args = argparse.Namespace(matterbeam_command="status", pid=pid)

    command.execute(args)

    out = capsys.readouterr().out
    assert "build ready" in out
    assert "abc123" in out
    assert "1 package(s) installed" in out


def test_execute_status_reports_a_failed_build(capsys, fake_server, monkeypatch):
    base_url, state = fake_server
    pid, _ = state.register("cli_status_failed_case", "ds", collector_type="hosted_dlt")
    state.pids[pid]["build_status"] = "failed"
    state.pids[pid]["build_error"] = "[build] boom"

    monkeypatch.setenv("DESTINATION__MATTERBEAM__MATTERBEAM_URL", base_url)
    monkeypatch.setenv("DESTINATION__MATTERBEAM__API_TOKEN", "fake-token")

    command = MatterbeamCommand()
    command.configure_parser(argparse.ArgumentParser())
    args = argparse.Namespace(matterbeam_command="status", pid=pid)

    command.execute(args)

    out = capsys.readouterr().out
    assert "build FAILED" in out
    assert "[build] boom" in out


def test_execute_status_without_credentials_raises_a_clear_error(monkeypatch):
    monkeypatch.delenv("DESTINATION__MATTERBEAM__MATTERBEAM_URL", raising=False)
    monkeypatch.delenv("DESTINATION__MATTERBEAM__API_TOKEN", raising=False)

    command = MatterbeamCommand()
    command.configure_parser(argparse.ArgumentParser())
    args = argparse.Namespace(matterbeam_command="status", pid="pid-123")

    with pytest.raises(MatterbeamGateError, match="matterbeam_url"):
        command.execute(args)
