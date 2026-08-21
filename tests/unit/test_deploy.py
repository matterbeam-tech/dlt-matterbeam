"""Pid handoff, secret submission, package upload via presigned S3.
`DeployClient`/`build_package` are tested directly against the fake server; the full
`deploy()` orchestration is tested against a pipeline script that really ran once locally
(mirroring how a customer would invoke `dlt matterbeam deploy` after developing their
pipeline)."""

import itertools
import os
import runpy
import tarfile

import pytest
import yaml
from dlt_matterbeam.deploy import (
    DeployClient,
    DeployError,
    build_package,
    deploy,
    poll_build_status,
    recover_secrets,
    resolve_standalone_credentials,
)

_counter = itertools.count()

SCRIPT_TEMPLATE = """
import dlt

from dlt_matterbeam.destinations import matterbeam

@dlt.source
def my_source(api_key: str = dlt.secrets.value):
    @dlt.resource(name="items")
    def items():
        yield [{{"id": 1, "used_key": api_key}}]
    return items

pipeline = dlt.pipeline(
    pipeline_name="{name}",
    destination=matterbeam(transport="http", matterbeam_url="{base_url}"),
    dataset_name="ds",
    pipelines_dir="{pipelines_dir}",
    progress=None,
)

if __name__ == "__main__":
    pipeline.run(my_source())
"""


def _write_and_run_once(tmp_path, base_url: str, monkeypatch) -> tuple[str, str]:
    """Writes a matterbeam-collector script whose `api_token` is left to config injection
    (never passed in code -- secrets passed in code can't be read back for redeployment)
    and actually executes it once, for real -- exactly the "pipeline already ran successfully
    locally" precondition `dlt matterbeam deploy` (and `dlt deploy`) require. Also carries a
    *source*-level secret (`my_source`'s `api_key`) so tests can assert the thing the
    destination's own token exclusion (below) must not also swallow: a real source credential
    a hosted pipeline genuinely needs. Returns `(script_path, source_secret_env_key)`.
    """
    n = next(_counter)
    module_name = f"deploy_case_{n}"
    script_path = tmp_path / f"{module_name}.py"
    pipelines_dir = tmp_path / f"pipelines_{n}"
    source_secret_env_key = f"SOURCES__{module_name.upper()}__MY_SOURCE__API_KEY"
    monkeypatch.setenv(source_secret_env_key, "the-source-secret")
    script_path.write_text(
        SCRIPT_TEMPLATE.format(name=module_name, base_url=base_url, pipelines_dir=str(pipelines_dir))
    )
    runpy.run_path(str(script_path), run_name="__main__")
    return str(script_path), source_secret_env_key


@pytest.fixture(autouse=True)
def _api_token_env(monkeypatch):
    # The destination's own secret field, resolved from the environment (never passed
    # in code) -- so both the real local run and `recover_secrets`'s later re-attach resolve
    # the same value the same way. Its exclusion from what gets submitted is exactly what
    # test_recover_secrets_excludes_the_matterbeam_destinations_own_token asserts.
    monkeypatch.setenv("DESTINATION__MATTERBEAM__API_TOKEN", "fake-token")


# ---------------------------------------------------------------------------- build_package


def test_build_package_excludes_secrets_and_matches_manifest(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    (project / "pipeline.py").write_text("print('hi')\n")
    (project / "requirements.txt").write_text("dlt\n")
    dlt_dir = project / ".dlt"
    dlt_dir.mkdir()
    (dlt_dir / "config.toml").write_text("x = 1\n")
    (dlt_dir / "secrets.toml").write_text("api_token = 'super-secret'\n")
    pycache = project / "__pycache__"
    pycache.mkdir()
    (pycache / "pipeline.cpython-312.pyc").write_bytes(b"\x00\x01")

    output_path = str(tmp_path / "out.tar.gz")
    built = build_package(str(project / "pipeline.py"), output_path)

    with tarfile.open(built.path, "r:gz") as tar:
        names = tar.getnames()
        manifest = yaml.safe_load(tar.extractfile("manifest.yaml").read())

    assert "files/pipeline.py" in names
    assert "files/.dlt/config.toml" in names
    assert not any("secrets.toml" in n for n in names)
    assert not any("__pycache__" in n for n in names)

    manifest_paths = {f["relative_path"] for f in manifest["files"]}
    assert manifest_paths == {"pipeline.py", "requirements.txt", ".dlt/config.toml"}
    assert manifest["entry_script"] == "pipeline.py"  # which file the runtime task runs

    # deterministic: building twice from the same inputs produces the same content hash
    built_again = build_package(str(project / "pipeline.py"), str(tmp_path / "out2.tar.gz"))
    assert built_again.content_hash == built.content_hash


# ---------------------------------------------------------------------------- DeployClient


def test_register_hosted_pid_is_idempotent(fake_server):
    base_url, state = fake_server
    client = DeployClient(base_url, "fake-token")

    pid1 = client.register_hosted_pid("pipeline_a", "ds")
    pid2 = client.register_hosted_pid("pipeline_a", "ds")

    assert pid1 == pid2
    assert state.pids[pid1]["collector_type"] == "hosted_dlt"
    assert state.pids[pid1]["execution"] == "hosted"
    assert state.pids[pid1]["runtime"] == "ECS"


def test_register_hosted_pid_adopts_an_existing_external_claim_in_place(fake_server):
    """A claim already resolved to an external_dlt collector is transitioned, not
    duplicated -- the handoff falls out of one pid, one process_state."""
    base_url, state = fake_server
    external_pid, created = state.register("pipeline_b", "ds2", "external_dlt")
    assert created
    assert state.pids[external_pid]["execution"] == "external"

    client = DeployClient(base_url, "fake-token")
    hosted_pid = client.register_hosted_pid("pipeline_b", "ds2")

    assert hosted_pid == external_pid  # adopted, not a second pid
    assert state.pids[hosted_pid]["collector_type"] == "hosted_dlt"
    assert state.pids[hosted_pid]["execution"] == "hosted"
    assert state.pids[hosted_pid]["runtime"] == "ECS"
    assert len(state.pids) == 1


def test_submit_secrets_lands_encrypted_not_verbatim(fake_server):
    base_url, state = fake_server
    client = DeployClient(base_url, "fake-token")
    pid = client.register_hosted_pid("pipeline_c", "ds3")

    client.submit_secrets(pid, {"SOURCES__MY_SOURCE__API_KEY": "s3cr3t"})

    stored = state.pids[pid]["secret"]["SOURCES__MY_SOURCE__API_KEY"]
    assert stored != "s3cr3t"  # never verbatim
    assert state.decrypt_secret(pid, "SOURCES__MY_SOURCE__API_KEY") == "s3cr3t"


def test_submit_secrets_is_a_noop_for_an_empty_map(fake_server):
    base_url, state = fake_server
    client = DeployClient(base_url, "fake-token")
    pid = client.register_hosted_pid("pipeline_noop", "ds")
    client.submit_secrets(pid, {})
    assert state.pids[pid]["secret"] == {}


def test_create_upload_url_and_upload_package_round_trip(fake_server, tmp_path):
    base_url, state = fake_server
    client = DeployClient(base_url, "fake-token")
    pid = client.register_hosted_pid("pipeline_d", "ds4")

    package_path = tmp_path / "pkg.tar.gz"
    package_path.write_bytes(b"fake-tarball-bytes")

    upload = client.create_upload_url(pid, "pkg.tar.gz")
    assert upload["key"] == f"deployments/dlt/{pid}/pkg.tar.gz"

    client.upload_package(upload["upload_url"], str(package_path))

    assert state.uploaded_packages[pid] == b"fake-tarball-bytes"


# ------------------------------------------------------------------------------------- build


def test_trigger_build_and_get_build_status_round_trip(fake_server):
    base_url, state = fake_server
    client = DeployClient(base_url, "fake-token")
    pid = client.register_hosted_pid("pipeline_build", "ds_build")

    assert client.get_build_status(pid)["build_status"] == "never_built"

    triggered = client.trigger_build(pid)

    assert triggered is True
    assert client.get_build_status(pid)["build_status"] == "building"


# --------------------------------------------------------------------- poll_build_status


def test_poll_build_status_stops_early_on_a_terminal_status(fake_server):
    """A build that's already `ready` (or `failed`) by the first check must not
    burn through the remaining attempts/sleeps."""
    base_url, state = fake_server
    client = DeployClient(base_url, "fake-token")
    pid = client.register_hosted_pid("pipeline_poll", "ds_poll")
    client.trigger_build(pid)
    state.pids[pid]["build_status"] = "ready"
    state.pids[pid]["current_package_hash"] = "hash-1"

    sleeps = []
    status = poll_build_status(client, pid, max_attempts=5, interval_seconds=1.0, sleep=sleeps.append)

    assert status["build_status"] == "ready"
    assert sleeps == []  # never slept -- resolved on the very first check


def test_poll_build_status_gives_up_after_max_attempts(fake_server):
    base_url, state = fake_server
    client = DeployClient(base_url, "fake-token")
    pid = client.register_hosted_pid("pipeline_poll_2", "ds_poll_2")
    client.trigger_build(pid)  # fake server never advances past "building" on its own

    updates = []
    sleeps = []
    status = poll_build_status(
        client, pid, max_attempts=3, interval_seconds=0.5, on_update=updates.append, sleep=sleeps.append
    )

    assert status["build_status"] == "building"
    assert len(updates) == 3
    assert sleeps == [0.5, 0.5]  # slept between attempts, not after the last one


# --------------------------------------------------------------- resolve_standalone_credentials


def test_resolve_standalone_credentials_reads_env_vars(monkeypatch):
    monkeypatch.setenv("DESTINATION__MATTERBEAM__MATTERBEAM_URL", "http://example.test")
    monkeypatch.setenv("DESTINATION__MATTERBEAM__API_TOKEN", "a-token")

    matterbeam_url, api_token = resolve_standalone_credentials()

    assert matterbeam_url == "http://example.test"
    assert api_token == "a-token"


def test_resolve_standalone_credentials_raises_a_clear_error_when_matterbeam_url_missing(monkeypatch):
    monkeypatch.delenv("DESTINATION__MATTERBEAM__MATTERBEAM_URL", raising=False)

    with pytest.raises(DeployError, match="matterbeam_url"):
        resolve_standalone_credentials()


# ---------------------------------------------------------------------------- recover_secrets


def test_recover_secrets_reads_real_values_after_a_prior_local_run(fake_server, tmp_path, monkeypatch):
    base_url, _state = fake_server
    script_path, source_secret_env_key = _write_and_run_once(tmp_path, base_url, monkeypatch)

    secrets = recover_secrets(script_path)

    by_key = {s.key: s.value for s in secrets}
    assert by_key.get(source_secret_env_key) == "the-source-secret"


def test_recover_secrets_excludes_the_matterbeam_destinations_own_token(fake_server, tmp_path, monkeypatch):
    """Once uploaded, a hosted pipeline is already authenticated with Matterbeam for its own
    internal calls (BEAMIX_PID identity, and the in-runtime `[internal-log]` transport makes
    no HTTP call at all) -- it never needs its own `api_token` back as a "secret." Secret
    recovery is scoped to *source* credentials, not the destination's own auth."""
    base_url, _state = fake_server
    script_path, _source_secret_env_key = _write_and_run_once(tmp_path, base_url, monkeypatch)

    secrets = recover_secrets(script_path)

    keys = {s.key for s in secrets}
    assert "DESTINATION__MATTERBEAM__API_TOKEN" not in keys


def test_recover_secrets_requires_a_prior_run(tmp_path):
    script_path = tmp_path / "never_run.py"
    script_path.write_text("""
import dlt

from dlt_matterbeam.destinations import matterbeam

pipeline = dlt.pipeline(
    pipeline_name="never_run_case",
    destination=matterbeam(transport="http", matterbeam_url="http://127.0.0.1:1"),
    dataset_name="ds",
    pipelines_dir="{pipelines_dir}",
    progress=None,
)

if __name__ == "__main__":
    pipeline.run([{{"id": 1}}], table_name="items")
""".format(pipelines_dir=str(tmp_path / "pipelines_never_run")))

    with pytest.raises(DeployError, match="run successfully at least once locally"):
        recover_secrets(str(script_path))


# ---------------------------------------------------------------------------- deploy()


def test_deploy_end_to_end_against_fake_server(fake_server, tmp_path, monkeypatch):
    base_url, state = fake_server
    script_path, source_secret_env_key = _write_and_run_once(tmp_path, base_url, monkeypatch)

    # The fake server's trigger_build leaves build_status at "building" forever (no async
    # worker) -- a single, zero-wait poll attempt is enough to exercise the wiring without
    # this test paying real wall-clock time for a poll loop that could never resolve.
    result = deploy(script_path, poll_max_attempts=1, poll_interval_seconds=0)

    assert result.build_triggered is True
    assert result.build_status == "building"
    assert state.pids[result.pid]["build_status"] == "building"
    assert result.pid in state.pids
    assert state.pids[result.pid]["collector_type"] == "hosted_dlt"
    assert result.secret_count >= 1
    assert state.decrypt_secret(result.pid, source_secret_env_key) == "the-source-secret"
    # the destination's own credential never gets submitted at all (not just unencrypted)
    assert "DESTINATION__MATTERBEAM__API_TOKEN" not in state.pids[result.pid]["secret"]

    uploaded = state.uploaded_packages[result.pid]
    assert uploaded  # bytes actually landed server-side, not just a 200

    import io

    with tarfile.open(fileobj=io.BytesIO(uploaded), mode="r:gz") as tar:
        names = tar.getnames()
    assert "manifest.yaml" in names
    assert not any("secrets.toml" in n for n in names)


def test_deploy_rejects_a_pipeline_with_no_configured_matterbeam_url(tmp_path):
    script_path = tmp_path / "no_base_url.py"
    script_path.write_text("""
import dlt

from dlt_matterbeam.destinations import matterbeam

pipeline = dlt.pipeline(
    pipeline_name="no_base_url_case",
    destination=matterbeam(),
    dataset_name="ds",
)
""")

    with pytest.raises(DeployError, match="matterbeam_url"):
        deploy(str(script_path))
