import itertools
import os
import sys

import pytest

_counter = itertools.count()

FAKE_MATTERBEAM_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "fake_matterbeam"))
if os.path.dirname(FAKE_MATTERBEAM_DIR) not in sys.path:
    sys.path.insert(0, os.path.dirname(FAKE_MATTERBEAM_DIR))


@pytest.fixture
def fake_server(tmp_path):
    """One fake-Matterbeam server per test, its own coldlog directory. Yields
    (base_url, state) -- `state` exposes the counters/process_state a test asserts on."""
    from fake_matterbeam.server import start_server

    coldlog_root = str(tmp_path / "coldlog")
    server, state = start_server(coldlog_root)
    port = server.server_address[1]
    try:
        yield f"http://127.0.0.1:{port}", state
    finally:
        server.shutdown()


@pytest.fixture
def http_pipeline_factory(tmp_path, fake_server):
    """Builds an isolated dlt pipeline pointed at the fake server via `HttpTransport`."""
    import dlt
    from dlt_matterbeam.destinations import matterbeam

    matterbeam_url, state = fake_server

    def make(
        dataset_name: str = "ds", restore_from_destination: bool = True, pipeline_name: str = None, **destination_kwargs
    ):
        n = next(_counter)
        pipelines_dir = tmp_path / f"pipelines_{n}"
        destination_kwargs.setdefault("transport", "http")
        destination_kwargs.setdefault("matterbeam_url", matterbeam_url)
        destination_kwargs.setdefault("api_token", "fake-token")
        return dlt.pipeline(
            pipeline_name=pipeline_name or f"test_pipeline_{n}",
            destination=matterbeam(**destination_kwargs),
            dataset_name=dataset_name,
            pipelines_dir=str(pipelines_dir),
            progress=None,
            restore_from_destination=restore_from_destination,
        )

    return make, state


@pytest.fixture
def pipeline_factory(tmp_path):
    """Builds an isolated dlt pipeline: fresh pipelines_dir and a fresh matterbeam
    output_dir per call, and a unique pipeline_name so nothing collides even within one
    test process."""
    import dlt
    from dlt_matterbeam.destinations import matterbeam

    def make(dataset_name: str = "ds", **destination_kwargs):
        n = next(_counter)
        pipelines_dir = tmp_path / f"pipelines_{n}"
        output_dir = tmp_path / f"coldlog_{n}"
        destination_kwargs.setdefault("transport", "file")
        return dlt.pipeline(
            pipeline_name=f"test_pipeline_{n}",
            destination=matterbeam(output_dir=str(output_dir), **destination_kwargs),
            dataset_name=dataset_name,
            pipelines_dir=str(pipelines_dir),
            progress=None,
        )

    return make


def output_path_for(pipeline, table_name: str) -> str:
    output_dir = pipeline.destination_client(pipeline.default_schema).config.output_dir
    return os.path.join(output_dir, f"{pipeline.dataset_name}.{table_name}.jsonl")


def read_all_records(pipeline, table_name: str) -> list[dict]:
    import json

    path = output_path_for(pipeline, table_name)
    if not os.path.exists(path):
        return []
    with open(path, "rb") as f:
        return [json.loads(line) for line in f if line.strip()]
