"""Resolving `destination="matterbeam"` by name on unmodified, pypi-installed dlt."""

import dlt


def test_destination_resolves_by_name(pipeline_factory):
    pipeline = dlt.pipeline(pipeline_name="p_entrypoint", destination="matterbeam", dataset_name="ds")
    assert type(pipeline.destination).__name__ == "matterbeam"


def test_client_class_is_our_job_client(pipeline_factory):
    pipeline = pipeline_factory()
    client_class = pipeline.destination.client_class
    assert client_class.__name__ == "MatterbeamJobClient"
