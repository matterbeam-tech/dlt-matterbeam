"""`MatterbeamLoadJob` streams a load file into chunks instead of reading it all first.

dlt writes one load file per table per load package, so for a large extract the file is
the whole table: reading it into a list before sending (1.0.0's behaviour) ran a hosted
pipeline out of memory at the very start of its load stage. These tests count how many
rows have been read from the file at the moment each chunk is sent.
"""

import json
import os

import pytest
from dlt_matterbeam import envelope
from dlt_matterbeam.client import MatterbeamJobClient


def _records(pipeline, table_name):
    output_dir = pipeline.destination.config_params.get("output_dir")
    path = os.path.join(output_dir, f"{pipeline.dataset_name}.{table_name}.jsonl")
    with open(path, "rb") as f:
        return [json.loads(line) for line in f if line.strip()]


@pytest.fixture
def reads_at_send(monkeypatch):
    """[(rows read from the load file so far, rows in this chunk)] for every chunk sent."""
    read = {"n": 0}
    sends: list[tuple[int, int]] = []
    original_iter = envelope.iter_stripped_rows
    original_send = MatterbeamJobClient.send_chunk

    def counting_iter(lines, columns):
        for row in original_iter(lines, columns):
            read["n"] += 1
            yield row

    def recording_send(self, **kwargs):
        sends.append((read["n"], len(kwargs["rows"])))
        return original_send(self, **kwargs)

    monkeypatch.setattr(envelope, "iter_stripped_rows", counting_iter)
    monkeypatch.setattr(MatterbeamJobClient, "send_chunk", recording_send)
    return sends


def test_chunks_are_sent_while_the_file_is_still_being_read(pipeline_factory, reads_at_send):
    pipeline = pipeline_factory(chunk_records=10)

    pipeline.run([{"id": i, "n": i} for i in range(95)], table_name="items", primary_key="id")

    item_sends = [s for s in reads_at_send if s[1]]
    # The first chunk goes out after reading 10 rows, not all 95
    assert item_sends[0] == (10, 10)
    assert [size for _, size in item_sends] == [10] * 9 + [5]
    assert sorted(r["id"] for r in _records(pipeline, "items")) == list(range(95))


def test_a_dedup_sort_table_still_orders_across_the_whole_file(pipeline_factory, reads_at_send):
    """Same-key ordering needs every row, so this one table type is still read in full."""
    import dlt

    @dlt.resource(
        name="events",
        primary_key="id",
        write_disposition="merge",
        columns={"version": {"dedup_sort": "asc"}},
    )
    def events():
        yield [{"id": 1, "version": 3}, {"id": 1, "version": 1}, {"id": 1, "version": 2}]

    pipeline = pipeline_factory(chunk_records=1)
    pipeline.run(events())

    assert [r["version"] for r in _records(pipeline, "events")] == [1, 2, 3]
    assert [s for s in reads_at_send if s[1]][0][0] == 3


def test_an_empty_job_still_sends_one_empty_chunk(pipeline_factory, reads_at_send):
    pipeline = pipeline_factory()

    pipeline.run([], table_name="nothing", columns={"id": {"data_type": "bigint"}})

    assert all(size == 0 for _, size in reads_at_send)
