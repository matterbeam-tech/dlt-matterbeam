"""Phase 1 assertion 2 (design §6): declaring `supported_merge_strategies` makes `_dlt_id`
a deterministic hash of the primary key (A17 §3a) rather than random per extract -- verified
here in the real package, not just a probe."""

import json


def _dlt_ids_by_key(output_dir, recordtype_id):
    import os

    ids = {}
    path = os.path.join(output_dir, f"{recordtype_id}.jsonl")
    if not os.path.exists(path):
        return ids
    with open(path, "rb") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            ids.setdefault(rec["id"], []).append(rec["mb.metadata"]["source_record_id"])
    return ids


def test_dlt_id_is_byte_identical_across_two_runs_of_the_same_data(pipeline_factory):
    import dlt

    pipeline_a = pipeline_factory(dataset_name="ds")
    pipeline_b = pipeline_factory(dataset_name="ds")

    @dlt.resource(name="rows", write_disposition="merge", primary_key="id")
    def rows():
        yield [{"id": 1, "v": "x"}, {"id": 2, "v": "y"}]

    pipeline_a.run(rows())
    pipeline_b.run(rows())

    ids_a = _dlt_ids_by_key(pipeline_a.destination.config_params["output_dir"], "ds.rows")
    ids_b = _dlt_ids_by_key(pipeline_b.destination.config_params["output_dir"], "ds.rows")

    assert ids_a[1] == ids_b[1]
    assert ids_a[2] == ids_b[2]
    assert ids_a[1] != ids_a[2]


def test_dlt_id_is_stable_across_a_retry_of_the_same_job(pipeline_factory):
    """A11: a retry re-reads the same physical file, so `_dlt_id` must be identical --
    confirmed by running the same source through the same pipeline twice in one load-adjacent
    sequence and checking the ids for an unchanged row match, matching what the design cites
    as the retry-idempotency backstop (D10 layer 2)."""
    import dlt

    pipeline = pipeline_factory(dataset_name="ds")

    @dlt.resource(name="rows", write_disposition="merge", primary_key="id")
    def rows(which):
        yield [{"id": 1, "v": which}]

    pipeline.run(rows("first"))
    pipeline.run(rows("first"))  # identical row content -- same key, same value

    ids = _dlt_ids_by_key(pipeline.destination.config_params["output_dir"], "ds.rows")
    assert len(set(ids[1])) == 1
