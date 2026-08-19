"""A stock dlt merge resource, through the real destination, folded back from disk (D3/D5/H1)."""

import json

from dlt_matterbeam import crf


def _fold(records, key_fields):
    state = {}
    for _rid, rec in records:
        k = ",".join(str(rec.get(f)) for f in key_fields)
        if rec["mb.metadata"].get("is_tombstone"):
            state.pop(k, None)
        else:
            state[k] = rec
    return state


def _all_records(output_dir, recordtype_id):
    import glob
    import os

    records = []
    for path in sorted(glob.glob(os.path.join(output_dir, "crf_v2", recordtype_id, "*.zst"))):
        for rid, _rt, data in crf.read_segment(path):
            records.append((rid, json.loads(data)))
    return records


def test_merge_with_hard_delete_folds_correctly(pipeline_factory):
    import dlt

    pipeline = pipeline_factory(dataset_name="shop")

    @dlt.resource(
        name="customers",
        write_disposition="merge",
        primary_key="id",
        columns={"deleted": {"hard_delete": True}},
    )
    def customers_run1():
        yield [
            {"id": 1, "name": "ada", "deleted": False},
            {"id": 2, "name": "bob", "deleted": False},
            {"id": 3, "name": "cy", "deleted": False},
        ]

    @dlt.resource(
        name="customers",
        write_disposition="merge",
        primary_key="id",
        columns={"deleted": {"hard_delete": True}},
    )
    def customers_run2():
        yield [
            {"id": 2, "name": "bob", "deleted": False, "plan": "enterprise"},
            {"id": 3, "name": "cy", "deleted": True},
        ]

    pipeline.run(customers_run1())
    pipeline.run(customers_run2())

    output_dir = pipeline.destination.config_params.get("output_dir")
    records = _all_records(output_dir, "shop.customers")
    assert len(records) == 5  # 3 + 1 update + 1 tombstone

    state = _fold(records, ["id"])
    assert set(state) == {"1", "2"}
    assert state["2"]["plan"] == "enterprise"
    assert state["1"]["name"] == "ada"

    tombstones = [r for _, r in records if r["mb.metadata"].get("is_tombstone")]
    assert len(tombstones) == 1
    assert set(tombstones[0]) == {"id", "mb.metadata"}  # stripped to key fields (D3)

    for _, r in records:
        assert not any(k.startswith("_dlt_") for k in r)  # D9: promoted to metadata, not body
        assert r["mb.metadata"].get("source_record_id")  # A17 §3a


def test_scd2_is_refused_end_to_end(pipeline_factory):
    import dlt
    from dlt.pipeline.exceptions import PipelineStepFailed

    pipeline = pipeline_factory(dataset_name="shop")

    @dlt.resource(name="hist", write_disposition={"disposition": "merge", "strategy": "scd2"}, primary_key="id")
    def hist():
        yield [{"id": 1, "v": "a"}]

    try:
        pipeline.run(hist())
        assert False, "expected scd2 to be refused"
    except PipelineStepFailed:
        pass


def test_replace_degrades_to_append_across_two_runs(pipeline_factory, caplog):
    import dlt

    pipeline = pipeline_factory(dataset_name="shop")

    @dlt.resource(name="dims", write_disposition="replace", primary_key="id")
    def dims():
        yield [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]

    pipeline.run(dims())
    pipeline.run(dims())

    output_dir = pipeline.destination.config_params.get("output_dir")
    records = _all_records(output_dir, "shop.dims")
    assert len(records) == 4  # both runs' rows remain -- no truncation marker exists (B8)
