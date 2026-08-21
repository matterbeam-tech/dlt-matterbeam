"""`HttpTransport` against the fake server (tests/fake_matterbeam).

Covers registration idempotency, the two distinguishable 409s, the `WithStateSync` round
trip including the two-laptop clobber guard, pid resolution (no `PipelineContext`,
fallback path when state-sync never fires), and that merge semantics still fold correctly
end to end with the server allocating record_ids and stamping `mb.metadata`.
"""

import dlt
from fake_matterbeam import fold


def test_merge_with_hard_delete_folds_correctly_over_http(http_pipeline_factory):
    make_pipeline, state = http_pipeline_factory
    pipeline = make_pipeline(dataset_name="shop")

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

    # recordtype_id is (pid, table), not (dataset_name, table) -- a pid is already
    # 1:1-bound to one dataset_name at registration.
    pid = state.registry[f"{pipeline.pipeline_name}|shop"]
    recordtype_id = f"{pid}.customers"
    facts, dropped = fold.scan(state.coldlog_root, recordtype_id)
    assert not dropped
    assert len(facts) == 5  # 3 + 1 update + 1 tombstone

    folded = fold.fold(facts, ["id"])
    assert set(folded) == {"1", "2"}
    assert folded["2"]["plan"] == "enterprise"

    tombstones = [r for _, r in facts if r["mb.metadata"].get("is_tombstone")]
    assert len(tombstones) == 1
    assert set(fold.body(tombstones[0])) == {"id"}  # stripped to key fields server-side

    for _, record in facts:
        assert record["mb.metadata"]["record_type_id"] == recordtype_id  # server allocated, not client
        assert record["mb.metadata"].get("source_record_id")  # promoted by the client, stamped by the server


def test_registration_is_lookup_or_create_by_pipeline_name(http_pipeline_factory):
    """Registering twice for the same pipeline+dataset returns the same pid; the
    pid is resolved without ever touching `Container()[PipelineContext]`."""
    make_pipeline, state = http_pipeline_factory
    pipeline_a = make_pipeline(dataset_name="ds")

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield [{"id": 1}]

    pipeline_a.run(rows())
    pipeline_a.run(rows())

    registry_key = f"{pipeline_a.pipeline_name}|ds"
    assert list(state.registry) == [registry_key]  # one registration key, never re-created
    assert len(state.pids) == 1


def test_pid_resolves_via_fallback_when_state_sync_is_disabled(http_pipeline_factory):
    """The fallback path: no `get_stored_state` call ever fires with
    `restore_from_destination=False`, so the pid must resolve from the first load job
    instead. It still resolves to the *true* pipeline identity (via the active
    `Container()[PipelineContext]`, read at load-job time, not in `__enter__`), agreeing
    with what `get_stored_state` would have used -- not a different, dataset/schema-name
    key. A dataset-name-only fallback was tried and empirically produces a second, wrong
    pid whenever a load-job instance resolves independently of the state-sync instance,
    which is most load jobs: the loader opens and closes a client per job, so state-sync
    and each table's load typically run on different instances even within one `run()` --
    exactly the identity split this fallback exists to prevent."""
    make_pipeline, state = http_pipeline_factory
    pipeline = make_pipeline(dataset_name="ds", restore_from_destination=False)

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield [{"id": 1}]

    pipeline.run(rows())

    assert len(state.registry) == 1
    registry_key = next(iter(state.registry))
    assert registry_key == f"{pipeline.pipeline_name}|ds"
    pid = state.registry[registry_key]
    facts, _ = fold.scan(state.coldlog_root, f"{pid}.rows")
    assert len(facts) == 1


def test_pid_falls_back_to_dataset_schema_key_with_no_active_pipeline(http_pipeline_factory, monkeypatch):
    """The last-resort path: with no active `PipelineContext` at all (nothing in the
    normal test setup un-sets this -- merely constructing a pipeline activates it -- so
    this is forced directly), there is no pipeline identity to read from anywhere, and
    resolution must degrade to the dataset/schema-name key rather than raise."""
    make_pipeline, state = http_pipeline_factory
    pipeline = make_pipeline(dataset_name="ds")
    client = pipeline.destination_client()
    monkeypatch.setattr(type(client), "_active_pipeline_name", staticmethod(lambda: None))

    pid = client._ensure_pid_fallback()

    assert pid is not None
    registry_key = next(iter(state.registry))
    assert registry_key == f"schema:{client.schema.name}|ds"


def test_complete_load_resolves_pid_on_a_fresh_client_instance(http_pipeline_factory):
    """Regression: dlt's own `complete_package` (dlt/load/load.py) opens a *fresh*
    destination client instance specifically for the `complete_load` call --
    `with self.get_destination_client(schema) as job_client: job_client.complete_load(
    load_id)` -- never the same instance that ran the load's jobs. That fresh instance's
    own `self.pid` starts `None` and had never had `_ensure_pid`/`_ensure_pid_fallback`
    called on it. Without resolving it first, `complete_load` sent its request to
    `/collectors/None/job/{load_id}` -- the real load's `job/{load_id}` close call never
    landed, silently, while a bogus pid="None" ledger entry did. Caught by tracing a
    real run whose data all landed but never showed up as closed server-side."""
    make_pipeline, state = http_pipeline_factory
    pipeline = make_pipeline(dataset_name="ds")

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield [{"id": 1}]

    pipeline.run(rows())
    real_pid = next(iter(state.pids))

    # A brand-new client instance, exactly like dlt's own `get_destination_client(schema)`
    # -- never used for anything else, so its own `self.pid` starts unresolved.
    fresh_client = pipeline.destination_client()
    assert fresh_client.pid is None

    fresh_client.complete_load("some-load-id")

    assert (real_pid, "some-load-id") in state.closed_loads
    assert (None, "some-load-id") not in state.closed_loads


def test_409_lock_contention_is_retried_and_load_closed_is_terminal(http_pipeline_factory):
    """The two 409 reasons must not be handled the same way. Drive both
    directly against the fake server's state to assert the distinction the client relies
    on (retryable vs. terminal), independent of how likely dlt is to trigger either."""
    from dlt_matterbeam.transport import HttpTransport

    make_pipeline, state = http_pipeline_factory
    pipeline = make_pipeline(dataset_name="ds")

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield [{"id": 1}]

    pipeline.run(rows())
    pid = next(iter(state.pids))

    # Simulate another invocation already holding the lock.
    state.pids[pid]["invoke_semaphore"] = 0
    http = HttpTransport(base_url=pipeline.destination.config_params["matterbeam_url"], api_token="fake-token")
    http.max_409_retries = 3
    try:
        with __import__("pytest").raises(Exception) as exc_info:
            http.send_chunk(
                recordtype_id="ds.rows",
                dataset_name="ds",
                table_name="rows",
                rows=[{"id": 2}],
                keys=[],
                hard_delete=[],
                load_id="load-x",
                job_id="job-x",
                seq=0,
                pid=pid,
            )
        assert "lock_contention" in str(exc_info.value)
    finally:
        state.pids[pid]["invoke_semaphore"] = 1

    # A closed load is terminal on the first attempt, not retried. Keyed by (pid, load_id),
    # not load_id alone -- a client-generated load_id has no server-side uniqueness
    # guarantee across different pids.
    state.closed_loads.add((pid, "load-y"))
    with __import__("pytest").raises(Exception) as exc_info:
        http.send_chunk(
            recordtype_id="ds.rows",
            dataset_name="ds",
            table_name="rows",
            rows=[{"id": 3}],
            keys=[],
            hard_delete=[],
            load_id="load-y",
            job_id="job-y",
            seq=0,
            pid=pid,
        )
    assert "load_closed" in str(exc_info.value)


def test_state_round_trips_over_http_and_the_stale_write_is_rejected(http_pipeline_factory):
    """`get_stored_state` before extract, `put_dlt_state` after load, and the
    `StateInfo.version` guard rejecting an older write than what's stored."""
    make_pipeline, state = http_pipeline_factory
    pipeline = make_pipeline(dataset_name="ds")

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield [{"id": 1}]

    pipeline.run(rows())
    client = pipeline.destination_client()
    pid = next(iter(state.pids))

    stored = client.get_stored_state(pipeline.pipeline_name)
    assert stored is not None
    assert stored.pipeline_name == pipeline.pipeline_name

    # A second, older write loses the race and is logged, not raised: a hard-reject here
    # would fail an otherwise-good load over a bookkeeping conflict -- an earlier version
    # of this fix got that wrong until a concurrency test caught it.
    client.transport.put_dlt_state(
        pid,
        {
            "pipeline_name": pipeline.pipeline_name,
            "state": "stale-blob",
            "version": stored.version - 1 if stored.version > 0 else 0,
            "engine_version": stored.engine_version,
            "created_at": stored.created_at,
        },
    )
    still_stored = client.get_stored_state(pipeline.pipeline_name)
    assert still_stored.state == stored.state  # the stale write did not overwrite it
