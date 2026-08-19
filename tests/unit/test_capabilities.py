"""D2/D4/D7: these capabilities are NOT inherited from the sink -- extension point 2 must
declare them, or dlt applies its relational defaults and the rest of the design is void."""


def test_declared_capabilities(pipeline_factory):
    pipeline = pipeline_factory()
    caps = pipeline.destination.capabilities()
    assert caps.max_table_nesting == 0
    assert caps.naming_convention == "direct"
    assert caps.loader_parallelism_strategy == "sequential"
    assert caps.supported_merge_strategies == ["upsert", "insert-only", "delete-insert"]
    # D4: 0 is falsy and ignored by dlt's own loader -- must stay unset, not 0.
    assert caps.max_parallel_load_jobs is None
