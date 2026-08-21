"""These capabilities are NOT inherited from the sink -- the destination must declare
them, or dlt applies its relational defaults."""


def test_declared_capabilities(pipeline_factory):
    pipeline = pipeline_factory()
    caps = pipeline.destination.capabilities()
    assert caps.max_table_nesting == 0
    assert caps.naming_convention == "direct"
    assert caps.loader_parallelism_strategy == "sequential"
    assert caps.supported_merge_strategies == ["upsert", "insert-only", "delete-insert"]
    # 0 is falsy and ignored by dlt's own loader -- must stay unset, not 0.
    assert caps.max_parallel_load_jobs is None
