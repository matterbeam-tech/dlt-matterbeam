# dlt-matterbeam

A [dlt](https://dlthub.com) adapter for [Matterbeam](https://matterbeam.com). Point any
dlt pipeline at `destination="matterbeam"` and its rows land as facts in a Matterbeam dataset
(an immutable fact log).

`pip install`, set the destination, add the Matterbeam 
url for your instance and an API token, and run. The pipeline should run normally.
In the Matterbeam UI the pipeline will appear as a 'dlt collector' with stats,
schema, and normal Matterbeam functionality. All runs will show as executions
of the collector in the UI. The dataset will be available for replay, transformation
and emitting to different destinations.

This same package can also deploy the pipeline to run hosted
inside Matterbeam, via `dlt matterbeam deploy` (see below). 

```toml
# .dlt/secrets.toml
[destination.matterbeam]
matterbeam_url = "https://api.<customer>.matterbeam.com"
api_token = "<your API token>"
```

## Install

```bash
pip install dlt-matterbeam
```

## Quickstart

```python
import dlt

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="matterbeam")

@dlt.resource(primary_key="id", write_disposition="merge")
def users():
    yield {"id": 1, "name": "ada"}
    yield {"id": 2, "name": "bob"}

pipeline.run(users())
```

`destination="matterbeam"` resolves by name because the `dlt-matterbeam` package registers
itself with dlt's plugin system on install.

Transport defaults to `http`, so running this exact snippet requires the `matterbeam_url`/
`api_token` shown above. For local debugging/testing with no Matterbeam account, pass
`transport="file"` explicitly — it writes segments to a local directory instead
(`output_dir`, default `.dlt/matterbeam_coldlog`) and is not chosen implicitly.

## Deploy to Matterbeam

The same pipeline that runs on your machine can also run hosted inside Matterbeam's own
runtime, using Matterbeam for scheduling. The pipeline is unchanged
— it's the same pipeline from the Quickstart above.

```bash
dlt matterbeam deploy path/to/pipeline.py
```

This validates that the script's pipeline has `destination="matterbeam"`, packages the
script's directory and its dependencies, uploads it, and triggers a server-side build,
polling briefly for the result before returning. Secret values are read locally and
submitted separately — they are never included in the uploaded package.

Check build status again later without redeploying:

```bash
dlt matterbeam status <pid>
```

`status` resolves `destination.matterbeam.matterbeam_url` / `api_token` the same way the
pipeline itself does (env vars or `.dlt/secrets.toml`). Run it from the pipeline's own
directory, or export `DESTINATION__MATTERBEAM__MATTERBEAM_URL` and
`DESTINATION__MATTERBEAM__API_TOKEN`.

Two current limits: only a pipeline with `destination="matterbeam"` can be deployed
(a `source="matterbeam"` pipeline isn't supported yet); and if your account's
build service isn't available, `deploy` reports that rather than hanging.

## How is this different from a typical destination / warehouse?

Matterbeam is a fact log, not a relational warehouse. This destination keeps as much of
your source data intact as possible rather than making it look like a normal dlt
destination. See the note on nesting below for why.

**Table shape.** Nested objects and arrays are kept as-is, a single object, not
decomposed into child tables (`max_table_nesting=0`), and column names are passed through
unchanged (no snake_case mangling). **This is deliberate**, flattening *adds* structure
(synthetic parent/child keys) and *destroys* the original document boundary. This
processing, can be done later from the intact document. When `source=matterbeam`
is available, this processing can be done to land in destinations in standard dlt
format if desired.

**Write dispositions.**

| Disposition / strategy | What happens |
| --- | --- |
| `append`, keyed | Every row is a fact; last-value-wins per key downstream |
| `append`, unkeyed | Every row is a fact; an append-only event stream, no dedup |
| `merge` (no strategy, or `upsert`/`delete-insert`) | Rows are upserted by `primary_key`/`merge_key` |
| `merge` + `hard_delete` column, truthy | The row becomes a tombstone, body stripped to key fields |
| `merge` + `insert-only` | Degraded to upsert — existing keys are overwritten, not skipped |
| `replace` | **Degraded to `append`, with a warning.** Matterbeam has no truncation marker; the previous load's rows remain. On a keyed table this is nearly harmless (every key in the new load overwrites); on an unkeyed table the old rows are indistinguishable from new ones and linger. |
| `merge` + `scd2` | **Refused, with an error.** Matterbeam stores full history natively; retiring old rows by closing a validity window isn't something this destination can do from an append-only log. Use plain `merge` and query the log's history instead. |

**`_dlt_id` / `_dlt_load_id`.** Carried as record metadata, not left in your row data. Under
`upsert`/`insert-only`/`delete-insert` — which this destination always declares —
`_dlt_id` is a deterministic hash of the primary key, so retries and re-runs produce the
same id for the same row.

**Incremental state.** incremental cursors 
(`dlt.sources.incremental`) survive a fresh machine or a fresh container. `run()` reads
the cursor back from Matterbeam before extracting on every run. Schema
restore and `dlt pipeline sync`/`drop` currently not supported but coming soon.

**Concurrency and retries.** Running the pipeline from multiple machines at 
once (two machines, one pipeline name) is safe. One gets a 409 and retries automatically,
neither corrupts the other's data. A load that fails partway through and gets retried by
dlt is deduplicated; one edge case that isn't fully handled is
a **keyless `append`** table retried after a partial failure, which can land a permanent
duplicate row (keyed tables fold duplicates away for free).


## Requirements

Tested with python `>=3.10`. Works with released dlt `>=1.29,<1.31` from PyPI, unmodified. Deploying
requires the same `destination.matterbeam` credentials used above.

## License

Apache-2.0.
