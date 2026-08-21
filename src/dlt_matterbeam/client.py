"""MatterbeamJobClient: JobClientBase + WithStateSync.

Only the cursor half of `WithStateSync` is implemented: `get_stored_state` is backed by
a real server call, so incremental extraction survives a fresh machine.
`get_stored_schema` / `get_stored_schema_by_hash` return `None` -- there is no
server-side schema storage, so schema restore / `dlt pipeline sync`/`drop` don't work,
but extraction cursors do.
"""

from __future__ import annotations

from typing import Iterable, Optional

from dlt.common.destination.client import (
    JobClientBase,
    LoadJob,
    PreparedTableSchema,
    StateInfo,
    StorageSchemaInfo,
    WithStateSync,
)
from dlt.common.schema import Schema, TSchemaTables
from dlt_matterbeam.configuration import MatterbeamClientConfiguration
from dlt_matterbeam.load_job import MatterbeamLoadJob, MatterbeamStateJob
from dlt_matterbeam.transport import MatterbeamTransport, resolve_transport

# dlt's own bookkeeping tables never become facts in the log. `_dlt_version` and
# `_dlt_loads` carry write_disposition="skip" and never reach create_load_job at all;
# `_dlt_pipeline_state` does reach it, and is the one we intercept ourselves.
_DLT_STATE_TABLE = "_dlt_pipeline_state"


class MatterbeamJobClient(JobClientBase, WithStateSync):
    def __init__(
        self,
        schema: Schema,
        config: MatterbeamClientConfiguration,
        capabilities,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: MatterbeamClientConfiguration = config
        self.transport: MatterbeamTransport = resolve_transport(config.transport, config)
        self.pid: Optional[str] = None
        self._pid_resolved = False

    def __enter__(self) -> "MatterbeamJobClient":
        # dlt never binds `pipeline_name` onto the destination config (only
        # `dataset_name` is, `dlt/dataset/utils.py:30`) -- it is not available here.
        # Registration is deferred to `_ensure_pid`, called from
        # `get_stored_state(pipeline_name)` on the common path (that call happens
        # immediately after `__enter__`, before any extraction, so registration still
        # happens at the start of a run -- just not inside this method), or from the
        # first load job if state-sync never fires.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    # ------------------------------------------------------------------------- identity

    def _ensure_pid(self, pipeline_key: str) -> Optional[str]:
        if self._pid_resolved:
            return self.pid
        self.pid = self.transport.register(pipeline_key, self.config.dataset_name)
        self._pid_resolved = True
        return self.pid

    def _ensure_pid_fallback(self) -> Optional[str]:
        """Reached from a load job whenever *this* client instance was never the one
        `get_stored_state` ran on -- which is most load jobs: the loader opens and closes
        a client per job, so state-sync and each table's load typically run on different
        instances even within one `run()`.

        A per-dataset-name key here is unsound, not just imprecise: it produces a
        *second, wrong* pid the moment a load-job instance resolves before -- or
        independently of -- the state-sync instance, splitting one pipeline's identity.
        `Container()[PipelineContext]` gives the same true `pipeline_name`
        `get_stored_state` would have used, so this agrees with the primary path whenever
        a pipeline is actually running -- it is not a parallel identity scheme, it is the
        same one read a different way. It is used here rather than in `__enter__` because
        no pipeline is guaranteed active that early. This is a fallback of a fallback:
        `get_stored_state` first, this second, and only when truly nothing is running (a
        client built standalone, outside any `pipeline.run()`) does this drop to a
        dataset/schema-name key that cannot claim to be more than a last resort.
        """
        if self._pid_resolved:
            return self.pid
        return self._ensure_pid(self._active_pipeline_name() or f"schema:{self.schema.name}")

    @staticmethod
    def _active_pipeline_name() -> Optional[str]:
        from dlt.common.configuration.container import Container
        from dlt.common.pipeline import PipelineContext

        ctx = Container()[PipelineContext]
        return ctx.pipeline().pipeline_name if ctx.is_active() else None

    # ---------------------------------------------------------------------- transport

    def send_chunk(
        self,
        *,
        recordtype_id: str,
        dataset_name: str,
        table_name: str,
        rows: list[dict],
        keys: list[str],
        hard_delete: list[str],
        load_id: str,
        job_id: str,
        seq: int,
    ) -> str:
        self._ensure_pid_fallback()
        return self.transport.send_chunk(
            recordtype_id=recordtype_id,
            dataset_name=dataset_name,
            table_name=table_name,
            rows=rows,
            keys=keys,
            hard_delete=hard_delete,
            load_id=load_id,
            job_id=job_id,
            seq=seq,
            pid=self.pid,
        )

    def put_dlt_state(self, row: dict) -> None:
        self._ensure_pid_fallback()
        if self.pid is None:
            return
        doc = dict(row)
        if "_dlt_load_id" in doc:
            doc["dlt_load_id"] = doc.pop("_dlt_load_id")
        self.transport.put_dlt_state(self.pid, doc)

    # ------------------------------------------------------------- JobClientBase parts

    def initialize_storage(self, truncate_tables: Optional[Iterable[str]] = None) -> None:
        # dlt's only signal about `replace` -- which tables it means to truncate. There
        # is no truncation marker in Matterbeam, so `replace` degrades to `append`
        # (envelope.check_disposition warns per-table at load time); nothing to do here.
        pass

    def is_storage_initialized(self) -> bool:
        return True

    def drop_storage(self) -> None:
        pass

    def update_stored_schema(
        self,
        only_tables: Optional[Iterable[str]] = None,
        expected_update: Optional[TSchemaTables] = None,
        force: bool = False,
    ) -> Optional[TSchemaTables]:
        # No server-side schema storage (see module docstring) -- nothing to push.
        # Still call super() so dlt's own bookkeeping about what's "applied" stays
        # correct.
        return super().update_stored_schema(only_tables, expected_update, force)

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        if table["name"] == _DLT_STATE_TABLE:
            return MatterbeamStateJob(file_path)
        return MatterbeamLoadJob(file_path, table)

    def complete_load(self, load_id: str) -> None:
        # dlt's own `complete_package` (dlt/load/load.py) opens a *fresh* destination
        # client instance specifically for this call (`with self.get_destination_client
        # (schema) as job_client: job_client.complete_load(load_id)`) -- this instance
        # never had `_ensure_pid`/`_ensure_pid_fallback` called on it before now, so
        # `self.pid` is still `None` here unless resolved first. Without this, the
        # request goes to `/collectors/None/job/{load_id}` -- a real bug found by
        # tracing a live run whose data all landed correctly but never showed up as
        # "closed" server-side: the close call silently landed on a bogus `None` pid
        # instead of the real one, so the real load's audience-log section (and any
        # future retry-rejection via `load_closed`) never closed either.
        self._ensure_pid_fallback()
        self.transport.complete_load(load_id, self.pid)

    # -------------------------------------------------------------------- WithStateSync

    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        self._ensure_pid(pipeline_name)
        if self.pid is None:
            return None
        doc = self.transport.get_dlt_state(self.pid)
        if doc is None:
            return None
        return StateInfo(
            version=doc["version"],
            engine_version=doc["engine_version"],
            pipeline_name=doc["pipeline_name"],
            state=doc["state"],
            created_at=doc["created_at"],
            version_hash=doc.get("version_hash"),
            _dlt_load_id=doc.get("dlt_load_id"),
        )

    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        return None

    def get_stored_schema_by_hash(self, version_hash: str) -> Optional[StorageSchemaInfo]:
        return None
