"""LoadJob classes for the `matterbeam` destination.

`batch_size=0` hands `create_load_job` a file path rather than rows, so all per-row
work -- PUA stripping, chunking -- happens here rather than in dlt's loop. Record
*encoding* (the minimal wire envelope vs. a self-contained local fact) is
transport-specific and lives in `transport.py`, not here -- this stays
transport-agnostic on purpose.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from dlt.common.destination.client import PreparedTableSchema, RunnableLoadJob
from dlt.common.storages import FileStorage
from dlt_matterbeam import envelope

if TYPE_CHECKING:
    from dlt_matterbeam.client import MatterbeamJobClient

logger = logging.getLogger("dlt_matterbeam")


class MatterbeamLoadJob(RunnableLoadJob):
    def __init__(self, file_path: str, table: PreparedTableSchema) -> None:
        super().__init__(file_path)
        self._table = table

    def run(self) -> None:
        client: "MatterbeamJobClient" = self._job_client
        table = self._table
        table_name = table["name"]

        envelope.check_disposition(table, warn=logger.warning)

        keys = envelope.key_fields(table)
        hard_delete = envelope.hard_delete_fields(table)
        sort = envelope.dedup_sort(table)

        columns = set(table["columns"].keys()) | {"_dlt_id", "_dlt_load_id"}
        with FileStorage.open_zipsafe_ro(self._file_path, "rb") as f:
            rows = list(envelope.iter_stripped_rows(f, columns))
        rows = envelope.sort_rows(rows, sort)

        recordtype_id = f"{client.config.dataset_name}.{table_name}"
        chunk_records = client.config.chunk_records
        chunk_bytes = client.config.chunk_bytes

        pending: list[dict] = []
        pending_bytes = 0
        seq = 0
        for row in rows:
            pending.append(row)
            # An estimate of the raw row's own size, not the final wire/segment size --
            # cheap, and close enough for a soft chunking target. Precise byte-size
            # chunking against the real 4 MB/6 MB limits is unmeasured and left for
            # when a real payload shape is measured.
            pending_bytes += len(json.dumps(row))
            if len(pending) >= chunk_records or pending_bytes >= chunk_bytes:
                client.send_chunk(
                    recordtype_id=recordtype_id,
                    dataset_name=client.config.dataset_name,
                    table_name=table_name,
                    rows=pending,
                    keys=keys,
                    hard_delete=hard_delete,
                    load_id=self._load_id,
                    job_id=self.job_id(),
                    seq=seq,
                )
                seq += 1
                pending, pending_bytes = [], 0
        if pending or not rows:
            # send a (possibly empty) chunk even for a zero-row job, so a table that only
            # ever sees deletes/no-ops still has a real recordtype on disk
            client.send_chunk(
                recordtype_id=recordtype_id,
                dataset_name=client.config.dataset_name,
                table_name=table_name,
                rows=pending,
                keys=keys,
                hard_delete=hard_delete,
                load_id=self._load_id,
                job_id=self.job_id(),
                seq=seq,
            )


class MatterbeamStateJob(RunnableLoadJob):
    """`_dlt_pipeline_state` never becomes a fact in the log -- diverted to
    `client.put_dlt_state` instead. Only meaningful when the transport has a server and
    a pid (`HttpTransport`); `FileTransport` degrades this to a no-op exactly like not
    implementing `WithStateSync` at all."""

    def run(self) -> None:
        client: "MatterbeamJobClient" = self._job_client
        with FileStorage.open_zipsafe_ro(self._file_path, "rb") as f:
            row = None
            for raw in f:
                if raw.strip():
                    recs = json.loads(raw)
                    row = envelope.strip_pua(recs[0] if isinstance(recs, list) else recs)
        if row is None:
            return
        client.put_dlt_state(row)
