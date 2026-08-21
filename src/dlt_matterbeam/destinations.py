"""The `matterbeam` Destination factory -- dlt extension point 2.

`destination="matterbeam"` resolves to this class by name via the pluggy entry point in
pyproject.toml: `DestinationReference` expands the shorthand to
`<plugin top module>.destinations.<name>`. Verified end-to-end against released pypi
dlt (tests/unit/test_entrypoint.py).
"""

import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.normalizers.naming.naming import NamingConvention
from dlt_matterbeam.configuration import MatterbeamClientConfiguration

if t.TYPE_CHECKING:
    from dlt_matterbeam.client import MatterbeamJobClient


class matterbeam(Destination[MatterbeamClientConfiguration, "MatterbeamJobClient"]):
    spec = MatterbeamClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext.generic_capabilities("typed-jsonl")
        caps.supported_loader_file_formats = ["typed-jsonl"]
        caps.supported_staging_file_formats = []
        caps.preferred_staging_file_format = None
        caps.supports_ddl_transactions = False
        caps.supports_transactions = False

        # With extension point 2, these are NOT inherited from the sink -- must be
        # declared explicitly or dlt applies its relational defaults.
        caps.naming_convention = "direct"
        caps.max_table_nesting = 0

        # The ordering lever. A per-pipeline entry lock is per-recordtype serialisation
        # only if dlt never generates the concurrent job files the lock would have to
        # reject. `max_parallel_load_jobs` stays unset -- 0 is falsy and ignored.
        caps.loader_parallelism_strategy = "sequential"

        # Declaring these is what turns on the deterministic key_hash `_dlt_id`.
        caps.supported_merge_strategies = ["upsert", "insert-only", "delete-insert"]
        return caps

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: MatterbeamClientConfiguration,
        naming: t.Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        return super().adjust_capabilities(caps, config, naming)

    @property
    def client_class(self) -> t.Type["MatterbeamJobClient"]:
        from dlt_matterbeam.client import MatterbeamJobClient

        return MatterbeamJobClient
