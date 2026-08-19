import dataclasses
import os
from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.typing import TSecretValue

# Set only for a process beamix itself manages (Lambda or ECS) -- never present on a
# customer's own machine. The internal-log transport's own `register()` already keys off
# this exact variable for identity (R12's spike); reusing it here as the "are we running
# inside Matterbeam's own runtime" signal is the same coupling, not a new one.
_HOSTED_RUNTIME_ENV_VAR = "BEAMIX_PID"


@configspec
class MatterbeamClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: str = dataclasses.field(default="matterbeam", init=False, repr=False, compare=False)

    # filetransport only destination (for testing)
    output_dir: str = ".dlt/matterbeam_coldlog"

    # http transport required settings
    matterbeam_url: Optional[str] = None
    api_token: Optional[TSecretValue] = None

    transport: Optional[str] = None
    """Left unset, always resolves to "http" (`on_resolved`, below) -- a pipeline with no
    transport configured is assumed to mean a real Matterbeam account, not a silent local
    file drop. "file" is never chosen implicitly; set it explicitly for local
    debugging/testing without a Matterbeam account. Set explicitly to override the "http"
    default -- **except inside Matterbeam's own hosted runtime, where this is
    unconditionally forced to "internal_log" regardless of what's set here (see
    `on_resolved`)**: once a pipeline is deployed, http is never a supported choice --
    slower, and pointless when the internal transport is right there. Debugging over http
    belongs to before deploy, on the customer's own machine, not after."""

    # HTTP transport chunking target: min(4 MB compressed, 50k records).
    chunk_records: int = 50_000
    chunk_bytes: int = 4 * 1024 * 1024

    # u16 disambiguator for concurrent writers to one recordtype.
    writer_id: int = 0

    def on_resolved(self) -> None:
        if os.environ.get(_HOSTED_RUNTIME_ENV_VAR):
            # Inside the hosted runtime (dlt_runner), always -- overrides even an
            # explicitly-authored `transport="http"`/`transport="file"`, not just the
            # unset-default case below. Not a request a deployed pipeline's own config gets
            # a vote on.
            self.transport = "internal_log"
        elif self.transport is None:
            self.transport = "http"
