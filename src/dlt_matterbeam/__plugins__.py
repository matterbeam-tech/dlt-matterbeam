"""pluggy plugin module: its presence in the `dlt` entry-point group is what makes
`destination="matterbeam"` resolvable by name -- `DestinationReference` only uses
the entry point to find this module and then imports `<module>.destinations.<name>`
directly, no hookimpl needed for that.

The `plug_cli` hookimpl below is what registers `dlt matterbeam ...` as a top-level
`dlt` command -- same entry-point group, no new one."""

from typing import Optional, Type

from dlt.common.configuration import plugins


@plugins.hookimpl(specname="plug_cli")
def plug_cli_matterbeam(host: str) -> Optional[Type[plugins.SupportsCliCommand]]:
    from dlt_matterbeam.cli import MatterbeamCommand

    return MatterbeamCommand
