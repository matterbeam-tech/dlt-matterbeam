"""`dlt matterbeam` CLI commands.

A brand-new top-level `dlt` command via the same `plug_cli`
entry-point mechanism (`dlt`'s setuptools entry-point group) `dlt-matterbeam`
already uses for the destination side -- not `dlt deploy matterbeam` and not
a standalone console script.
"""

import argparse
from typing import Optional

from dlt.common.configuration.plugins import SupportsCliCommand, TCliCommandCompose

from dlt_matterbeam.deploy import (
    DeployClient,
    DeployError,
    deploy,
    poll_build_status,
    resolve_standalone_credentials,
)
from dlt_matterbeam.gate import MatterbeamGateError, run_gate

# Looked up at call time (not bound as a default parameter value) specifically so a test can
# monkeypatch these module attributes down and have `_deploy` actually observe the change --
# `deploy()`'s own keyword defaults are evaluated once at import time, which a post-import
# monkeypatch can't reach.
_DEPLOY_POLL_MAX_ATTEMPTS = 5
_DEPLOY_POLL_INTERVAL_SECONDS = 2.0


class MatterbeamCommand(SupportsCliCommand):
    command = "matterbeam"
    help_string = "Deploy dlt pipelines targeting Matterbeam to run inside Matterbeam's hosted runtime"
    description = """
Uploads a dlt pipeline to Matterbeam and runs it there under Matterbeam's own
scheduling, invocation, and state management. The pipeline must use matterbeam
as a destination or a source.

`dlt matterbeam deploy <pipeline-script-path>` validates, packages, and uploads a
pipeline. `dlt matterbeam status <pid>` re-polls build status without redeploying.
"""
    docs_url: Optional[str] = None
    parent: Optional[str] = None
    compose: TCliCommandCompose = "replace"

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        self.parser = parser
        subparsers = parser.add_subparsers(
            title="Available subcommands", dest="matterbeam_command", required=True
        )

        deploy_cmd = subparsers.add_parser(
            "deploy",
            help="Validate, package, and upload a pipeline to Matterbeam",
            description="""
Validates that the pipeline's destination is `matterbeam`, registers or adopts its pid,
submits its secrets, packages and uploads it, triggers the server-side build, then polls
build status for a short, bounded window. If the build hasn't resolved by the time that
window elapses, re-run `dlt matterbeam status <pid>` to check on it later.
""",
        )
        deploy_cmd.add_argument(
            "pipeline_script_path",
            metavar="pipeline-script-path",
            help="Path to a pipeline script whose destination is matterbeam",
        )

        status_cmd = subparsers.add_parser(
            "status",
            help="Re-poll build status for a pipeline already deployed",
            description="""
Polls build status for a pid a previous `dlt matterbeam deploy` returned. Credentials are
resolved the same way the pipeline's own destination resolves them (destination.matterbeam.
matterbeam_url/api_token via env vars or .dlt/secrets.toml) -- run this from the pipeline's own
directory, or export DESTINATION__MATTERBEAM__MATTERBEAM_URL (and _API_TOKEN) first.
""",
        )
        status_cmd.add_argument("pid", help="The pid returned by a previous `dlt matterbeam deploy`")

    def execute(self, args: argparse.Namespace) -> None:
        if args.matterbeam_command == "deploy":
            self._deploy(args)
        elif args.matterbeam_command == "status":
            self._status(args)
        else:
            self.parser.print_usage()

    def _deploy(self, args: argparse.Namespace) -> None:
        try:
            run_gate(args.pipeline_script_path)
        except MatterbeamGateError as ex:
            raise MatterbeamGateError(f"dlt matterbeam deploy: {ex}") from ex
        print(f"{args.pipeline_script_path!r} recognized as a collector (destination=matterbeam).")

        # cli-and-upload-options.md §5's polled-UX shape: "uploading... done, validating...
        # done, building... done" -- the pid/secrets/package steps (BRIEF §1 steps 3-5) are
        # synchronous from the CLI's point of view; `deploy()` itself only triggers the build
        # (step 6, async, server-side), so the polling below (Task 4 item 1) is what actually
        # closes the loop on "done."
        try:
            result = deploy(
                args.pipeline_script_path,
                poll_max_attempts=_DEPLOY_POLL_MAX_ATTEMPTS,
                poll_interval_seconds=_DEPLOY_POLL_INTERVAL_SECONDS,
            )
        except DeployError as ex:
            raise MatterbeamGateError(f"dlt matterbeam deploy: {ex}") from ex

        print(f"registered pid {result.pid!r}.")
        print(f"submitted {result.secret_count} secret(s).")
        print(f"uploaded package (content hash {result.package_content_hash}).")
        if not result.build_triggered:
            print(
                "build NOT triggered -- the package_builder component isn't deployed to this "
                "customer account yet. The upload above still succeeded; re-run `dlt matterbeam "
                f"deploy` for pid {result.pid!r} once it is."
            )
            return

        print("building environment...")
        self._print_build_status(
            result.pid,
            {
                "build_status": result.build_status,
                "build_error": result.build_error,
                "current_package_hash": result.package_content_hash,
                "installed_packages": result.installed_packages,
            },
        )

    def _status(self, args: argparse.Namespace) -> None:
        try:
            matterbeam_url, api_token = resolve_standalone_credentials()
        except DeployError as ex:
            raise MatterbeamGateError(f"dlt matterbeam status: {ex}") from ex

        client = DeployClient(matterbeam_url, api_token)
        try:
            status = poll_build_status(client, args.pid, max_attempts=1, interval_seconds=0)
        except DeployError as ex:
            raise MatterbeamGateError(f"dlt matterbeam status: {ex}") from ex
        self._print_build_status(args.pid, status)

    @staticmethod
    def _print_build_status(pid: str, status: dict) -> None:
        build_status = status.get("build_status") or "never_built"
        if build_status == "ready":
            packages = status.get("installed_packages") or {}
            print(f"build ready for pid {pid!r} (hash {status.get('current_package_hash')}).")
            print(f"{len(packages)} package(s) installed.")
        elif build_status == "failed":
            print(f"build FAILED for pid {pid!r}: {status.get('build_error')}")
        elif build_status == "building":
            print(f"still building for pid {pid!r}. Run `dlt matterbeam status {pid}` again shortly.")
        else:
            print(f"pid {pid!r} has not been built yet (build_status={build_status!r}).")
