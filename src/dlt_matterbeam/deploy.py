"""Task 2 of the upload-to-running walkthrough (BRIEF §1 steps 3-5): pid handoff, secret
submission, and package upload via presigned S3. Pass-1 gate classification (step 2) is
`gate.py`'s job; this module picks up once `gate.py` has already confirmed a script is a
matterbeam collector.

Backend status, updated now that `../backend` is writable for this half of the project and
all three routes below are real, live, and exercised against a running dev server
(`http://localhost:4040`), not just modeled against the fake test server:
- `POST /collectors` with `type=hosted_dlt` -- a new sibling case next to `external_dlt` in
  `handle_create_collector` (`legacy/handlers/collectors.py`), reusing the exact same
  `DLT_PROCESS#{pipeline_key}|{dataset_name}` claim key and lookup-or-create mechanism.
  Confirmed live: a fresh claim creates `collector_type=hosted_dlt`/`execution=hosted`/
  `runtime=ecs`; a claim already resolved to an `external_dlt` collector is transitioned in
  place (`_adopt_collector_as_hosted`, calling the already-real `set_pid_runtime`) -- verified
  against the dev server that the *same* pid comes back and its FSM `runtime` flips from
  `lambda` to `ecs`.
- `PATCH /collectors/{pid}/secrets` -- a new route (`domains/deploy/repository.py`,
  `routers/collectors_ext/router.py`), not the legacy `PATCH /collectors/{id}` reclassify
  path (see `submit_secrets`'s docstring for why). Confirmed live: values land KMS-encrypted
  via the same `matterbeam_shared.secrets.encrypt` helper the legacy path uses.
- `POST /collectors/{pid}/deployment/upload-url` -- deliberately **not** the same path as the
  legacy CSV upload-url route (`POST /collectors/{collectorId}/upload-url`), since
  `collectors_ext`'s router is included before the legacy router in `app.py` and would
  otherwise shadow it for every collector. Also **not** the same bucket: both routes now
  share a dedicated `CustomerUploads` bucket (`matterbeam-<customer>-uploads`,
  `aws-infra/lib/constructs/data-core/customer-uploads.ts`) instead of the old, 14-day-
  expiring `exports` bucket -- the CSV route keeps a 14-day-expiring `file-uploads/`
  prefix, this route gets a non-expiring `deployments/dlt/{pid}/` prefix, since the
  runtime task restores from this object on every cold invocation. Confirmed live:
  returns a real presigned S3 `PutObject` URL, and a real `PUT` to it succeeds.

Tests still exercise the fake server (`tests/fake_matterbeam`), same as Phase 2's
`HttpTransport` before `external_dlt` existed for real -- the dev-server checks above were a
one-time verification, not a substitute for the unit suite.
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import logging
import os
import tarfile
import tempfile
import time
from io import BytesIO
from pathlib import Path
from typing import Callable, Dict, List, Optional

import yaml
from dlt_matterbeam.gate import open_collector_pipeline

logger = logging.getLogger("dlt_matterbeam")

_TERMINAL_BUILD_STATUSES = {"ready", "failed"}

# Mirrors WorkspaceFileSelector's DEFAULT_IGNORES (dlt/_workspace/deployment/file_selector.py)
# closely enough for a single-script project; kept local rather than imported since that
# selector is built around a full dlt *workspace* (profiles, WorkspaceRunContext) this
# project's "one script path" input model (A11) doesn't have. `*.secrets.toml` is excluded
# unconditionally below, not via this list -- R10 §2's decision, not a default-ignore pattern.
_IGNORED_DIR_NAMES = {
    "__pycache__",
    ".venv",
    "venv",
    ".git",
    "dist",
    "build",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    "htmlcov",
}
_IGNORED_FILE_SUFFIXES = (".pyc", ".pyo", ".so")

_MANIFEST_FILE_NAME = "manifest.yaml"
_FILES_PREFIX = "files"


class DeployError(Exception):
    """Raised for a Task 2 failure that isn't already a `MatterbeamGateError` -- a missing
    destination credential, a secret the customer needs to set locally and redeploy, or a
    failed register/secrets/upload-url call."""


@dataclasses.dataclass
class CollectorDeployInfo:
    """Everything Task 2 needs about the pipeline, recovered once and reused across the
    register/secrets/upload steps."""

    pipeline_name: str
    dataset_name: str
    matterbeam_url: Optional[str]
    api_token: Optional[str]


@dataclasses.dataclass
class SecretItem:
    key: str
    """`EnvironProvider`'s `SECTION__SUBSECTION__KEY` convention (R10 §1/§4) -- the shape the
    hosted runtime task will eventually set as a process env var immediately before
    `pipeline.run()` (R10 §4), so nothing needs translating between "what got submitted" and
    "what the runtime sets" once that piece is built."""
    value: str


@dataclasses.dataclass
class BuiltPackage:
    path: str
    content_hash: str


@dataclasses.dataclass
class DeployResult:
    pid: str
    package_content_hash: str
    secret_count: int
    build_triggered: bool = False
    """False whenever `trigger_build` itself raised (e.g. `501` -- the dlt-package-builder
    component isn't deployed/published to this customer account yet, `domains/deploy/
    repository.py`'s `_package_builder_lambda_arn`) -- deploy() swallows that one specific
    failure rather than failing the whole command, since upload having already succeeded
    is real, useful progress even if the build can't be kicked off yet (BRIEF §1 step 6
    is this project's own newest piece)."""
    build_status: Optional[str] = None
    """Populated by `deploy()`'s post-trigger poll (Task 4 item 1) -- `None` whenever
    `build_triggered` is False (nothing to poll). One of the `[build]` failure-tag
    taxonomy's terminal values (`ready`/`failed`) if the poll resolved in time, or
    `"building"` if the bounded poll window elapsed first -- the caller (`cli.py`) tells
    those apart to decide what to print."""
    build_error: Optional[str] = None
    installed_packages: Dict[str, str] = dataclasses.field(default_factory=dict)


def gather_deploy_info(pipeline_script_path: str) -> CollectorDeployInfo:
    """Resolves `pipeline_name`/`dataset_name` (literal constructor args, safe under the gate's
    ephemeral run) and the destination's own resolved `matterbeam_url`/`api_token` -- reusing
    the exact Matterbeam account credentials the pipeline's own matterbeam destination is
    already configured with, rather than asking the customer to set up a second one (BRIEF
    §4.1, vanilla dlt authoring; identity-and-handoff-options.md §4)."""
    from dlt.common.configuration.exceptions import ConfigurationValueError

    with open_collector_pipeline(pipeline_script_path) as pipeline:
        matterbeam_url = None
        api_token = None
        try:
            config = pipeline.destination_client().config
        except ConfigurationValueError:
            # No `matterbeam_url` configured -- transport now defaults to "http", which
            # requires one, so client construction itself fails before we ever get to
            # inspect it. Leave matterbeam_url/api_token unset rather than letting this raise
            # here: deploy()'s own matterbeam_url check below already gives a clean, specific
            # `DeployError` for exactly this case.
            pass
        else:
            matterbeam_url = getattr(config, "matterbeam_url", None)
            api_token = getattr(config, "api_token", None)
        return CollectorDeployInfo(
            pipeline_name=pipeline.pipeline_name,
            dataset_name=pipeline.dataset_name,
            matterbeam_url=matterbeam_url,
            api_token=api_token,
        )


def recover_secrets(pipeline_script_path: str) -> List[SecretItem]:
    """R10 §1/§3: recover secret *names* the same way `dlt deploy` does -- from the pipeline's
    own last **real** local run trace (`resolved_config_values`), via `dlt.attach` +
    `get_state_and_trace`, dlt's own precondition-checked mechanism (`_deploy_command_helpers`)
    -- then read their *values* locally and submit them separately (`submit_secrets`), never
    trusting the trace's own value for a secret: dlt itself stopped storing it there ("Starting
    from 1.0 version of dlt, those are not stored in the traces", `_deploy_command_helpers.py`'s
    own `_display_missing_secret_info`).

    Deliberately does not reuse `open_collector_pipeline`'s ephemeral-`DLT_DATA_DIR` pipeline
    object for this: that pipeline never actually calls `.run()` (the gate intercepts it), so it
    has no trace at all. Secret recovery needs the pipeline's *real* local state instead --
    exactly what `dlt deploy` attaches to.
    """
    import dlt
    from dlt._workspace.cli._deploy_command_helpers import get_state_and_trace, get_visitors, parse_pipeline_info
    from dlt._workspace.cli.exceptions import CliCommandInnerException, PipelineWasNotRun
    from dlt.common.configuration.exceptions import ConfigFieldMissingException
    from dlt.common.configuration.providers import EnvironProvider, StringTomlProvider
    from dlt.common.utils import set_working_dir
    from dlt.pipeline.exceptions import CannotRestorePipelineException

    script_dir = os.path.dirname(os.path.abspath(pipeline_script_path)) or "."
    with open(pipeline_script_path, "r", encoding="utf-8") as f:
        script_source = f.read()

    pipeline_name: Optional[str] = None
    pipelines_dir: Optional[str] = None
    try:
        visitor = get_visitors(script_source, pipeline_script_path)
        possible_pipelines = parse_pipeline_info(visitor)
        if possible_pipelines:
            # Task 2 needs one pipeline's secrets, not an interactive disambiguation prompt
            # (`dlt deploy`'s own behavior for >1 candidate) -- the gate already established
            # exactly one collector pipeline exists in this script (A11), so the first hit
            # recovered by AST is the one we want.
            pipeline_name, pipelines_dir = possible_pipelines[0]
    except CliCommandInnerException as ex:
        logger.warning(
            f"dlt matterbeam deploy: could not statically locate pipeline_name/pipelines_dir ({ex}); falling back to defaults"
        )

    with set_working_dir(script_dir):
        try:
            pipeline = dlt.attach(pipeline_name=pipeline_name, pipelines_dir=pipelines_dir)
            _state, trace = get_state_and_trace(pipeline)
        except (PipelineWasNotRun, CannotRestorePipelineException) as ex:
            raise DeployError(
                "dlt matterbeam deploy requires the pipeline to have run successfully at least "
                f"once locally before secrets can be recovered ({ex})."
            ) from ex

        items: List[SecretItem] = []
        seen: set = set()
        for resolved in trace.resolved_config_values:
            if not resolved.is_secret_hint:
                continue
            if resolved.sections and resolved.sections[0] == "destination":
                # The matterbeam destination's own credential (`[destination.matterbeam]
                # api_token`, sections=('destination', 'matterbeam') -- confirmed by reading
                # a real trace) is how the *external* HttpTransport authenticates to
                # Matterbeam. A hosted pipeline doesn't need it: once uploaded, it's already
                # authenticated as itself for internal calls (the runtime hands it identity
                # via BEAMIX_PID, same as every other beamix component), and the in-runtime
                # transport (`[internal-log]`) doesn't make an HTTP call at all. Submitting
                # it anyway would mean re-encrypting and storing a credential the hosted run
                # never reads -- R10 scopes this mechanism to *source* credentials
                # ("what a dlt pipeline needs source credentials to do anything," §1), not
                # the destination's own auth, so any `destination.*` section is out of scope
                # here regardless of which destination it names.
                continue
            env_key = EnvironProvider.get_key_name(resolved.key, *resolved.sections)
            if env_key in seen:
                continue
            seen.add(env_key)
            dotted_key = StringTomlProvider.get_key_name(resolved.key, *resolved.sections)
            try:
                value = dlt.secrets[dotted_key]
            except ConfigFieldMissingException:
                logger.warning(
                    f"dlt matterbeam deploy: could not recover a value for secret {env_key!r} "
                    "-- set it locally (.dlt/secrets.toml or env) and redeploy."
                )
                continue
            items.append(SecretItem(key=env_key, value=str(value)))
        return items


def _iter_package_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _IGNORED_DIR_NAMES and not d.endswith(".egg-info")]
        for name in filenames:
            if name.endswith(_IGNORED_FILE_SUFFIXES):
                continue
            if name.endswith("secrets.toml"):
                # R10 §2: secrets never enter the code package, even though dlt's own
                # `ConfigurationFileSelector` would include them by default.
                continue
            abs_path = Path(dirpath) / name
            yield abs_path, abs_path.relative_to(root)


class _HashingReader:
    """Mirrors dlt's own `PackageBuilder._HashingReader` (`dlt_package_builder.py`) -- same
    sha3_256-while-streaming shape, reimplemented locally rather than imported since that class
    is a private (`_`-prefixed) implementation detail of a builder tied to `WorkspaceRunContext`
    (see module docstring)."""

    def __init__(self, fileobj) -> None:
        self._f = fileobj
        self._h = hashlib.sha3_256()

    def read(self, size: int = -1) -> bytes:
        chunk = self._f.read(size)
        self._h.update(chunk)
        return chunk

    def digest_b64(self) -> str:
        return base64.b64encode(self._h.digest()).decode("ascii")


def _compute_content_hash(sorted_files: List[dict]) -> str:
    """Same construction as dlt's `compute_package_content_hash` (A3): a hash over the sorted
    `(relative_path, sha3_256)` pairs. `sorted_files` must already be sorted by `relative_path`."""
    h = hashlib.sha3_256()
    for item in sorted_files:
        h.update(item["relative_path"].encode("utf-8"))
        h.update(item["sha3_256"].encode("ascii"))
    return base64.b64encode(h.digest()).decode("ascii")


_DEPENDENCY_SPEC_NAMES = {"requirements.txt", "pyproject.toml"}


def _frozen_requirements_bytes() -> bytes:
    """A `pip freeze`-equivalent built from `importlib.metadata` alone -- no `pip`/
    `pipdeptree` subprocess, matching cli-and-upload-options.md §1's reasoning for why
    this design doesn't inherit `dlt`'s own `[cli]` extra (that extra exists for
    `dlt deploy`'s dependency-tree walk, which this design deliberately doesn't need).
    Used only as a fallback (see `build_package`) for the common case of a single-script
    customer pipeline with no `requirements.txt`/`pyproject.toml` of its own: A11's own
    "this pass gets importability for free" reasoning applies here too -- whatever's
    importable in the customer's local environment, where the gate and this packaging
    step both already run, is exactly what the server-side build step needs to reproduce."""
    from importlib.metadata import distributions

    seen: Dict[str, str] = {}
    for dist in distributions():
        name = dist.metadata.get("Name") if dist.metadata else None
        if not name or not dist.version:
            continue
        seen.setdefault(name.lower(), f"{name}=={dist.version}")
    return ("\n".join(sorted(seen.values())) + "\n").encode("utf-8")


def build_package(pipeline_script_path: str, output_path: str) -> BuiltPackage:
    """execution-packaging-options.md, "Packaging": produces the same artifact *shape* dlt's
    own orphaned `PackageBuilder` does (A3) -- a gzipped tar under a `files/` prefix, a
    `manifest.yaml` of per-file size + sha3_256, and a content hash over the sorted
    `(path, hash)` pairs -- built directly here rather than by calling `PackageBuilder`/
    `ConfigurationFileSelector`, which assume a full dlt *workspace* project (profiles, a
    `WorkspaceRunContext`) that a bare pipeline-script path (this project's own input model,
    per the gate, A11) doesn't have. Walks the script's own containing directory.

    `output_path` is written to directly (a plain gzipped tar) -- the caller owns cleanup, and
    it must resolve outside the script's own directory: this function tars that directory's
    contents, so an output path inside it would render the tar self-referential (worse: reading
    it mid-write, while `os.walk` is still discovering files).

    If the walked directory ships neither `requirements.txt` nor `pyproject.toml`, one is
    synthesized (`_frozen_requirements_bytes`) and added to the tar/manifest under
    `files/requirements.txt` -- the server-side build-runner (`dlt-package-builder`) needs *some*
    dependency spec to install against, and a bare single-script customer pipeline (the common
    case this design targets, per BRIEF §4.1) usually has none of its own.

    The manifest also records `entry_script`: `pipeline_script_path`'s own path relative to
    `root` (almost always just its basename, since `root` is its immediate parent directory).
    Task 4's runtime task needs this -- the uploaded tarball's `files/` directory is the
    customer's whole pipeline directory, and nothing else records which file inside it is the
    one to actually execute. `dlt-package-builder` (Task 3) carries this field through onto the
    collector record's own build metadata (`entry_script`, alongside `current_package_hash`)
    so the runtime task can read it without re-deriving it.
    """
    root = Path(pipeline_script_path).resolve().parent
    entry_script = Path(pipeline_script_path).resolve().relative_to(root).as_posix()
    manifest_files: List[dict] = []

    with tarfile.open(output_path, "w:gz") as tar:
        for abs_path, rel_path in _iter_package_files(root):
            posix_path = rel_path.as_posix()
            st = abs_path.stat()
            info = tarfile.TarInfo(name=f"{_FILES_PREFIX}/{posix_path}")
            info.size = st.st_size
            info.mtime = int(st.st_mtime)
            info.mode = st.st_mode & 0o7777
            with abs_path.open("rb") as f:
                reader = _HashingReader(f)
                tar.addfile(info, reader)
            manifest_files.append(
                {
                    "relative_path": posix_path,
                    "size_in_bytes": st.st_size,
                    "sha3_256": reader.digest_b64(),
                }
            )

        if not any(item["relative_path"] in _DEPENDENCY_SPEC_NAMES for item in manifest_files):
            frozen = _frozen_requirements_bytes()
            digest = hashlib.sha3_256(frozen).digest()
            info = tarfile.TarInfo(name=f"{_FILES_PREFIX}/requirements.txt")
            info.size = len(frozen)
            tar.addfile(info, BytesIO(frozen))
            manifest_files.append(
                {
                    "relative_path": "requirements.txt",
                    "size_in_bytes": len(frozen),
                    "sha3_256": base64.b64encode(digest).decode("ascii"),
                }
            )

        manifest_files.sort(key=lambda x: x["relative_path"])
        manifest = {"engine_version": 1, "entry_script": entry_script, "files": manifest_files}
        manifest_yaml = yaml.dump(manifest, allow_unicode=True, default_flow_style=False, sort_keys=False).encode(
            "utf-8"
        )
        manifest_info = tarfile.TarInfo(name=_MANIFEST_FILE_NAME)
        manifest_info.size = len(manifest_yaml)
        tar.addfile(manifest_info, BytesIO(manifest_yaml))

    return BuiltPackage(path=output_path, content_hash=_compute_content_hash(manifest_files))


def _raise_for_status(response) -> None:
    if response.status_code >= 400:
        raise DeployError(
            f"{response.request.method} {response.request.url} -> {response.status_code}: {response.text}"
        )


class DeployClient:
    """Deploy-time calls to Matterbeam's REST API: pid handoff, secret submission, package
    upload. Reuses the same account credentials (`matterbeam_url`/`api_token`) already
    configured on the pipeline's own matterbeam destination (`gather_deploy_info`) -- not a second,
    deploy-specific credential. Auth convention mirrors `HttpTransport` (`transport.py`):
    `Authorization: Token <key>`, never `Bearer` (that header is reserved for Cognito JWTs and
    fails silently for an API key)."""

    def __init__(self, base_url: str, api_token: Optional[str]) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_token = api_token
        self._session = None

    @property
    def session(self):
        if self._session is None:
            import requests

            self._session = requests.Session()
        return self._session

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _headers(self, extra: Optional[dict] = None) -> dict:
        headers = {"Authorization": f"Token {self.api_token}"}
        headers.update(extra or {})
        return headers

    def register_hosted_pid(self, pipeline_key: str, dataset_name: str) -> str:
        """identity-and-handoff-options.md §1: same claim key and lookup-or-create mechanism
        `HttpTransport.register`'s `type=external_dlt` already uses -- real, wired-in backend
        code, `_create_external_dlt_collector` (`legacy/handlers/collectors.py:1528-1615`,
        confirmed by first-party read): a conditional put on
        `PK=DLT_PROCESS#{pipeline_key}|{dataset_name}`. `type=hosted_dlt` is this design's
        create-time payload for a fresh claim -- a new sibling `match` arm next to
        `CollectorTypes.EXTERNAL_DLT` in `handle_create_collector`, not yet added server-side.
        The adopt-in-place transition for a claim that already resolves to an *existing*
        external collector is that same new arm's job, calling `set_pid_runtime` (confirmed
        real, `pid_fsm.py:415-422`, but currently only ever called internally from
        `beamix/app.py`'s FSM helpers -- no route exposes it today). This client only ever
        calls the one idempotent endpoint and trusts the server to do the right thing for
        whichever branch the claim resolves to -- exactly like `HttpTransport.register`."""
        response = self.session.post(
            self._url("/collectors"),
            json={"type": "hosted_dlt", "name": pipeline_key, "config": {"dataset_name": dataset_name}},
            headers=self._headers(),
        )
        _raise_for_status(response)
        return response.json()["id"]

    def submit_secrets(self, pid: str, secrets: Dict[str, str]) -> None:
        """R10 §3: must land on the encrypt-on-write path, never the verbatim-on-create one --
        confirmed real: `PATCH /collectors/{collectorId}` -> `handle_update_collector` ->
        `_reclassify_secrets` -> `_apply_update_request`'s `encrypt_secrets`/KMS call
        (`legacy/handlers/collectors.py:797-874,1120-1155`, first-party read). Not reused
        directly: `_reclassify_secrets` relocates fields by matching their *name* against a
        fixed `KNOWN_SECRET_FIELDS` vocabulary (`client_secret`, `refresh_token`, `password`,
        ... -- `collectors.py:242-260`), which a dlt pipeline's own arbitrary
        `SECTION__KEY`-shaped secret names won't generally match. Modeled instead as a sibling
        route, `PATCH /collectors/{pid}/secrets`, taking an explicit `{name: value}` map straight
        to the same KMS `encrypt()` helper (`matterbeam_shared/secrets.py`) -- same per-customer
        KMS key, same DynamoDB `secret` field, different (and simpler) reclassification step.
        Confirmed live server-side (`domains/deploy/repository.py`, `routers/collectors_ext/
        router.py`) -- values land KMS-encrypted, verified by reading the raw DynamoDB item
        back."""
        if not secrets:
            return
        response = self.session.patch(
            self._url(f"/collectors/{pid}/secrets"),
            json={"secrets": secrets},
            headers=self._headers(),
        )
        _raise_for_status(response)

    def create_upload_url(self, pid: str, filename: str) -> dict:
        """execution-packaging-options.md, "Packaging" step 4 / B7: a presigned S3 `PutObject`,
        generalizing B7's real, confirmed precedent -- `POST /collectors/{collectorId}/
        upload-url` -> `handle_collector_upload_url` (`legacy/handlers/collectors.py:362-418`,
        first-party read): `{"filename": ...}` in, `{"upload_url": ..., "key": ...}` out
        (unwrapped, no `{"data": ...}` envelope -- this legacy handler's own convention), a
        `Content-Type` pinned into the signature, 900s TTL. Not reused at that exact path,
        though -- confirmed live server-side (`routers/collectors_ext/router.py`,
        `create_deployment_upload_url`; `../backend` is writable for this half of the
        project): `collectors_ext`'s router is included *before* the legacy router in
        `app.py`, so a route at the CSV route's own path would shadow it for every
        collector, not just dlt ones. Mounted at `/collectors/{pid}/deployment/upload-url`
        instead -- same request/response shape, different (and non-colliding) path. Bucket
        is also different now: a dedicated `CustomerUploads` bucket
        (`matterbeam-<customer>-uploads`) with two prefixes -- `file-uploads/` (14-day
        expiration, what the CSV route now uses) and `deployments/` (no expiration, since
        the runtime task restores from this object on every cold invocation and it must
        persist indefinitely). This client's key lands under `deployments/dlt/{pid}/
        {filename}` -- the `dlt/` segment leaves room for a future non-dlt deployment type
        under the same prefix without a key-shape collision."""
        response = self.session.post(
            self._url(f"/collectors/{pid}/deployment/upload-url"),
            json={"filename": filename},
            headers=self._headers(),
        )
        _raise_for_status(response)
        return response.json()

    def upload_package(self, upload_url: str, package_path: str) -> None:
        """The presigned `PutObject` itself bypasses API Gateway/auth entirely (B7) -- no
        `Authorization` header, the URL's own signature is the auth. `Content-Type` must match
        whatever `create_upload_url` had the server sign the URL for."""
        with open(package_path, "rb") as f:
            response = self.session.put(upload_url, data=f, headers={"Content-Type": "application/gzip"})
        _raise_for_status(response)

    def trigger_build(self, pid: str) -> bool:
        """execution-packaging-options.md, "The build process": fires the async
        build-runner right after `upload_package`'s `PutObject` completes --
        `POST /collectors/{pid}/deployment/build` (`routers/collectors_ext/router.py`'s
        `trigger_deployment_build`). Server-side this dispatches through the ordinary
        pid_fsm RUN path (a singleton dlt-package-builder pid), not a direct Lambda invoke --
        this client has no opinion on that, it just posts and returns quickly regardless
        of how long the actual build takes. Returns `False`, not an exception, for a 501
        specifically -- the dlt-package-builder component not yet deployed/published to this
        customer account (`domains/deploy/repository.py`'s `_package_builder_lambda_arn`)
        -- so `deploy()` can still report a successful upload rather than failing the
        whole command over infrastructure that hasn't caught up yet."""
        response = self.session.post(self._url(f"/collectors/{pid}/deployment/build"), headers=self._headers())
        if response.status_code == 501:
            return False
        _raise_for_status(response)
        return True

    def get_build_status(self, pid: str) -> dict:
        """Backs `dlt matterbeam status <pid>` -- `GET /collectors/{pid}/deployment/status`
        (`routers/collectors_ext/router.py`'s `get_deployment_build_status`)."""
        response = self.session.get(self._url(f"/collectors/{pid}/deployment/status"), headers=self._headers())
        _raise_for_status(response)
        return response.json()


def poll_build_status(
    client: DeployClient,
    pid: str,
    *,
    max_attempts: int,
    interval_seconds: float,
    on_update: Optional[Callable[[dict], None]] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    """Task 4 item 1: the CLI-side half of "poll for build status" (`execution-packaging-
    options.md`'s "The build process" step 4 -- poll a status field rather than block on a
    single HTTP request, since the customer REST API sits behind a hard 30s API Gateway
    timeout, parent B16). Shared by both `dlt matterbeam deploy` (a short bounded poll right
    after triggering, for the "uploading... done, building... done" UX `cli-and-upload-
    options.md` §5 describes) and `dlt matterbeam status <pid>` (a single-shot poll, i.e.
    `max_attempts=1`, for re-checking a build already in flight).

    Stops as soon as `build_status` reaches a terminal value (`ready`/`failed` --
    `execution-packaging-options.md`'s `[build]` failure-tag taxonomy) or `max_attempts` is
    exhausted, whichever comes first -- never blocks indefinitely on a build that's stuck
    (BRIEF §1 step 6's own documented gap: a Lambda timeout mid-build leaves `build_status`
    stuck at `"building"` until a manual retry, which no amount of client-side polling can
    detect as anything other than "still building").
    """
    status: dict = {}
    for attempt in range(max_attempts):
        status = client.get_build_status(pid)
        if on_update is not None:
            on_update(status)
        if status.get("build_status") in _TERMINAL_BUILD_STATUSES:
            break
        if attempt < max_attempts - 1:
            sleep(interval_seconds)
    return status


def resolve_standalone_credentials() -> tuple[str, Optional[str]]:
    """Recovers `matterbeam_url`/`api_token` for `dlt matterbeam status <pid>`, which -- unlike
    `deploy` -- has no pipeline script path to construct a `Pipeline` from and read
    `.destination_client().config` off of (`gather_deploy_info`). BRIEF's own open item
    (Task 3's carried-forward note: "its signature only takes a bare pid, no credentials --
    a CLI redesign question left open").

    Resolved this way instead: reuses dlt's own global config/secrets accessors (`dlt.config`/
    `dlt.secrets`), the same provider chain (env vars, `.dlt/secrets.toml`/`.dlt/config.toml`
    relative to the current working directory) `recover_secrets` already trusts for the exact
    same section (`destination.matterbeam`) -- not a new mechanism, and not a change to the
    command's own `<pid>` shape (`cli-and-upload-options.md` §1 already commits to that
    literal shape). Keeps the CLI's constraint intact (BRIEF §4.1, vanilla dlt authoring): a
    customer who has already configured `destination.matterbeam.matterbeam_url`/`api_token` for
    their pipeline (env vars or `.dlt/secrets.toml`) gets `status` working for free, from the
    same directory they'd run `deploy` from -- no second credential to set up.
    """
    import dlt
    from dlt.common.configuration.exceptions import ConfigFieldMissingException

    try:
        matterbeam_url = dlt.config["destination.matterbeam.matterbeam_url"]
    except ConfigFieldMissingException as ex:
        raise DeployError(
            "could not resolve destination.matterbeam.matterbeam_url -- run `dlt matterbeam status` "
            "from the pipeline's own directory (so its .dlt/secrets.toml or .dlt/config.toml "
            "is picked up), or set the DESTINATION__MATTERBEAM__MATTERBEAM_URL env var."
        ) from ex

    api_token: Optional[str] = None
    try:
        api_token = dlt.secrets["destination.matterbeam.api_token"]
    except ConfigFieldMissingException:
        pass
    return matterbeam_url, api_token


_DEPLOY_POLL_MAX_ATTEMPTS = 5
_DEPLOY_POLL_INTERVAL_SECONDS = 2.0


def deploy(
    pipeline_script_path: str,
    *,
    poll_max_attempts: int = _DEPLOY_POLL_MAX_ATTEMPTS,
    poll_interval_seconds: float = _DEPLOY_POLL_INTERVAL_SECONDS,
) -> DeployResult:
    """Orchestrates BRIEF §1 steps 3-6 for `dlt matterbeam deploy`: pid handoff, secret
    submission, package build + presigned upload, then triggers and polls the server-side
    build (Task 4 item 1) for a short, bounded window -- `cli-and-upload-options.md` §5's
    "uploading... done, building... done" UX. Step 2 (the gate) is assumed already run by
    the caller (`cli.py`) -- this re-validates via `gather_deploy_info`/`open_collector_pipeline`
    regardless, so this function is safe to call standalone.

    `poll_max_attempts`/`poll_interval_seconds` are keyword-only so a caller (a test, or a
    future non-CLI caller) can shrink the wait without touching `cli.py`'s own module-level
    defaults; `poll_max_attempts=0` skips polling entirely (matches the pre-Task-4 behavior)."""
    info = gather_deploy_info(pipeline_script_path)
    if not info.matterbeam_url:
        raise DeployError(
            "the pipeline's matterbeam destination has no matterbeam_url configured -- set "
            "destination.matterbeam.matterbeam_url (and api_token) the same way the pipeline "
            "already resolves them for its own runs, then redeploy."
        )

    client = DeployClient(info.matterbeam_url, info.api_token)
    pid = client.register_hosted_pid(info.pipeline_name, info.dataset_name)

    secrets = recover_secrets(pipeline_script_path)
    client.submit_secrets(pid, {item.key: item.value for item in secrets})

    with tempfile.TemporaryDirectory(prefix="dlt-matterbeam-package-") as tmp_dir:
        # Outside the pipeline's own directory deliberately (`build_package`'s docstring) --
        # a sibling-of-the-script location would be walked into its own tarball.
        package_name = f"{pid}.tar.gz"
        package_path = os.path.join(tmp_dir, package_name)
        built = build_package(pipeline_script_path, package_path)
        upload = client.create_upload_url(pid, package_name)
        client.upload_package(upload["upload_url"], built.path)

    build_triggered = client.trigger_build(pid)

    build_status: Optional[str] = None
    build_error: Optional[str] = None
    installed_packages: Dict[str, str] = {}
    if build_triggered and poll_max_attempts > 0:
        status = poll_build_status(client, pid, max_attempts=poll_max_attempts, interval_seconds=poll_interval_seconds)
        build_status = status.get("build_status")
        build_error = status.get("build_error")
        installed_packages = status.get("installed_packages") or {}

    return DeployResult(
        pid=pid,
        package_content_hash=built.content_hash,
        secret_count=len(secrets),
        build_triggered=build_triggered,
        build_status=build_status,
        build_error=build_error,
        installed_packages=installed_packages,
    )
