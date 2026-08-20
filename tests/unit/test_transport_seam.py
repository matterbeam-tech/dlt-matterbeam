"""D6's deployment seam: the internal-only optional package (BRIEF §3, out of scope here)
plugs in a `DirectColdlogTransport` under the `dlt_matterbeam.transports` entry-point
group. This package never imports it -- true in Phase 1 with one builtin transport, and
still true in Phase 2 with two (`FileTransport`, `HttpTransport`). These tests are what
stop that seam from rotting -- they fail loudly if `dlt_matterbeam` ever grows a direct
import of an internal package, or if the resolver stops degrading cleanly when no other
transport is registered.
"""

from importlib.metadata import entry_points

import pytest
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt_matterbeam.transport import (
    TRANSPORTS_ENTRY_POINT_GROUP,
    FileTransport,
    HttpTransport,
    resolve_transport,
)


def test_no_internal_transport_package_is_installed():
    """The environment this package's own test suite runs in has no internal-only
    package installed -- if this ever finds entries, someone added a hard dependency."""
    assert list(entry_points(group=TRANSPORTS_ENTRY_POINT_GROUP)) == []


def test_file_transport_resolves_with_no_other_package_present(tmp_path):
    class Cfg:
        output_dir = str(tmp_path)

    transport = resolve_transport("file", Cfg())
    assert isinstance(transport, FileTransport)


def test_http_transport_resolves_with_no_other_package_present():
    """Phase 2's second builtin transport -- still resolved without the entry-point
    group, same as `"file"`."""

    class Cfg:
        matterbeam_url = "http://127.0.0.1:8765"
        api_token = "unused"

    transport = resolve_transport("http", Cfg())
    assert isinstance(transport, HttpTransport)


def test_unknown_transport_name_fails_clearly_not_with_an_import_error():
    class Cfg:
        output_dir = "/tmp/unused"

    with pytest.raises(ConfigurationValueError, match="unknown transport"):
        resolve_transport("direct_coldlog", Cfg())


def test_full_pipeline_run_never_imports_a_matterbeam_internal_package(pipeline_factory):
    """The end-to-end proof: running a full load with the public package alone touches
    no module whose name would belong to the internal-only package."""
    import sys

    import dlt

    before = {name for name in sys.modules if "matterbeam_internal" in name or "matterbeam_shared" in name}
    assert before == set()

    pipeline = pipeline_factory(dataset_name="ds")

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield [{"id": 1}]

    pipeline.run(rows())

    after = {name for name in sys.modules if "matterbeam_internal" in name or "matterbeam_shared" in name}
    assert after == set()


def test_full_pipeline_run_over_http_never_imports_a_matterbeam_internal_package(http_pipeline_factory):
    """Same proof, `transport="http"` -- the seam holds for both builtin transports, not
    just the one Phase 1 shipped."""
    import sys

    import dlt

    before = {name for name in sys.modules if "matterbeam_internal" in name or "matterbeam_shared" in name}
    assert before == set()

    make_pipeline, _state = http_pipeline_factory
    pipeline = make_pipeline(dataset_name="ds")

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield [{"id": 1}]

    pipeline.run(rows())

    after = {name for name in sys.modules if "matterbeam_internal" in name or "matterbeam_shared" in name}
    assert after == set()
