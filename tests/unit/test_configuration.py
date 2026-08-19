"""`MatterbeamClientConfiguration.on_resolved`'s transport-selection logic -- both the
pre-hosted defaulting (unset -> always "http"; "file" only ever chosen explicitly) and the
unconditional override once running inside Matterbeam's own hosted runtime (`BEAMIX_PID`
set): a deployed pipeline never gets a vote on http vs internal_log, regardless of what its
own authored config says."""

from dlt_matterbeam.configuration import MatterbeamClientConfiguration


def test_defaults_to_http_when_matterbeam_url_is_set_and_not_hosted(monkeypatch):
    monkeypatch.delenv("BEAMIX_PID", raising=False)
    config = MatterbeamClientConfiguration()
    config.matterbeam_url = "http://example.test"

    config.on_resolved()

    assert config.transport == "http"


def test_defaults_to_http_even_with_no_matterbeam_url_and_not_hosted(monkeypatch):
    """`file` is never chosen implicitly, even with no `matterbeam_url` configured -- an
    unconfigured pipeline fails fast downstream (`resolve_transport`'s own
    `ConfigurationValueError` for a missing `matterbeam_url`) rather than silently writing
    local files."""
    monkeypatch.delenv("BEAMIX_PID", raising=False)
    config = MatterbeamClientConfiguration()

    config.on_resolved()

    assert config.transport == "http"


def test_explicit_transport_is_left_alone_when_not_hosted(monkeypatch):
    """An authored `transport="http"` (or anything else) is respected on a customer's own
    machine -- the override only ever fires inside the hosted runtime."""
    monkeypatch.delenv("BEAMIX_PID", raising=False)
    config = MatterbeamClientConfiguration()
    config.transport = "http"

    config.on_resolved()

    assert config.transport == "http"


def test_explicit_file_transport_is_left_alone_when_not_hosted(monkeypatch):
    """`file` remains fully supported as an explicit opt-in for local debugging/testing --
    it's only ever excluded as an *implicit* default."""
    monkeypatch.delenv("BEAMIX_PID", raising=False)
    config = MatterbeamClientConfiguration()
    config.transport = "file"

    config.on_resolved()

    assert config.transport == "file"


def test_forced_to_internal_log_when_hosted_with_no_explicit_transport(monkeypatch):
    monkeypatch.setenv("BEAMIX_PID", "some-hosted-pid")
    config = MatterbeamClientConfiguration()
    config.matterbeam_url = "http://example.test"

    config.on_resolved()

    assert config.transport == "internal_log"


def test_forced_to_internal_log_when_hosted_even_with_explicit_http_transport(monkeypatch):
    """The core fix: a pipeline authored with `transport="http"` (the public package's own
    default authoring shape) must still be forced to `internal_log` once deployed -- http is
    not a supported case inside the hosted runtime, period."""
    monkeypatch.setenv("BEAMIX_PID", "some-hosted-pid")
    config = MatterbeamClientConfiguration()
    config.transport = "http"
    config.matterbeam_url = "http://example.test"

    config.on_resolved()

    assert config.transport == "internal_log"


def test_forced_to_internal_log_when_hosted_even_with_explicit_file_transport(monkeypatch):
    monkeypatch.setenv("BEAMIX_PID", "some-hosted-pid")
    config = MatterbeamClientConfiguration()
    config.transport = "file"

    config.on_resolved()

    assert config.transport == "internal_log"
