import os

collect_ignore_glob: list[str] = []

if not (os.environ.get("MATTERBEAM_API_TOKEN") and os.environ.get("MATTERBEAM_BASE_URL")):
    # Phase 1 ships no networked transport (see README.md in this directory) -- there's
    # nothing here yet to run even with a token. This guard is what Phase 2's HttpTransport
    # tests will rely on to stay out of CI without secrets.
    collect_ignore_glob.append("test_*.py")
