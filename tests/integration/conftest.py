import os

collect_ignore_glob: list[str] = []

if not (os.environ.get("MATTERBEAM_API_TOKEN") and os.environ.get("MATTERBEAM_BASE_URL")):
    # No networked transport is exercised without these set (see README.md in this
    # directory), so this guard keeps HttpTransport tests out of CI without secrets.
    collect_ignore_glob.append("test_*.py")
