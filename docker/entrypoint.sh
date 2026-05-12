#!/bin/sh
set -e

# Named volumes are created owned by root:root regardless of the
# image's USER. Fix ownership on every start so the meshdb user can
# write — cheap when already correct, essential on first run on a
# fresh host.
if [ "$(id -u)" = "0" ]; then
    chown -R meshdb:meshdb /var/lib/meshdb
    exec gosu meshdb /usr/local/bin/meshdb-server "$@"
fi

# Container was launched with --user, so we're already non-root.
# Trust the operator to have arranged permissions and run directly.
exec /usr/local/bin/meshdb-server "$@"
