#!/bin/sh
set -eu
# Re-analyze at startup if requested or if no build-time result exists.
if [ "${ANALYZE_ON_START:-false}" = "true" ] || [ ! -f /srv/site/graph.html ]; then
  /usr/local/bin/analyze.sh
fi
cd /srv/site
exec python3 -m http.server 8080 --bind 0.0.0.0
