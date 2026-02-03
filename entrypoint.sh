#!/bin/bash
set -e

echo "=========================================="
echo "VolGuard 3.3 FastAPI - Container Starting"
echo "=========================================="

# Fix volume permissions (needs root)
echo "Fixing permissions..."
chown -R volguard:volguard /app/data /app/logs 2>/dev/null || true

# Show configuration
echo "Configuration:"
echo "  - Environment: ${VG_ENV:-PRODUCTION}"
echo "  - Dry Run Mode: ${VG_DRY_RUN:-TRUE}"
echo "  - Base Capital: ${VG_BASE_CAPITAL:-1000000}"
echo "  - API Port: 8000"
echo "=========================================="

# Switch to non-root user and start FastAPI
echo "Starting FastAPI server as volguard user..."
exec gosu volguard python /app/volguard.py --mode api --host 0.0.0.0 --port 8000
