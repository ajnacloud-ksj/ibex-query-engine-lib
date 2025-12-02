#!/bin/bash
set -e

echo "=== Running IbexDB Library Unit Tests ==="

cd "$(dirname "$0")"

# Check if pytest is installed
if ! python -m pytest --version > /dev/null 2>&1; then
    echo "Installing test dependencies..."
    pip install pytest pytest-cov -q
fi

# Run tests
echo ""
echo "Running unit tests..."
python -m pytest tests/ -v --tb=short

echo ""
echo "✓ Unit tests completed!"

