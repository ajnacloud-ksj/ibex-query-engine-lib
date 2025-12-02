#!/bin/bash
# Run all feature tests in sequence

set -e

cd "$(dirname "$0")"

echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║   IbexDB Library - Feature-by-Feature Test Suite                  ║"
echo "╚════════════════════════════════════════════════════════════════════╝"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ python3 not found"
    exit 1
fi

echo ""
echo "📋 Running 6 feature test suites..."
echo ""

# Run each test
python3 01_test_models.py || exit 1
python3 02_test_config.py || exit 1
python3 03_test_connectivity.py || exit 1
python3 04_test_operations_init.py || exit 1
python3 05_test_table_ops.py || exit 1
python3 06_test_write_query.py || exit 1

echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║                    🎉 ALL TESTS PASSED! 🎉                         ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""
echo "✅ Models & Validation"
echo "✅ Configuration"
echo "✅ Infrastructure Connectivity"
echo "✅ Operations Initialization"
echo "✅ Table Operations (CREATE/LIST/DESCRIBE)"
echo "✅ Write & Query Operations"
echo ""
echo "📦 Library is ready for Lambda integration!"
echo ""

