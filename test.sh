#!/bin/bash
# Quick test script for IbexDB library

set -e

echo "🚀 IbexDB Library Test Runner"
echo "================================"

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start services
echo ""
echo "📦 Starting Docker services..."
docker-compose -f docker-compose.test.yml up -d

# Wait for services
echo ""
echo "⏳ Waiting for services to be ready (30 seconds)..."
sleep 30

# Check service health
echo ""
echo "🔍 Checking service health..."
docker-compose -f docker-compose.test.yml ps

# Run tests
echo ""
echo "🧪 Running tests..."
python3 test_with_docker.py

# Store exit code
TEST_EXIT_CODE=$?

# Show services status
echo ""
echo "📊 Services status:"
docker-compose -f docker-compose.test.yml ps

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ All tests passed!"
    echo ""
    echo "To stop services:"
    echo "  docker-compose -f docker-compose.test.yml down"
    echo ""
    echo "To clean up everything (including data):"
    echo "  docker-compose -f docker-compose.test.yml down -v"
else
    echo ""
    echo "❌ Tests failed!"
    echo ""
    echo "To view logs:"
    echo "  docker-compose -f docker-compose.test.yml logs -f"
    echo ""
    echo "To stop services:"
    echo "  docker-compose -f docker-compose.test.yml down"
fi

exit $TEST_EXIT_CODE

