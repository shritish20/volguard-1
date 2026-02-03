#!/bin/bash

# VolGuard 3.3 FastAPI - Simplified Deployment (No Grafana)
# This script automates the deployment process

set -e

echo "=========================================="
echo "VolGuard 3.3 FastAPI Deployment Script"
echo "=========================================="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}ERROR: Docker is not installed${NC}"
    echo "Please install Docker from https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}ERROR: Docker Compose is not installed${NC}"
    echo "Please install Docker Compose from https://docs.docker.com/compose/install/"
    exit 1
fi

echo -e "${GREEN}✓ Docker and Docker Compose found${NC}"

# Create necessary directories
echo ""
echo "Creating directory structure..."
mkdir -p volguard_data volguard_logs

# Check if volguard_3.3.py exists
if [ ! -f "volguard_3.3.py" ]; then
    echo -e "${RED}ERROR: volguard_3.3.py not found${NC}"
    echo "Please place your VolGuard application file in this directory"
    exit 1
fi

echo -e "${GREEN}✓ Application file found${NC}"

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}WARNING: .env file not found${NC}"
    echo "Please create .env file from env_template.txt"
    exit 1
fi

# Check if credentials are configured
if grep -q "your_token_here\|your_actual_token\|your_key_here" .env 2>/dev/null; then
    echo -e "${RED}ERROR: Please configure your credentials in .env file${NC}"
    echo "Required: UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, GROQ_API_KEY"
    exit 1
fi

echo -e "${GREEN}✓ Environment file configured${NC}"

# Fix line endings for entrypoint.sh
if [ -f "entrypoint.sh" ]; then
    echo ""
    echo "Fixing line endings in entrypoint.sh..."
    if command -v dos2unix &> /dev/null; then
        dos2unix entrypoint.sh 2>/dev/null || sed -i 's/\r$//' entrypoint.sh
    else
        sed -i 's/\r$//' entrypoint.sh
    fi
    chmod +x entrypoint.sh
    echo -e "${GREEN}✓ Line endings fixed${NC}"
fi

# Build Docker image
echo ""
echo "Building Docker image..."
docker-compose build

if [ $? -ne 0 ]; then
    echo -e "${RED}ERROR: Docker build failed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker image built successfully${NC}"

# Start services
echo ""
echo "Starting VolGuard service..."
docker-compose up -d

if [ $? -ne 0 ]; then
    echo -e "${RED}ERROR: Failed to start service${NC}"
    exit 1
fi

# Wait for service to start
echo ""
echo "Waiting for service to start..."
sleep 10

# Check if service is running
if docker ps | grep -q "volguard_fastapi"; then
    echo -e "${GREEN}✓ VolGuard container is running${NC}"
else
    echo -e "${RED}ERROR: VolGuard container failed to start${NC}"
    echo "Check logs with: docker-compose logs volguard"
    exit 1
fi

# Test health endpoint
echo ""
echo "Testing API endpoint..."
for i in {1..5}; do
    HEALTH_CHECK=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health 2>/dev/null || echo "000")
    if [ "$HEALTH_CHECK" = "200" ]; then
        echo -e "${GREEN}✓ API is responding (HTTP 200)${NC}"
        break
    else
        if [ $i -eq 5 ]; then
            echo -e "${YELLOW}WARNING: API not responding yet (HTTP $HEALTH_CHECK)${NC}"
            echo "Check logs with: docker-compose logs volguard"
        else
            echo "Attempt $i/5: Waiting for API to start..."
            sleep 5
        fi
    fi
done

# Show deployment info
echo ""
echo "=========================================="
echo -e "${GREEN}VolGuard Deployment Complete!${NC}"
echo "=========================================="
echo ""
echo "📊 Access Points:"
echo "  - API: http://localhost:8000"
echo "  - API Docs: http://localhost:8000/docs (FastAPI auto-generated)"
echo "  - Health Check: http://localhost:8000/health"
echo ""
echo "🔧 Useful Commands:"
echo "  - View logs: docker-compose logs -f volguard"
echo "  - Restart: docker-compose restart volguard"
echo "  - Stop: docker-compose down"
echo "  - Check status: docker ps"
echo ""
echo "🚀 API Testing:"
echo "  - Check health: curl http://localhost:8000/health"
echo "  - Market status: curl http://localhost:8000/market/status"
echo "  - Start trading: curl -X POST http://localhost:8000/trading/start"
echo "  - Portfolio status: curl http://localhost:8000/portfolio/status"
echo "  - Stop trading: curl -X POST http://localhost:8000/trading/stop"
echo "  - Emergency exit: curl -X POST http://localhost:8000/positions/exit-all"
echo ""
echo "⚠️  IMPORTANT:"
echo "  - System is in DRY RUN mode (VG_DRY_RUN=TRUE)"
echo "  - Monitor Telegram notifications carefully"
echo "  - Test thoroughly before enabling live trading"
echo "  - Change VG_DRY_RUN=FALSE in .env for live trading"
echo ""
echo "📝 Next Steps:"
echo "  1. Monitor logs: docker-compose logs -f volguard"
echo "  2. Test API: curl http://localhost:8000/health"
echo "  3. Check market: curl http://localhost:8000/market/status"
echo "  4. When ready: curl -X POST http://localhost:8000/trading/start"
echo ""
echo "=========================================="

# Offer to show live logs
echo ""
read -p "Show live logs now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo "Showing live logs (Ctrl+C to exit)..."
    echo ""
    docker-compose logs -f volguard
fi
