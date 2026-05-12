#!/bin/bash
# Quick start script for Prefect project

echo "=== Prefect Project Setup ==="
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo ""
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "✓ .env file created - please update it with your actual configuration"
fi

# Initialize Prefect
echo ""
echo "Initializing Prefect..."
echo ""

# Set Prefect API URL (local dev only — production uses the cluster-internal Prefect server)
export PREFECT_API_URL=http://localhost:4200/api

# Check if Prefect server is running
if ! curl -s http://localhost:4200/api/health > /dev/null 2>&1; then
    echo "Starting Prefect server in the background..."
    nohup prefect server start > prefect-server.log 2>&1 &
    PREFECT_PID=$!
    echo "Prefect server starting (PID: $PREFECT_PID)..."
    echo "Waiting for server to be ready..."
    
    # Wait for server to be ready (max 30 seconds)
    for i in {1..30}; do
        if curl -s http://localhost:4200/api/health > /dev/null 2>&1; then
            echo "✓ Prefect server is ready!"
            break
        fi
        sleep 1
        echo -n "."
    done
    echo ""
else
    echo "✓ Prefect server is already running"
fi

# Create work pool if it doesn't exist
echo ""
echo "Setting up work pool..."
if ! prefect work-pool ls | grep -q "k8s-pool"; then
    echo "Creating k8s-pool work pool..."
    prefect work-pool create k8s-pool --type process
    echo "✓ Work pool created"
else
    echo "✓ Work pool already exists"
fi

# Deploy all flows from prefect.yaml
echo ""
echo "Deploying flows to Prefect..."
yes n | prefect deploy --all

# Start a worker in the background
echo ""
echo "Starting Prefect worker..."
nohup prefect worker start --pool k8s-pool > prefect-worker.log 2>&1 &
WORKER_PID=$!
echo "✓ Prefect worker started (PID: $WORKER_PID)"
sleep 2

echo ""
echo "=== Setup Complete! ==="
echo ""
echo "✓ Virtual environment created and activated"
echo "✓ Dependencies installed"
echo "✓ Prefect server running at http://localhost:4200"
echo "✓ Work pool created"
echo "✓ Flows deployed and ready to run"
echo "✓ Worker running and ready to execute flows"
echo ""
echo "Access the Prefect UI at: http://localhost:4200"
echo ""
echo "Your flows are now ready to run!"
echo ""
echo "Quick commands:"
echo "  - View deployments: prefect deployment ls"
echo "  - Run a flow manually: prefect deployment run 'datalake-dataload/term-raw-daily'"
echo "  - View server logs: tail -f prefect-server.log"
echo "  - View worker logs: tail -f prefect-worker.log"
echo "  - Stop server: pkill -f 'prefect server start'"
echo "  - Stop worker: pkill -f 'prefect worker start'"
echo ""
echo "Note: Edit .env file with your actual configuration before running flows"
echo ""
echo "For more information, see README.md"
