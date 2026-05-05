#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="reinterpret-benchmarks"
CPUS="3.0"

# go to directory where the script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Building Maven artifacts..."
export JAVA_HOME=/Users/grzegorzkolakowski/Library/Java/JavaVirtualMachines/corretto-17.0.8.1/Contents/Home
mvn clean package -f ../pom.xml -pl flink-common,flink-optimisations

echo "==> Building Docker image..."
docker build . -t ${IMAGE_NAME}:latest

echo "==> Running Docker container..."
docker run --cpus ${CPUS} ${IMAGE_NAME}:latest

echo "==> Done."