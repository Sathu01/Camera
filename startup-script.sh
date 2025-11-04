#!/bin/bash

# Backend Camera Service Startup Script
# Usage: ./start-backendcam.sh

APP_NAME="backendcam"
JAR_FILE="target/backendcam-0.0.1-SNAPSHOT.jar"
LOG_DIR="./logs"
PID_FILE="${LOG_DIR}/${APP_NAME}.pid"

# JVM Configuration
JVM_OPTS="-Xmx4G \
          -Xms4G \
          -XX:MaxMetaspaceSize=512M \
          -XX:+UseG1GC \
          -XX:MaxGCPauseMillis=200 \
          -XX:+HeapDumpOnOutOfMemoryError \
          -XX:HeapDumpPath=${LOG_DIR}/heap_dump.hprof \
          -XX:+PrintGCDetails \
          -XX:+PrintGCDateStamps \
          -Xloggc:${LOG_DIR}/gc.log"

# Application Configuration
APP_OPTS="--spring.profiles.active=prod"

# Create log directory
mkdir -p ${LOG_DIR}

# Check if already running
if [ -f ${PID_FILE} ]; then
    PID=$(cat ${PID_FILE})
    if ps -p $PID > /dev/null 2>&1; then
        echo "❌ Service is already running (PID: $PID)"
        exit 1
    else
        echo "⚠️  Removing stale PID file"
        rm ${PID_FILE}
    fi
fi

# Check if JAR exists
if [ ! -f ${JAR_FILE} ]; then
    echo "❌ JAR file not found: ${JAR_FILE}"
    echo "Run 'mvn clean package' first"
    exit 1
fi

# Start application
echo "🚀 Starting ${APP_NAME}..."
echo "📊 Memory: 4GB (Xmx=4G, Xms=4G)"
echo "🗑️  GC: G1 with 200ms max pause"
echo "📝 Logs: ${LOG_DIR}/"

nohup java ${JVM_OPTS} \
           -jar ${JAR_FILE} \
           ${APP_OPTS} \
           > ${LOG_DIR}/application.log 2>&1 &

# Save PID
echo $! > ${PID_FILE}
PID=$(cat ${PID_FILE})

# Wait for startup
sleep 3

# Verify it's running
if ps -p $PID > /dev/null 2>&1; then
    echo "✅ Service started successfully (PID: $PID)"
    echo ""
    echo "📊 Monitor with:"
    echo "   tail -f ${LOG_DIR}/application.log"
    echo "   tail -f ${LOG_DIR}/gc.log"
    echo ""
    echo "🛑 Stop with:"
    echo "   ./stop-backendcam.sh"
else
    echo "❌ Service failed to start"
    echo "Check logs: tail -n 100 ${LOG_DIR}/application.log"
    rm ${PID_FILE}
    exit 1
fi
