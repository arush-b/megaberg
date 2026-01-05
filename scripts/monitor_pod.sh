#!/bin/bash

# Usage: ./monitor_pod.sh <pod-name> <namespace> [output-file] [interval-seconds]
# Example: ./monitor_pod.sh amoro-spark-optimizer-xxx tenant-query-history metrics.txt 1

POD_NAME="$1"
NAMESPACE="$2"
OUTPUT_FILE="${3:-pod_metrics.txt}"
INTERVAL="${4:-1}"

if [ -z "$POD_NAME" ] || [ -z "$NAMESPACE" ]; then
    echo "Usage: $0 <pod-name> <namespace> [output-file] [interval-seconds]"
    echo "Example: $0 my-pod my-namespace metrics.txt 1"
    exit 1
fi

echo "Monitoring pod: $POD_NAME in namespace: $NAMESPACE"
echo "Output file: $OUTPUT_FILE"
echo "Interval: ${INTERVAL}s"
echo "Press Ctrl+C to stop"
echo ""

# Write header
echo "timestamp,cpu_millicores,memory_mb" > "$OUTPUT_FILE"

# Trap Ctrl+C to show summary before exit
trap 'echo ""; echo "Monitoring stopped. Data saved to $OUTPUT_FILE"; exit 0' INT

while true; do
    # Get metrics using kubectl top
    METRICS=$(kubectl top pod "$POD_NAME" -n "$NAMESPACE" --no-headers 2>/dev/null)

    if [ -n "$METRICS" ]; then
        TIMESTAMP=$(date -u '+%Y-%m-%d %H:%M:%S')

        # Parse CPU (remove 'm' suffix) and Memory (remove 'Mi' suffix)
        CPU=$(echo "$METRICS" | awk '{print $2}' | sed 's/m//')
        MEM=$(echo "$METRICS" | awk '{print $3}' | sed 's/Mi//')

        # Write to file
        echo "$TIMESTAMP,$CPU,$MEM" >> "$OUTPUT_FILE"

        # Print to console
        echo "[$TIMESTAMP] CPU: ${CPU}m, MEM: ${MEM}Mi"
    else
        TIMESTAMP=$(date -u '+%Y-%m-%d %H:%M:%S')
        echo "[$TIMESTAMP] Pod not found or metrics unavailable"
    fi

    sleep "$INTERVAL"
done
