#!/bin/bash

# Configuration Variables
STREAM_COUNT=16
API_BASE_URL="http://localhost:8080"
RTSP_URL="rtsp://mediamtx:8554/v1"
BATCH_SIZE=8
API_ENDPOINT="/api/stream/hls/start"

# Results tracking
results_started=0
results_failed=0
results_errors=()

echo "========================================"
echo "Starting $STREAM_COUNT Streams Test"
echo "========================================"

# Calculate batch count
BATCH_COUNT=$(( (STREAM_COUNT + BATCH_SIZE - 1) / BATCH_SIZE ))

# Function to start a single stream and report status
start_stream() {
    local api_url="$1"
    local rtsp_url="$2"
    local stream_name="$3"
    local full_url="${api_url}${API_ENDPOINT}"
    local json_body="{\"rtspUrl\":\"${rtsp_url}\",\"streamName\":\"${stream_name}\"}"
    local result_file="/tmp/${stream_name}_result_$$"

    # Use curl to make the POST request
    # -s: Silent mode (don't show progress)
    # -o: Output response to /dev/null
    # -w: Write out custom format after transfer is complete
    # -m 15: Timeout after 15 seconds
    HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -m 15 \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$json_body" \
        "$full_url" 2>/dev/null)
    
    # Simple check: Assuming a successful API call returns 200 or 201
    if [[ "$HTTP_STATUS" -ge 200 && "$HTTP_STATUS" -lt 300 ]]; then
        echo "SUCCESS $stream_name" > "$result_file"
    else
        # Note: In Bash, it's harder to get the exact exception message like PowerShell.
        # We report the HTTP status as the error.
        echo "FAILURE $stream_name HTTP_STATUS $HTTP_STATUS" > "$result_file"
    fi
}

# Export the function so background jobs can access it
export -f start_stream
export API_ENDPOINT

# Main batch loop
for ((batch = 0; batch < BATCH_COUNT; batch++)); do
    start=$(( batch * BATCH_SIZE + 1 ))
    end=$(( (batch + 1) * BATCH_SIZE ))
    
    # Ensure end does not exceed STREAM_COUNT
    if [ "$end" -gt "$STREAM_COUNT" ]; then
        end=$STREAM_COUNT
    fi

    echo -e "\nBatch $((batch + 1))/$BATCH_COUNT : Streams $start-$end"

    pids=() # Array to hold Process IDs (PIDs) for current batch
    temp_result_files=() # Array to hold temporary result files

    for ((i = start; i <= end; i++)); do
        stream_name="cam_$i"
        result_file="/tmp/${stream_name}_result_$$"
        temp_result_files+=("$result_file")

        # Start the function in the background
        start_stream "$API_BASE_URL" "$RTSP_URL" "$stream_name" &
        pids+=($!) # Store the PID of the background job
    done

    # Wait for all background jobs in the batch to complete
    for pid in "${pids[@]}"; do
        wait "$pid"
    done

    # Process results from temporary files
    for file in "${temp_result_files[@]}"; do
        if [ -f "$file" ]; then
            result=$(cat "$file")
            stream_name=$(echo "$result" | awk '{print $2}')
            status=$(echo "$result" | awk '{print $1}')
            
            if [ "$status" == "SUCCESS" ]; then
                results_started=$((results_started + 1))
                echo -e "  \033[32mV $stream_name\033[0m" # Green V
            else
                results_failed=$((results_failed + 1))
                error_msg=$(echo "$result" | cut -d' ' -f3-)
                results_errors+=("$stream_name: $error_msg")
                echo -e "  \033[31mX $stream_name: $error_msg\033[0m" # Red X
            fi
            
            rm "$file" # Clean up temporary file
        fi
    done
    
    # Delay between batches, unless it's the last batch
    if [ "$batch" -lt "$((BATCH_COUNT - 1))" ]; then
        echo "  Waiting 2 seconds..."
        sleep 2
    fi
done

echo -e "\n========================================"
echo "FINAL RESULTS"
echo "========================================"
echo -e "Started: \033[32m$results_started\033[0m" # Green Started count
echo -e "Failed: \033[31m$results_failed\033[0m" # Red Failed count

if [ ${#results_errors[@]} -gt 0 ]; then
    echo -e "\nErrors:"
    for error in "${results_errors[@]}"; do
        echo -e "  \033[31m$error\033[0m" # Red errors
    done
fi

echo -e "\nTest Complete!"