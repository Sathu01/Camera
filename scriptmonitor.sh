#!/bin/bash

# ═══════════════════════════════════════════════════════════════════════════
# HLS STREAMING SERVICE - UBUNTU MONITORING SCRIPT
# ═══════════════════════════════════════════════════════════════════════════
# Usage:
#   ./monitor-hls.sh              - Monitor for 5 minutes
#   ./monitor-hls.sh 600          - Monitor for 10 minutes
#   ./monitor-hls.sh 0            - Monitor continuously (Ctrl+C to stop)
#
# Requirements:
#   - jq (for JSON parsing): sudo apt install jq
#   - Java application running on localhost:8080
# ═══════════════════════════════════════════════════════════════════════════

# Configuration
API_URL="${API_URL:-http://localhost:8080}"
DURATION="${1:-300}"  # Default 5 minutes (0 = infinite)
INTERVAL="${2:-5}"     # Default 5 seconds

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
GRAY='\033[0;90m'
NC='\033[0m' # No Color

# ═══════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

print_header() {
    echo -e "${CYAN}"
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║  $1"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ $1${NC}"
}

# ═══════════════════════════════════════════════════════════════════════════
# JAVA THREAD CHECKER
# ═══════════════════════════════════════════════════════════════════════════

get_java_threads() {
    # Find Java process
    JAVA_PID=$(pgrep -f "java.*backendcam" | head -n 1)
    
    if [ -z "$JAVA_PID" ]; then
        echo "0"
        return
    fi
    
    # Count threads for this PID
    THREAD_COUNT=$(ps -T -p $JAVA_PID | wc -l)
    # Subtract header line
    echo $((THREAD_COUNT - 1))
}

get_java_memory() {
    JAVA_PID=$(pgrep -f "java.*backendcam" | head -n 1)
    
    if [ -z "$JAVA_PID" ]; then
        echo "0"
        return
    fi
    
    # Get memory in MB
    MEM_KB=$(ps -p $JAVA_PID -o rss= | tr -d ' ')
    MEM_MB=$((MEM_KB / 1024))
    echo $MEM_MB
}

get_java_cpu() {
    JAVA_PID=$(pgrep -f "java.*backendcam" | head -n 1)
    
    if [ -z "$JAVA_PID" ]; then
        echo "0.0"
        return
    fi
    
    # Get CPU percentage
    CPU=$(ps -p $JAVA_PID -o %cpu= | tr -d ' ')
    echo $CPU
}

# ═══════════════════════════════════════════════════════════════════════════
# DETAILED THREAD BREAKDOWN
# ═══════════════════════════════════════════════════════════════════════════

show_thread_breakdown() {
    print_header "JAVA THREAD BREAKDOWN"
    
    JAVA_PID=$(pgrep -f "java.*backendcam" | head -n 1)
    
    if [ -z "$JAVA_PID" ]; then
        print_error "Java process not found"
        return
    fi
    
    print_info "Java PID: $JAVA_PID"
    echo ""
    
    # Get thread list with names
    echo -e "${CYAN}Thread Name                        | Count${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # HLS Workers
    WORKER_COUNT=$(ps -T -p $JAVA_PID | grep "HLS-Worker" | wc -l)
    echo -e "HLS-Worker threads                 | ${GREEN}$WORKER_COUNT${NC}"
    
    # FFmpeg threads
    FFMPEG_COUNT=$(ps -T -p $JAVA_PID | grep -i "ffmpeg\|avcodec\|swscale" | wc -l)
    echo -e "FFmpeg threads                     | ${YELLOW}$FFMPEG_COUNT${NC}"
    
    # GC threads
    GC_COUNT=$(ps -T -p $JAVA_PID | grep "GC\|Gang" | wc -l)
    echo -e "GC threads                         | $GC_COUNT"
    
    # Startup controller
    STARTUP_COUNT=$(ps -T -p $JAVA_PID | grep "Startup" | wc -l)
    echo -e "Startup controller                 | $STARTUP_COUNT"
    
    # Other JVM threads
    TOTAL=$(get_java_threads)
    OTHER=$((TOTAL - WORKER_COUNT - FFMPEG_COUNT - GC_COUNT - STARTUP_COUNT))
    echo -e "JVM/System threads                 | $OTHER"
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "TOTAL                              | ${CYAN}$TOTAL${NC}"
    echo ""
    
    # Thread assessment
    if [ $TOTAL -lt 300 ]; then
        print_success "Thread count is optimal (< 300)"
    elif [ $TOTAL -lt 500 ]; then
        print_warning "Thread count is acceptable (300-500)"
    else
        print_error "Thread count is too high (> 500)"
    fi
}

# ═══════════════════════════════════════════════════════════════════════════
# API HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════

check_api_health() {
    print_header "API HEALTH CHECK"
    
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL/api/stream/health" 2>/dev/null)
    
    if [ "$HTTP_CODE" = "200" ]; then
        print_success "API is UP (HTTP $HTTP_CODE)"
        
        # Get health data
        HEALTH=$(curl -s "$API_URL/api/stream/health" 2>/dev/null)
        
        if command -v jq &> /dev/null; then
            ACTIVE=$(echo "$HEALTH" | jq -r '.activeStreams // 0')
            QUEUE=$(echo "$HEALTH" | jq -r '.queueSize // 0')
            THREADS=$(echo "$HEALTH" | jq -r '.threadCount // 0')
            
            echo ""
            echo "Active Streams: $ACTIVE"
            echo "Queue Size:     $QUEUE"
            echo "Thread Count:   $THREADS"
        else
            print_warning "Install jq for detailed stats: sudo apt install jq"
        fi
        
        return 0
    else
        print_error "API is DOWN or unreachable (HTTP $HTTP_CODE)"
        return 1
    fi
}

# ═══════════════════════════════════════════════════════════════════════════
# SYSTEM STATS
# ═══════════════════════════════════════════════════════════════════════════

show_system_stats() {
    print_header "SYSTEM STATISTICS"
    
    # Java threads
    JAVA_THREADS=$(get_java_threads)
    JAVA_MEM=$(get_java_memory)
    JAVA_CPU=$(get_java_cpu)
    
    echo "Java Process:"
    echo "  Threads: $JAVA_THREADS"
    echo "  Memory:  ${JAVA_MEM} MB"
    echo "  CPU:     ${JAVA_CPU}%"
    echo ""
    
    # System resources
    echo "System Resources:"
    
    # CPU usage
    CPU_IDLE=$(top -bn1 | grep "Cpu(s)" | awk '{print $8}' | cut -d'%' -f1)
    CPU_USED=$(echo "100 - $CPU_IDLE" | bc)
    echo "  CPU Usage: ${CPU_USED}%"
    
    # Memory
    MEM_INFO=$(free -m | grep "Mem:")
    MEM_TOTAL=$(echo $MEM_INFO | awk '{print $2}')
    MEM_USED=$(echo $MEM_INFO | awk '{print $3}')
    MEM_PERCENT=$(echo "scale=1; $MEM_USED * 100 / $MEM_TOTAL" | bc)
    echo "  Memory:    ${MEM_USED} MB / ${MEM_TOTAL} MB (${MEM_PERCENT}%)"
    
    # Load average
    LOAD=$(uptime | awk -F'load average:' '{print $2}' | xargs)
    echo "  Load Avg:  $LOAD"
    
    echo ""
}

# ═══════════════════════════════════════════════════════════════════════════
# REAL-TIME MONITORING
# ═══════════════════════════════════════════════════════════════════════════

start_monitoring() {
    local duration=$1
    local interval=$2
    
    if [ $duration -eq 0 ]; then
        print_header "REAL-TIME MONITORING (Continuous - Press Ctrl+C to stop)"
    else
        print_header "REAL-TIME MONITORING ($duration seconds)"
    fi
    
    echo -e "${CYAN}Time     | Active | Queue | Threads | CPU%  | Memory | FPS Avg | Errors${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    START_TIME=$(date +%s)
    ITERATION=0
    
    while true; do
        CURRENT_TIME=$(date +%s)
        ELAPSED=$((CURRENT_TIME - START_TIME))
        
        # Check if we should stop
        if [ $duration -gt 0 ] && [ $ELAPSED -ge $duration ]; then
            break
        fi
        
        ITERATION=$((ITERATION + 1))
        TIMESTAMP=$(date +%H:%M:%S)
        
        # Get API data
        if HEALTH=$(curl -s "$API_URL/api/stream/health" 2>/dev/null); then
            if command -v jq &> /dev/null; then
                ACTIVE=$(echo "$HEALTH" | jq -r '.activeStreams // 0')
                QUEUE=$(echo "$HEALTH" | jq -r '.queueSize // 0')
                API_THREADS=$(echo "$HEALTH" | jq -r '.threadCount // 0')
            else
                ACTIVE="N/A"
                QUEUE="N/A"
                API_THREADS="N/A"
            fi
        else
            ACTIVE="ERR"
            QUEUE="ERR"
            API_THREADS="ERR"
        fi
        
        # Get stream stats
        if STREAMS=$(curl -s "$API_URL/api/stream/hls/list" 2>/dev/null); then
            if command -v jq &> /dev/null; then
                AVG_FPS=$(echo "$STREAMS" | jq '[.streams[].stats.fps // 0] | add / length' 2>/dev/null | xargs printf "%.1f")
                TOTAL_ERRORS=$(echo "$STREAMS" | jq '[.streams[].stats.errors // 0] | add' 2>/dev/null)
            else
                AVG_FPS="N/A"
                TOTAL_ERRORS="N/A"
            fi
        else
            AVG_FPS="N/A"
            TOTAL_ERRORS="N/A"
        fi
        
        # Get system stats
        JAVA_THREADS=$(get_java_threads)
        JAVA_MEM=$(get_java_memory)
        JAVA_CPU=$(get_java_cpu)
        
        # Format line
        printf "%s | %6s | %5s | %7s | %5s | %6s | %7s | %6s\n" \
            "$TIMESTAMP" "$ACTIVE" "$QUEUE" "$JAVA_THREADS" "$JAVA_CPU%" "${JAVA_MEM}M" "$AVG_FPS" "$TOTAL_ERRORS"
        
        # Warnings
        if [ "$JAVA_THREADS" != "N/A" ] && [ "$JAVA_THREADS" != "ERR" ]; then
            if [ $JAVA_THREADS -gt 500 ]; then
                print_warning "High thread count: $JAVA_THREADS"
            fi
        fi
        
        if [ "$TOTAL_ERRORS" != "N/A" ] && [ "$TOTAL_ERRORS" != "ERR" ]; then
            if [ $TOTAL_ERRORS -gt 50 ]; then
                print_warning "High error count: $TOTAL_ERRORS"
            fi
        fi
        
        sleep $interval
    done
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_success "Monitoring complete - $ITERATION samples collected"
}

# ═══════════════════════════════════════════════════════════════════════════
# STREAM LIST
# ═══════════════════════════════════════════════════════════════════════════

show_stream_list() {
    print_header "ACTIVE STREAMS"
    
    if ! STREAMS=$(curl -s "$API_URL/api/stream/hls/list" 2>/dev/null); then
        print_error "Failed to get stream list"
        return
    fi
    
    if ! command -v jq &> /dev/null; then
        print_warning "Install jq for detailed view: sudo apt install jq"
        echo "$STREAMS"
        return
    fi
    
    COUNT=$(echo "$STREAMS" | jq -r '.count // 0')
    
    if [ "$COUNT" -eq 0 ]; then
        print_warning "No active streams"
        return
    fi
    
    echo "Total Streams: $COUNT"
    echo ""
    echo -e "${CYAN}Name       | Status   | FPS  | Frames | Errors | Uptime | Resolution${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    echo "$STREAMS" | jq -r '.streams[] | "\(.name) | \(.status) | \(.stats.fps // 0 | tonumber | . * 10 | round / 10) | \(.stats.totalFrames // 0) | \(.stats.errors // 0) | \(.stats.uptimeSeconds // 0)s | \(.stats.resolution // "N/A")"' | \
    while IFS='|' read -r name status fps frames errors uptime resolution; do
        printf "%-10s | %-8s | %4s | %6s | %6s | %6s | %s\n" \
            "$name" "$status" "$fps" "$frames" "$errors" "$uptime" "$resolution"
    done
}

# ═══════════════════════════════════════════════════════════════════════════
# TOP THREADS (Most CPU intensive)
# ═══════════════════════════════════════════════════════════════════════════

show_top_threads() {
    print_header "TOP 20 THREADS BY CPU USAGE"
    
    JAVA_PID=$(pgrep -f "java.*backendcam" | head -n 1)
    
    if [ -z "$JAVA_PID" ]; then
        print_error "Java process not found"
        return
    fi
    
    echo -e "${CYAN}  TID   | CPU% | Thread Name${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    ps -T -p $JAVA_PID -o tid,%cpu,comm | grep -v "TID" | sort -k2 -rn | head -n 20
    
    echo ""
}

# ═══════════════════════════════════════════════════════════════════════════
# MAIN MENU
# ═══════════════════════════════════════════════════════════════════════════

show_menu() {
    echo ""
    echo -e "${CYAN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║         HLS STREAMING SERVICE - UBUNTU MONITOR                ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "1) Check API Health"
    echo "2) Show System Stats"
    echo "3) Show Thread Breakdown"
    echo "4) Show Active Streams"
    echo "5) Show Top 20 Threads (CPU)"
    echo "6) Start Real-Time Monitoring (5 min)"
    echo "7) Start Continuous Monitoring"
    echo "8) Exit"
    echo ""
    read -p "Select option [1-8]: " choice
    
    case $choice in
        1) check_api_health ;;
        2) show_system_stats ;;
        3) show_thread_breakdown ;;
        4) show_stream_list ;;
        5) show_top_threads ;;
        6) start_monitoring 300 5 ;;
        7) start_monitoring 0 5 ;;
        8) echo "Goodbye!"; exit 0 ;;
        *) print_error "Invalid option" ;;
    esac
    
    echo ""
    read -p "Press Enter to continue..."
    show_menu
}

# ═══════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

clear

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    print_warning "jq is not installed. Install it for better output:"
    echo "  sudo apt install jq"
    echo ""
fi

# If arguments provided, run monitoring directly
if [ $# -gt 0 ]; then
    check_api_health
    echo ""
    show_system_stats
    echo ""
    show_thread_breakdown
    echo ""
    start_monitoring $DURATION $INTERVAL
    exit 0
fi

# Otherwise show menu
show_menu