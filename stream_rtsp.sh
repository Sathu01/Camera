#!/bin/bash

INPUT_DIR="/recordings"
RTSP_BASE="rtsp://127.0.0.1:8554"

idx=1

for file in "$INPUT_DIR"/*.mp4; do
  if [ -f "$file" ]; then
    cam_name="cam${idx}"
    echo "[MediaMTX] Starting stream: $cam_name <= $(basename "$file")"

    ffmpeg -re -stream_loop -1 -i "$file" \
      -vf "scale=960:540,fps=25" \
      -pix_fmt yuv420p \
      -c:v libx264 -preset veryfast -tune zerolatency -profile:v baseline -level 3.0 \
      -b:v 800k -maxrate 800k -bufsize 1600k \
      -an \
      -rtsp_transport tcp \
      -fflags +genpts -use_wallclock_as_timestamps 1 \
      -f rtsp "${RTSP_BASE}/${cam_name}" \
      >/dev/null 2>&1 &

    idx=$((idx + 1))
    sleep 1
  fi
done

echo "[MediaMTX] ✅ All MP4 files are streaming to RTSP (${RTSP_BASE}/camX)"
