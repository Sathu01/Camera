#!/bin/bash
echo "[MediaMTX] Stopping all FFmpeg streams..."
pkill -f "ffmpeg -re -stream_loop -1 -i"
echo "[MediaMTX] ✅ All streams stopped."
