#!/bin/bash

# --- Configuration Variables ---
# 1. Update the Windows path to a Linux path (e.g., using a mount point)
#    ASSUMPTION: The video is available at this path on your Ubuntu system.
VIDEO_INPUT="/home/se-lab/Downloads/samplesvideo.mp4" 

# --- FFmpeg Command ---
ffmpeg -stream_loop -1 -re \
  -i "$VIDEO_INPUT" \
  -f lavfi -i anullsrc=channel_layout=stereo:sample_rate=44100 \
  -map 0:v -map 1:a \
  -vf "fps=25,scale=960:540" \
  -pix_fmt yuv420p -c:v libx264 -profile:v baseline -level 3.0 -g 50 -keyint_min 50 -sc_threshold 0 \
  -b:v 800k -maxrate 800k -bufsize 1600k \
  -c:a aac -b:a 128k -ar 44100 -ac 2 \
  -rtsp_transport tcp \
  -fflags +genpts -use_wallclock_as_timestamps 1 \
  -f rtsp rtsp://localhost:8554/v1