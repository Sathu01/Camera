# Backendcam – Dockerized Spring Boot (Maven)

This repository contains a Spring Boot 3 (Java 17) app. A multi-stage Dockerfile is included to build the app with Maven and run it on a slim JRE.

## How it works
- Build stage: Uses `maven:3.9.9-eclipse-temurin-17` to compile the project and produce a fat jar.
- Runtime stage: Uses `eclipse-temurin:17-jre-jammy` to run the jar.
- The image exposes port `8080` (configured in `src/main/resources/application.properties`).
- The build context excludes the `Docker/` folder and other heavy assets via `.dockerignore` (your requested behavior).

## Optional: Build and run (PowerShell)
The following commands are provided as optional references if you want to build and run locally with Docker:

```powershell
# Build the image (from the project root)
docker build -t backendcam:latest .

# Run the container, mapping host port 8080 to container port 8080
docker run --rm -p 8080:8080 --name backendcam backendcam:latest

# (Optional) Pass JVM options
# docker run --rm -e JAVA_OPTS="-Xms256m -Xmx512m" -p 8080:8080 backendcam:latest
```

## Or, use Docker Compose (recommended)
Simple one-liner to build and start the app:

```powershell
# Build and start (from the project root)
docker compose up --build -d

# View logs
docker compose logs -f

# Stop and remove containers
docker compose down
```

Notes:
- The compose file mounts `./hls` to `/hls` to match `hls.server.path=/hls`.
- The `Docker/` folder is not used by this compose file and is excluded from the build context.

## Quick start: Backend + MediaMTX in one compose

This project includes a compose setup that runs both the Spring Boot backend and MediaMTX together. Use this flow on Windows PowerShell:

1) Start the stack

```powershell
docker compose up --build -d
```

2) Publish a test RTSP stream to MediaMTX (from your host)

```powershell
ffmpeg -stream_loop -1 -re -i "C:\Users\pc\Videos\Captures\VALORANT.mp4" `
	-f lavfi -i anullsrc=channel_layout=stereo:sample_rate=44100 `
	-map 0:v -map 1:a `
	-vf "fps=25,scale=960:540" `
	-pix_fmt yuv420p -c:v libx264 -profile:v baseline -level 3.0 -g 50 -keyint_min 50 -sc_threshold 0 `
	-b:v 800k -maxrate 800k -bufsize 1600k `
	-c:a aac -b:a 128k -ar 44100 -ac 2 `
	-rtsp_transport tcp `
	-fflags +genpts -use_wallclock_as_timestamps 1 `
	-f rtsp rtsp://localhost:8554/v1
```

3) Tell the backend to start HLS for that source (container-to-container URL)

```powershell
curl -s -X POST http://localhost:8080/api/stream/hls/start `
	-H "Content-Type: application/json" `
	-d '{"rtspUrl":"rtsp://mediamtx:8554/v1","streamName":"cam_15"}'
```

4) Play the HLS output

- URL: `http://localhost:8080/hls/cam_15/stream.m3u8`
- Files are written under `./hls/cam_15/` (mounted into the container).

5) Stop the HLS stream

```powershell
curl -s -X POST http://localhost:8080/api/stream/hls/stop/cam_15
```

Tips
- From the host, publish to `rtsp://localhost:8554/...` (since MediaMTX publishes the port to your host).
- From the backend container, refer to MediaMTX as `rtsp://mediamtx:8554/...` (service name on the Docker network). This is wired via `RTSP_HOST=mediamtx` in `docker-compose.yml`.

## Notes

## Troubleshooting
- If the Docker build fails on the Maven step due to network/cache issues, try rebuilding with `--no-cache`.
- If your host lacks Docker permissions, ensure Docker Desktop is running and you have access.
- If port `8080` is in use, change the host mapping (e.g., `-p 8081:8080`).