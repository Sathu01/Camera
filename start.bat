@echo off
echo Starting HLS Service with NVENC support...

set JAVA_OPTS=-Xms4g -Xmx8g
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseG1GC
set JAVA_OPTS=%JAVA_OPTS% -XX:MaxGCPauseMillis=200
set JAVA_OPTS=%JAVA_OPTS% -XX:ParallelGCThreads=4
set JAVA_OPTS=%JAVA_OPTS% -XX:ConcGCThreads=2
set JAVA_OPTS=%JAVA_OPTS% -XX:MaxDirectMemorySize=4g
set JAVA_OPTS=%JAVA_OPTS% -Dorg.bytedeco.javacpp.maxbytes=8G
set JAVA_OPTS=%JAVA_OPTS% -Dorg.bytedeco.javacpp.maxphysicalbytes=8G

java %JAVA_OPTS% -jar target\backendcam-0.0.1-SNAPSHOT.jar