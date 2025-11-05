package com.backendcam.backendcam;

import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.ffmpeg.global.avutil;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FFmpegLogCallback;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.io.File;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;


@Service
public class HLSStreamService {

    private static final Logger logger = Logger.getLogger(HLSStreamService.class.getName());
    
    // Configuration
    private static final String HLS_ROOT = "./hls";
    private static final int MAX_STREAMS = 100;
    private static final int WORKER_THREADS = 50;
    private static final long STARTUP_DELAY_MS = 800;
    private static final int TARGET_FPS = 10; //6 fps 
    private static final long FRAME_INTERVAL_NS = 1_000_000_000L / TARGET_FPS;
    private static final int MAX_RECONNECT_ATTEMPTS = 3;
    private static final long RECONNECT_DELAY_MS = 5000;
    
    // Thread pools - REFACTOR #3: SynchronousQueue for strict thread control
    private final ExecutorService streamExecutor;
    private final ScheduledExecutorService startupScheduler;
    
    // Startup control
    private final Semaphore startupSemaphore = new Semaphore(1, true);
    private final AtomicInteger startupQueue = new AtomicInteger(0);
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    
    // Stream tracking
    private final ConcurrentHashMap<String, String> streamLinks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Future<?>> streamTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StreamResources> streamResources = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StreamStats> streamStats = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicBoolean> streamStopFlags = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> streamRtspUrls = new ConcurrentHashMap<>();

    public HLSStreamService() {
        // REFACTOR #3: SynchronousQueue enforces fixed pool, no extra threads
        this.streamExecutor = new ThreadPoolExecutor(
            WORKER_THREADS,
            WORKER_THREADS,
            0L,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),  // Changed from LinkedBlockingQueue
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "HLS-Worker-" + counter.getAndIncrement());
                    t.setDaemon(false);
                    t.setPriority(Thread.NORM_PRIORITY);
                    return t;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
        
        this.startupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "HLS-Startup-Controller");
            t.setDaemon(false);
            return t;
        });
        
        ((ThreadPoolExecutor) streamExecutor).prestartAllCoreThreads();
        
        FFmpegLogCallback.set();
        avutil.av_log_set_level(avutil.AV_LOG_ERROR);
        
        logger.info("╔════════════════════════════════════════════╗");
        logger.info("║  HLS Service - Ultra-Optimized CPU Mode  ║");
        logger.info("║  Max Streams: " + MAX_STREAMS + "                          ║");
        logger.info("║  Worker Threads: " + WORKER_THREADS + "                      ║");
        logger.info("║  Target FPS: " + TARGET_FPS + "                            ║");
        logger.info("║  Thread Limit: ~" + (WORKER_THREADS * 3) + " expected              ║");
        logger.info("╚════════════════════════════════════════════╝");
    }

    public String startHLSStream(String rtspUrl, String streamName) {
        if (isShuttingDown.get()) {
            throw new RuntimeException("Service is shutting down");
        }
        
        if (streamLinks.containsKey(streamName)) {
            logger.info("✓ Stream already exists: " + streamName);
            return streamLinks.get(streamName);
        }

        if (streamTasks.size() >= MAX_STREAMS) {
            throw new RuntimeException("Max capacity reached: " + MAX_STREAMS);
        }

        String playlistPath = "/hls/" + streamName + "/stream.m3u8";
        streamLinks.put(streamName, playlistPath);
        streamRtspUrls.put(streamName, rtspUrl);
        streamStopFlags.put(streamName, new AtomicBoolean(false));
        
        StreamStats stats = new StreamStats(streamName);
        streamStats.put(streamName, stats);

        int queuePosition = startupQueue.incrementAndGet();
        long delay = (queuePosition - 1) * STARTUP_DELAY_MS;
        
        logger.info("→ Queued: " + streamName + " (position: " + queuePosition + ", delay: " + delay + "ms)");
        
        startupScheduler.schedule(() -> {
            try {
                startupSemaphore.acquire();
                
                if (isShuttingDown.get()) {
                    logger.info("⚠ Skipping startup (shutdown in progress): " + streamName);
                    cleanupStreamState(streamName);
                    return;
                }
                
                logger.info("▶ Starting: " + streamName);
                stats.recordStartAttempt();

                Future<?> future = streamExecutor.submit(() -> {
                    runStreamWithAutoReconnect(rtspUrl, streamName, stats);
                });

                streamTasks.put(streamName, future);
                
            } catch (InterruptedException e) {
                logger.severe("⚠ Startup interrupted: " + streamName);
                cleanupStreamState(streamName);
                Thread.currentThread().interrupt();
            } finally {
                startupSemaphore.release();
                startupQueue.decrementAndGet();
            }
        }, delay, TimeUnit.MILLISECONDS);

        return playlistPath;
    }

    // REFACTOR #6: Auto-reconnect wrapper
    private void runStreamWithAutoReconnect(String rtspUrl, String streamName, StreamStats stats) {
        int reconnectAttempt = 0;
        AtomicBoolean stopFlag = streamStopFlags.get(streamName);
        
        while (reconnectAttempt < MAX_RECONNECT_ATTEMPTS && 
               !isShuttingDown.get() && 
               streamLinks.containsKey(streamName) &&
               !stopFlag.get()) {
            
            try {
                if (reconnectAttempt > 0) {
                    logger.info("🔄 Reconnecting: " + streamName + " (attempt " + reconnectAttempt + ")");
                }
                
                runStream(rtspUrl, streamName, stats);
                
                // Stream ended normally
                if (stopFlag.get()) {
                    logger.info("✓ Stream stopped by user: " + streamName);
                    break;
                }
                
                // Unexpected stop - reconnect
                logger.warning("⚠ Stream ended unexpectedly: " + streamName);
                reconnectAttempt++;
                
                if (reconnectAttempt < MAX_RECONNECT_ATTEMPTS && !stopFlag.get()) {
                    logger.info("⏳ Reconnecting " + streamName + " in " + (RECONNECT_DELAY_MS/1000) + "s...");
                    Thread.sleep(RECONNECT_DELAY_MS);
                }
                
            } catch (Exception e) {
                stats.recordError(e);
                logger.warning("✗ Stream error: " + streamName + " - " + e.getMessage());
                reconnectAttempt++;
                
                if (reconnectAttempt < MAX_RECONNECT_ATTEMPTS && !stopFlag.get()) {
                    try {
                        Thread.sleep(RECONNECT_DELAY_MS * reconnectAttempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        if (reconnectAttempt >= MAX_RECONNECT_ATTEMPTS) {
            logger.severe("✗ Max reconnect attempts reached for: " + streamName);
        }
        
        cleanupStreamState(streamName);
    }

    private void runStream(String rtspUrl, String streamName, StreamStats stats) throws Exception {
        FFmpegFrameGrabber grabber = null;
        FFmpegFrameRecorder recorder = null;
        boolean recorderStarted = false;
        
        try {
            File outputDir = new File(HLS_ROOT, streamName);
            if (!outputDir.exists()) {
                outputDir.mkdirs();
            }
            String hlsOutput = outputDir.getAbsolutePath() + "/stream.m3u8";
            
            grabber = connectRtspWithRetry(rtspUrl, streamName);
            int width = grabber.getImageWidth();
            int height = grabber.getImageHeight();
            
            if (width <= 0 || height <= 0) {
                throw new RuntimeException("Invalid resolution: " + width + "x" + height);
            }
            
            stats.setResolution(width, height);
            logger.info("✓ RTSP connected: " + streamName + " (" + width + "x" + height + ")");
            
            recorder = createRecorder(hlsOutput, outputDir, width, height);
            
            // Small delay for stability
            Thread.sleep(200);
            
            recorder.start();
            recorderStarted = true;
            stats.setEncoder("libx264");
            logger.info("✓ Recording: " + streamName);
            
            StreamResources resources = new StreamResources(grabber, recorder);
            streamResources.put(streamName, resources);
            
            // Log active threads
            ThreadPoolExecutor executor = (ThreadPoolExecutor) streamExecutor;
            logger.info("📊 Active threads: " + executor.getActiveCount() + "/" + WORKER_THREADS + 
                       " | Total system threads: " + Thread.activeCount());
            
            streamFrames(grabber, recorder, streamName, stats);
            
        } finally {
            safeCleanup(streamName, grabber, recorder, recorderStarted);
        }
    }

    private FFmpegFrameGrabber connectRtspWithRetry(String rtspUrl, String streamName) throws Exception {
        List<String> candidates = buildRtspCandidates(rtspUrl);
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= 3; attempt++) {
            for (String url : candidates) {
                try {
                    FFmpegFrameGrabber grabber = createGrabber(url);
                    grabber.start();
                    
                    Frame testFrame = grabber.grabImage();
                    if (testFrame != null && testFrame.image != null) {
                        return grabber;
                    }
                    
                    grabber.release();
                    
                } catch (Exception e) {
                    lastException = e;
                }
            }
            
            if (attempt < 3) {
                Thread.sleep(1000 * attempt);
            }
        }
        
        throw lastException != null ? lastException : new RuntimeException("RTSP connection failed");
    }

    private FFmpegFrameGrabber createGrabber(String url) {
        FFmpegFrameGrabber g = new FFmpegFrameGrabber(url);
        g.setFormat("rtsp");
        g.setImageMode(org.bytedeco.javacv.FrameGrabber.ImageMode.COLOR);
        
        // REFACTOR #1: STRICT thread control - prevent FFmpeg internal threading
        g.setOption("threads", "1");
        g.setOption("thread_type", "slice");
        g.setOption("thread_count", "0");
        g.setVideoOption("threads", "1");
        //g.setAudioOption("threads", "1");
        
        // REFACTOR #2: Disable buffering to prevent background threads
        g.setOption("fflags", "nobuffer");
        g.setOption("flags", "low_delay");
        g.setOption("avioflags", "direct");
        
        // RTSP settings
        g.setOption("rtsp_transport", "tcp");
        g.setOption("rtsp_flags", "prefer_tcp");
        g.setOption("stimeout", "5000000");
        g.setOption("timeout", "5000000");
        
        // Performance
        g.setOption("analyzeduration", "1000000");
        g.setOption("probesize", "1000000");
        g.setOption("max_delay", "0");
        g.setOption("allowed_media_types", "video");
        
        // Error handling
        g.setOption("err_detect", "ignore_err");
        g.setOption("skip_frame", "noref");
        g.setOption("ec", "guess_mvs+deblock");
        
        return g;
    }

    private List<String> buildRtspCandidates(String rtspUrl) {
        List<String> candidates = new ArrayList<>();
        candidates.add(rtspUrl);
        
        try {
            URI u = new URI(rtspUrl);
            String base = u.getScheme() + "://" + 
                         (u.getUserInfo() != null ? u.getUserInfo() + "@" : "") + 
                         u.getHost() + 
                         (u.getPort() > 0 ? ":" + u.getPort() : "");
            
            candidates.add(base + "/Streaming/Channels/101");
            candidates.add(base + "/live");
        } catch (Exception ignored) {
        }
        
        return candidates;
    }

    private FFmpegFrameRecorder createRecorder(String hlsOutput, File outputDir, int width, int height) throws Exception {
        int targetWidth = width;
        int targetHeight = height;
        
        //production 480p
        if (height > 720) {
            double aspectRatio = (double) width / height;
            targetHeight = 720;
            targetWidth = (int) (targetHeight * aspectRatio);
            targetWidth = (targetWidth / 2) * 2;
            targetHeight = (targetHeight / 2) * 2;
        }
        
        FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(hlsOutput, targetWidth, targetHeight, 0);
        
        recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
        recorder.setFormat("hls");
        
        recorder.setFrameRate(TARGET_FPS);
        recorder.setGopSize(TARGET_FPS * 2);
        
        // HLS settings
        recorder.setOption("hls_time", "3");
        recorder.setOption("hls_list_size", "2");
        recorder.setOption("hls_flags", "delete_segments");
        recorder.setOption("hls_segment_type", "mpegts");
        recorder.setOption("hls_allow_cache", "0");
        
        String segPath = outputDir.getAbsolutePath().replace('\\', '/') + "/s%d.ts";
        recorder.setOption("hls_segment_filename", segPath);
        
        // REFACTOR #1: STRICT single-threaded encoding
        recorder.setOption("threads", "1");
        recorder.setVideoOption("threads", "1");
        recorder.setOption("thread_type", "slice");
        recorder.setVideoOption("slice_threading", "0");
        recorder.setVideoOption("frame_threading", "0");
        recorder.setVideoOption("tile_columns", "0");
        
        // Aggressive CPU-friendly settings
        recorder.setOption("preset", "ultrafast");
        recorder.setOption("tune", "zerolatency");
        recorder.setOption("crf", "38");
        recorder.setOption("maxrate", "280k");
        recorder.setOption("bufsize", "560k");
        
        // Minimize CPU work
        recorder.setOption("sc_threshold", "0");
        recorder.setOption("g", String.valueOf(TARGET_FPS * 2));
        recorder.setOption("keyint_min", String.valueOf(TARGET_FPS));
        recorder.setOption("refs", "1");
        recorder.setOption("bf", "0");
        recorder.setOption("me_method", "dia");
        recorder.setOption("subq", "0");
        recorder.setOption("trellis", "0");
        recorder.setOption("cabac", "0");
        recorder.setOption("fast-pskip", "1");
        recorder.setOption("no-dct-decimate", "1");
        recorder.setOption("flags", "+low_delay");
        recorder.setOption("fflags", "+genpts");
        recorder.setOption("fps_mode", "cfr");
        
        return recorder;
    }

    private void streamFrames(FFmpegFrameGrabber grabber, FFmpegFrameRecorder recorder, String streamName, StreamStats stats) throws Exception {
        Frame frame;
        long lastLogTime = System.currentTimeMillis();
        long lastStatsUpdate = System.currentTimeMillis();
        
        final int MAX_CONSECUTIVE_ERRORS = 20;
        int consecutiveErrors = 0;
        
        long nextFrameTimeNs = System.nanoTime();
        AtomicBoolean stopFlag = streamStopFlags.get(streamName);
        
        while (!isShuttingDown.get() && 
               !Thread.currentThread().isInterrupted() && 
               !stopFlag.get()) {
            
            try {
                frame = grabber.grabImage();
                
                if (frame == null) {
                    // REFACTOR #5: LockSupport instead of Thread.sleep
                    LockSupport.parkNanos(50_000_000);  // 50ms
                    frame = grabber.grabImage();
                    if (frame == null) {
                        logger.info("📡 Stream ended: " + streamName);
                        break;
                    }
                }
                
                long nowNs = System.nanoTime();
                
                if (nowNs < nextFrameTimeNs) {
                    stats.recordSkippedFrame();
                    long sleepNs = nextFrameTimeNs - nowNs;
                    if (sleepNs > 5_000_000) {
                        // REFACTOR #5: LockSupport.parkNanos() for better performance
                        LockSupport.parkNanos(sleepNs);
                    }
                    continue;
                }
                
                // REFACTOR #6: Better error handling around record
                if (frame.image != null) {
                    try {
                        recorder.record(frame);
                        stats.recordEncodedFrame();
                        nextFrameTimeNs = nowNs + FRAME_INTERVAL_NS;
                        consecutiveErrors = 0;
                        
                    } catch (Exception e) {
                        stats.recordError(e);
                        consecutiveErrors++;
                        
                        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                            logger.warning(streamName + ": Too many encode errors, restarting stream");
                            throw new RuntimeException("Too many encode errors (" + consecutiveErrors + ")");
                        }
                        
                        nextFrameTimeNs = nowNs + FRAME_INTERVAL_NS;
                        continue;
                    }
                }
                
                long now = System.currentTimeMillis();
                if (now - lastLogTime > 10000) {
                    logger.info(stats.getLogSummary());
                    lastLogTime = now;
                }
                
                if (now - lastStatsUpdate > 5000) {
                    stats.updateFps();
                    lastStatsUpdate = now;
                }
                
            } catch (Exception e) {
                stats.recordError(e);
                consecutiveErrors++;
                
                String msg = e.getMessage();
                if (msg != null && (msg.contains("error while decoding") || 
                                   msg.contains("corrupted") ||
                                   msg.contains("cbp too large"))) {
                    
                    if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                        throw new RuntimeException("Too many decode errors");
                    }
                    
                    // REFACTOR #5: LockSupport instead of Thread.sleep
                    LockSupport.parkNanos(10_000_000);  // 10ms
                    continue;
                }
                
                throw e;
            }
        }
    }

    public void stopHLSStream(String streamName) {
        logger.info("■ Stopping: " + streamName);
        
        // Set stop flag
        AtomicBoolean stopFlag = streamStopFlags.get(streamName);
        if (stopFlag != null) {
            stopFlag.set(true);
        }
        
        // Cancel the task
        Future<?> future = streamTasks.remove(streamName);
        if (future != null && !future.isDone()) {
            future.cancel(true);
            
            try {
                future.get(3, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                logger.warning("⚠ Timeout waiting for " + streamName + " to stop");
            } catch (Exception e) {
                // Expected for cancelled tasks
            }
        }
        
        cleanupStreamState(streamName);
    }

    private void cleanupStreamState(String streamName) {
        streamLinks.remove(streamName);
        streamRtspUrls.remove(streamName);
        streamStopFlags.remove(streamName);
        
        StreamResources resources = streamResources.remove(streamName);
        if (resources != null) {
            safeCleanup(streamName, resources.grabber, resources.recorder, true);
        }
        
        StreamStats stats = streamStats.remove(streamName);
        if (stats != null) {
            logger.info("📊 Final stats: " + stats.getFinalSummary());
        }
        
        deleteStreamFiles(streamName);
    }

    // REFACTOR #4: Removed System.gc() calls
    private void safeCleanup(String streamName, FFmpegFrameGrabber grabber, FFmpegFrameRecorder recorder, boolean recorderWasStarted) {
        streamResources.remove(streamName);
        
        if (recorder != null && recorderWasStarted) {
            try {
                recorder.stop();
            } catch (Exception e) {
                // Ignore
            }
            
            try {
                recorder.release();
            } catch (Exception e) {
                // Ignore
            }
        }
        
        if (grabber != null) {
            try {
                grabber.stop();
            } catch (Exception e) {
                // Ignore
            }
            
            try {
                grabber.release();
            } catch (Exception e) {
                // Ignore
            }
        }
        
        // REFACTOR #4: No explicit System.gc() - let JVM handle it
    }

    private void deleteStreamFiles(String streamName) {
        try {
            File dir = new File(HLS_ROOT, streamName);
            if (dir.exists()) {
                File[] files = dir.listFiles();
                if (files != null) {
                    for (File f : files) {
                        f.delete();
                    }
                }
                dir.delete();
            }
        } catch (Exception e) {
            logger.warning("⚠ Error deleting files: " + streamName);
        }
    }

    public List<String> getActiveStreams() {
        return new ArrayList<>(streamLinks.keySet());
    }

    public int getActiveStreamCount() {
        return streamLinks.size();
    }
    
    public int getQueueSize() {
        return startupQueue.get();
    }

    public boolean isStreamReady(String streamName) {
        return streamResources.containsKey(streamName);
    }

    public String getStreamStatus(String streamName) {
        if (!streamLinks.containsKey(streamName)) return "NOT_FOUND";
        if (streamResources.containsKey(streamName)) return "RUNNING";
        Future<?> future = streamTasks.get(streamName);
        if (future != null && !future.isDone()) return "STARTING";
        return "STOPPED";
    }
    
    public StreamStats getStreamStats(String streamName) {
        return streamStats.get(streamName);
    }

    @PreDestroy
    public void shutdown() {
        logger.info("╔════════════════════════════════════════════╗");
        logger.info("  Shutting down HLS Service...");
        logger.info("  Active streams: " + getActiveStreamCount());
        
        isShuttingDown.set(true);
        
        // Set all stop flags
        streamStopFlags.values().forEach(flag -> flag.set(true));
        
        startupScheduler.shutdown();
        
        try {
            if (!startupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                startupScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            startupScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        List<String> streamsToStop = new ArrayList<>(streamLinks.keySet());
        logger.info("  Stopping " + streamsToStop.size() + " streams...");
        
        for (String streamName : streamsToStop) {
            try {
                stopHLSStream(streamName);
            } catch (Exception e) {
                logger.warning("⚠ Error stopping: " + streamName);
            }
        }
        
        streamExecutor.shutdown();
        try {
            if (!streamExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warning("⚠ Forcing executor shutdown...");
                streamExecutor.shutdownNow();
                
                if (!streamExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.severe("✗ Executor did not terminate");
                }
            }
        } catch (InterruptedException e) {
            streamExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.info("  ✓ Shutdown complete");
        logger.info("╚════════════════════════════════════════════╝");
    }

    private static class StreamResources {
        final FFmpegFrameGrabber grabber;
        final FFmpegFrameRecorder recorder;
        
        StreamResources(FFmpegFrameGrabber grabber, FFmpegFrameRecorder recorder) {
            this.grabber = grabber;
            this.recorder = recorder;
        }
    }
    
    // REFACTOR #7: LongAdder for better concurrent performance
    public static class StreamStats {
        private final String streamName;
        private final long startTime;
        private final LongAdder totalFrames = new LongAdder();
        private final LongAdder skippedFrames = new LongAdder();
        private final LongAdder errors = new LongAdder();
        private final AtomicInteger startAttempts = new AtomicInteger(0);
        private volatile String encoder = "unknown";
        private volatile String resolution = "unknown";
        private volatile double currentFps = 0.0;
        private volatile long lastFrameCount = 0;
        private volatile long lastFpsUpdate = System.currentTimeMillis();
        
        public StreamStats(String streamName) {
            this.streamName = streamName;
            this.startTime = System.currentTimeMillis();
        }
        
        public void recordEncodedFrame() {
            totalFrames.increment();
        }
        
        public void recordSkippedFrame() {
            skippedFrames.increment();
        }
        
        public void recordError(Exception e) {
            errors.increment();
        }
        
        public void recordStartAttempt() {
            startAttempts.incrementAndGet();
        }
        
        public void setEncoder(String encoder) {
            this.encoder = encoder;
        }
        
        public void setResolution(int width, int height) {
            this.resolution = width + "x" + height;
        }
        
        public void updateFps() {
            long now = System.currentTimeMillis();
            long elapsed = now - lastFpsUpdate;
            if (elapsed > 0) {
                long current = totalFrames.sum();
                long framesSinceLastUpdate = current - lastFrameCount;
                currentFps = (framesSinceLastUpdate * 1000.0) / elapsed;
                lastFrameCount = current;
                lastFpsUpdate = now;
            }
        }
        
        public String getLogSummary() {
            long uptime = (System.currentTimeMillis() - startTime) / 1000;
            return String.format("%s | %s | %s | %d frames (%.1f fps) | skip: %d | err: %d | uptime: %ds",
                streamName, encoder, resolution, totalFrames.sum(), currentFps, 
                skippedFrames.sum(), errors.sum(), uptime);
        }
        
        public String getFinalSummary() {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;
            long total = totalFrames.sum();
            double avgFps = totalTime > 0 ? total / (double) totalTime : 0;
            return String.format("%s | Total: %d frames in %ds (%.1f fps avg) | Encoder: %s | Errors: %d | Attempts: %d",
                streamName, total, totalTime, avgFps, encoder, errors.sum(), startAttempts.get());
        }
        
        // Getters
        public String getStreamName() { return streamName; }
        public int getTotalFrames() { return totalFrames.intValue(); }
        public int getSkippedFrames() { return skippedFrames.intValue(); }
        public int getErrors() { return errors.intValue(); }
        public String getEncoder() { return encoder; }
        public String getResolution() { return resolution; }
        public double getCurrentFps() { return currentFps; }
        public long getUptimeSeconds() { return (System.currentTimeMillis() - startTime) / 1000; }
    }
}
