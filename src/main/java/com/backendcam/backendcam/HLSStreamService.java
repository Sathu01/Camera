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
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

/**
 * ✅ FIXED: Proper reconnection handling
 * - Waits for old task to fully stop before starting new one
 * - Properly releases FFmpeg resources
 * - No duplicate tasks per stream
 */
@Service
public class HLSStreamService {

    private static final Logger logger = Logger.getLogger(HLSStreamService.class.getName());

    private static final String HLS_ROOT = "./hls";
    private static final String LOG_ROOT = "./logs";
    private static final int MAX_STREAMS = 100;
    private static final int WORKER_THREADS = 70;
    private static final long STARTUP_DELAY_MS = 3000;
    private static final int TARGET_FPS = 8;
    private static final long RECONNECT_DELAY_MS = 5000;
    private static final long MAX_RECONNECT_DELAY_MS = 60000;
    private static final long HEALTH_CHECK_INTERVAL_MS = 120000;
    private static final long MEMORY_CHECK_INTERVAL_MS = 60000;
    private static final long CSV_LOG_INTERVAL_MS = 300000;
    private static final long STREAM_TIMEOUT_MS = 600000;

    private final ExecutorService streamExecutor;
    private final ScheduledExecutorService startupScheduler;
    private final ScheduledExecutorService healthCheckScheduler;
    private final ScheduledExecutorService memoryMonitor;
    private final ScheduledExecutorService csvLogger;

    private final Semaphore startupSemaphore = new Semaphore(1, true);
    private final AtomicInteger startupQueue = new AtomicInteger(0);
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    private final ConcurrentHashMap<String, String> streamLinks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Future<?>> streamTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StreamResources> streamResources = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StreamStats> streamStats = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicBoolean> streamStopFlags = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> streamRtspUrls = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> lastFrameTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> streamReconnectAttempts = new ConcurrentHashMap<>();

    private final OperatingSystemMXBean osBean;
    private PrintWriter csvWriter;

    private static final int MAX_HEALTH_CHECK_RECONNECTS = 10;

    public HLSStreamService() {
        this.streamExecutor = new ThreadPoolExecutor(
                WORKER_THREADS, WORKER_THREADS, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(200),
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
                new ThreadPoolExecutor.CallerRunsPolicy());

        this.startupScheduler = Executors.newSingleThreadScheduledExecutor(r -> 
            new Thread(r, "HLS-Startup"));
        this.healthCheckScheduler = Executors.newSingleThreadScheduledExecutor(r -> 
            new Thread(r, "HLS-Health"));
        this.memoryMonitor = Executors.newSingleThreadScheduledExecutor(r -> 
            new Thread(r, "HLS-Memory"));
        this.csvLogger = Executors.newSingleThreadScheduledExecutor(r -> 
            new Thread(r, "HLS-CSV"));

        ((ThreadPoolExecutor) streamExecutor).prestartAllCoreThreads();
        FFmpegLogCallback.set();
        avutil.av_log_set_level(avutil.AV_LOG_FATAL);

        this.osBean = ManagementFactory.getOperatingSystemMXBean();

        initializeCSVLogger();
        startHealthCheck();
        startMemoryMonitor();
        startCSVLogging();

        logger.info("╔════════════════════════════════════════════╗");
        logger.info("║  HLS Service - FIXED RECONNECTION         ║");
        logger.info("║  Max Streams: 100, Workers: 70            ║");
        logger.info("║  ✅ Proper task cleanup                  ║");
        logger.info("║  ✅ No duplicate reconnects              ║");
        logger.info("╚════════════════════════════════════════════╝");
    }

    private void initializeCSVLogger() {
        try {
            File logDir = new File(LOG_ROOT);
            if (!logDir.exists()) logDir.mkdirs();

            String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            File csvFile = new File(LOG_ROOT, "system_metrics_" + timestamp + ".csv");
            csvWriter = new PrintWriter(new FileWriter(csvFile, true));
            csvWriter.println("Timestamp,ActiveStreams,WorkerThreads,ActiveThreads,QueueSize," +
                    "UsedMemoryMB,MaxMemoryMB,MemoryUsagePercent,SystemCPULoad,ProcessCPULoad," +
                    "TotalReadFrames,TotalEncodedFrames,TotalErrors,DeadStreams");
            csvWriter.flush();
            logger.info("✓ CSV logger: " + csvFile.getAbsolutePath());
        } catch (Exception e) {
            logger.severe("❌ CSV init failed: " + e.getMessage());
        }
    }

    private void startHealthCheck() {
        healthCheckScheduler.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                List<String> deadStreams = new ArrayList<>();
                List<String> reconnectStreams = new ArrayList<>();

                for (Map.Entry<String, Long> entry : lastFrameTimes.entrySet()) {
                    String streamName = entry.getKey();
                    long lastFrame = entry.getValue();

                    if (now - lastFrame > STREAM_TIMEOUT_MS) {
                        AtomicInteger reconnectCount = streamReconnectAttempts.computeIfAbsent(
                            streamName, k -> new AtomicInteger(0));
                        int attempts = reconnectCount.get();

                        if (attempts < MAX_HEALTH_CHECK_RECONNECTS) {
                            logger.warning("⚠ Timeout: " + streamName + 
                                         " - Reconnect " + (attempts + 1) + "/" + MAX_HEALTH_CHECK_RECONNECTS);
                            reconnectStreams.add(streamName);
                            reconnectCount.incrementAndGet();
                        } else {
                            logger.severe("☠ Dead after " + MAX_HEALTH_CHECK_RECONNECTS + " attempts: " + streamName);
                            deadStreams.add(streamName);
                        }
                    } else {
                        streamReconnectAttempts.remove(streamName);
                    }
                }

                for (String streamName : reconnectStreams) {
                    triggerStreamReconnect(streamName);
                }

                for (String deadStream : deadStreams) {
                    streamReconnectAttempts.remove(deadStream);
                    stopHLSStream(deadStream);
                }

                if (reconnectStreams.size() > 0 || deadStreams.size() > 0) {
                    ThreadPoolExecutor pool = (ThreadPoolExecutor) streamExecutor;
                    logger.info(String.format("📊 Health: Streams=%d/%d, Workers=%d/%d, Reconnecting=%d, Killed=%d",
                        streamLinks.size(), MAX_STREAMS, pool.getActiveCount(), WORKER_THREADS,
                        reconnectStreams.size(), deadStreams.size()));
                }
            } catch (Exception e) {
                logger.severe("❌ Health check error: " + e.getMessage());
            }
        }, HEALTH_CHECK_INTERVAL_MS, HEALTH_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * ✅ FIXED: Properly stops old task before starting new one
     * Uses a lock per stream to prevent concurrent reconnections
     */
    private final ConcurrentHashMap<String, Object> streamLocks = new ConcurrentHashMap<>();
    
    private void triggerStreamReconnect(String streamName) {
        // ✅ Get or create a lock for this specific stream
        Object lock = streamLocks.computeIfAbsent(streamName, k -> new Object());
        
        synchronized (lock) {
            try {
                logger.info("🔒 Acquired reconnect lock: " + streamName);
                
                // ✅ Step 1: Signal stop to existing task
                AtomicBoolean stopFlag = streamStopFlags.get(streamName);
                if (stopFlag != null) {
                    stopFlag.set(true);
                    logger.info("🛑 Stop flag set: " + streamName);
                }

                // ✅ Step 2: Cancel and wait for old task to finish
                Future<?> oldFuture = streamTasks.get(streamName);
                if (oldFuture != null && !oldFuture.isDone()) {
                    logger.info("⏸ Stopping old task: " + streamName);
                    oldFuture.cancel(true);
                    
                    try {
                        // Wait up to 8 seconds for graceful shutdown
                        oldFuture.get(8, TimeUnit.SECONDS);
                        logger.info("✓ Old task stopped gracefully: " + streamName);
                    } catch (TimeoutException e) {
                        logger.warning("⚠ Old task timeout after 8s: " + streamName + " (forcing shutdown)");
                        // Task is stuck, force cleanup
                    } catch (CancellationException e) {
                        logger.info("✓ Old task cancelled: " + streamName);
                    } catch (Exception e) {
                        logger.warning("⚠ Old task stop error: " + streamName + " - " + e.getMessage());
                    }
                }

                // ✅ Step 3: Force clean up resources (even if task is stuck)
                StreamResources resources = streamResources.remove(streamName);
                if (resources != null) {
                    logger.info("🧹 Cleaning up FFmpeg resources: " + streamName);
                    safeCleanup(streamName, resources.grabber, resources.recorder, true);
                }

                // ✅ Step 4: Small delay to ensure thread fully exits
                Thread.sleep(500);

                // ✅ Step 5: Verify RTSP URL exists
                String rtspUrl = streamRtspUrls.get(streamName);
                if (rtspUrl == null) {
                    logger.warning("⚠ No RTSP URL for: " + streamName);
                    streamLocks.remove(streamName);
                    return;
                }

                // ✅ Step 6: Reset stop flag for new task
                streamStopFlags.put(streamName, new AtomicBoolean(false));
                lastFrameTimes.put(streamName, System.currentTimeMillis());

                // ✅ Step 7: Get or create stats
                StreamStats stats = streamStats.get(streamName);
                if (stats == null) {
                    stats = new StreamStats(streamName);
                    streamStats.put(streamName, stats);
                }

                final StreamStats finalStats = stats;
                logger.info("▶ Starting new task: " + streamName);
                
                // ✅ Step 8: Start new task
                Future<?> newFuture = streamExecutor.submit(() -> {
                    runStreamWithAutoReconnect(rtspUrl, streamName, finalStats);
                });

                streamTasks.put(streamName, newFuture);
                logger.info("✓ New task submitted: " + streamName);

            } catch (Exception e) {
                logger.severe("❌ Reconnect failed for " + streamName + ": " + e.getMessage());
                e.printStackTrace();
            } finally {
                logger.info("🔓 Released reconnect lock: " + streamName);
            }
        }
    }

    private void startMemoryMonitor() {
        memoryMonitor.scheduleAtFixedRate(() -> {
            try {
                Runtime runtime = Runtime.getRuntime();
                long usedMemory = runtime.totalMemory() - runtime.freeMemory();
                long maxMemory = runtime.maxMemory();
                double usedPercent = (usedMemory * 100.0) / maxMemory;

                if (usedPercent > 80) {
                    logger.info(String.format("💾 Memory: %dMB/%dMB (%.1f%%), Streams=%d",
                        usedMemory / (1024 * 1024), maxMemory / (1024 * 1024), usedPercent, streamLinks.size()));
                }

                if (usedPercent > 85) {
                    logger.warning("⚠ High memory: " + String.format("%.1f%%", usedPercent) + " - Running GC");
                    System.gc();
                    Thread.sleep(2000);
                }

                if (usedPercent > 95) {
                    logger.severe("🔥 CRITICAL! Stopping 5 oldest streams");
                    stopOldestStreams(5);
                }
            } catch (Exception e) {
                logger.severe("❌ Memory monitor error: " + e.getMessage());
            }
        }, MEMORY_CHECK_INTERVAL_MS, MEMORY_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void startCSVLogging() {
        csvLogger.scheduleAtFixedRate(() -> {
            try {
                logSystemMetricsToCSV();
            } catch (Exception e) {
                logger.severe("❌ CSV logging error: " + e.getMessage());
            }
        }, CSV_LOG_INTERVAL_MS, CSV_LOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void logSystemMetricsToCSV() {
        try {
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = runtime.totalMemory() - runtime.freeMemory();
            long maxMemory = runtime.maxMemory();
            double memoryUsagePercent = (usedMemory * 100.0) / maxMemory;

            ThreadPoolExecutor pool = (ThreadPoolExecutor) streamExecutor;

            double systemCpuLoad = -1;
            double processCpuLoad = -1;
            if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean sunOsBean = 
                    (com.sun.management.OperatingSystemMXBean) osBean;
                systemCpuLoad = sunOsBean.getSystemCpuLoad() * 100;
                processCpuLoad = sunOsBean.getProcessCpuLoad() * 100;
            }

            long totalReadFrames = 0, totalEncodedFrames = 0, totalErrors = 0;
            for (StreamStats stats : streamStats.values()) {
                totalReadFrames += stats.getTotalReadFrames();
                totalEncodedFrames += stats.getTotalEncodedFrames();
                totalErrors += stats.getErrors();
            }

            csvWriter.printf("%s,%d,%d,%d,%d,%d,%d,%.2f,%.2f,%.2f,%d,%d,%d,%d%n",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),
                    streamLinks.size(), WORKER_THREADS, pool.getActiveCount(), pool.getQueue().size(),
                    usedMemory / (1024 * 1024), maxMemory / (1024 * 1024), memoryUsagePercent,
                    systemCpuLoad, processCpuLoad, totalReadFrames, totalEncodedFrames, totalErrors, 0);
            csvWriter.flush();
        } catch (Exception e) {
            logger.severe("❌ CSV write error: " + e.getMessage());
        }
    }

    private void stopOldestStreams(int count) {
        List<Map.Entry<String, StreamStats>> sorted = new ArrayList<>(streamStats.entrySet());
        sorted.sort(Comparator.comparingLong(e -> e.getValue().startTime));
        
        int stopped = 0;
        for (Map.Entry<String, StreamStats> entry : sorted) {
            if (stopped >= count) break;
            logger.warning("🛑 Emergency stop: " + entry.getKey());
            stopHLSStream(entry.getKey());
            stopped++;
        }
    }

    public String startHLSStream(String rtspUrl, String streamName) {
        if (isShuttingDown.get()) throw new RuntimeException("Service shutting down");
        if (streamLinks.containsKey(streamName)) return streamLinks.get(streamName);
        if (streamTasks.size() >= MAX_STREAMS) throw new RuntimeException("Max capacity: " + MAX_STREAMS);

        String playlistPath = "/api/hls/" + streamName + "/stream.m3u8";

        streamLinks.put(streamName, playlistPath);
        streamRtspUrls.put(streamName, rtspUrl);
        streamStopFlags.put(streamName, new AtomicBoolean(false));
        lastFrameTimes.put(streamName, System.currentTimeMillis());

        StreamStats stats = new StreamStats(streamName);
        streamStats.put(streamName, stats);

        int queuePosition = startupQueue.incrementAndGet();
        long delay = (queuePosition - 1) * STARTUP_DELAY_MS;

        logger.info("→ Queued: " + streamName + " (pos: " + queuePosition + ")");

        startupScheduler.schedule(() -> {
            try {
                startupSemaphore.acquire();
                if (isShuttingDown.get()) {
                    cleanupStreamState(streamName);
                    return;
                }

                stats.recordStartAttempt();
                Future<?> future = streamExecutor.submit(() -> {
                    runStreamWithAutoReconnect(rtspUrl, streamName, stats);
                });
                streamTasks.put(streamName, future);
            } catch (Exception e) {
                logger.severe("⚠ Startup failed: " + streamName);
                cleanupStreamState(streamName);
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            } finally {
                startupSemaphore.release();
                startupQueue.decrementAndGet();
            }
        }, delay, TimeUnit.MILLISECONDS);

        return playlistPath;
    }

    /**
     * ✅ FIXED: Check thread interruption more frequently
     */
    private void runStreamWithAutoReconnect(String rtspUrl, String streamName, StreamStats stats) {
        int reconnectAttempt = 0;
        AtomicBoolean stopFlag = streamStopFlags.get(streamName);

        while (!isShuttingDown.get() && 
               !Thread.currentThread().isInterrupted() &&  // ✅ Check interruption
               streamLinks.containsKey(streamName) && 
               stopFlag != null && !stopFlag.get()) {
            try {
                if (reconnectAttempt > 0) {
                    logger.info("🔄 Internal reconnect #" + reconnectAttempt + ": " + streamName);
                }

                runStream(rtspUrl, streamName, stats);

                // ✅ Check if we should stop after stream ends
                if (stopFlag.get() || Thread.currentThread().isInterrupted()) {
                    logger.info("🛑 Stop requested after stream ended: " + streamName);
                    break;
                }

                reconnectAttempt++;
                if (!stopFlag.get() && !Thread.currentThread().isInterrupted()) {
                    long delay = Math.min(RECONNECT_DELAY_MS * reconnectAttempt, MAX_RECONNECT_DELAY_MS);
                    logger.info("⏳ Waiting " + (delay/1000) + "s before reconnect #" + (reconnectAttempt+1) + ": " + streamName);
                    Thread.sleep(delay);
                }
            } catch (InterruptedException e) {
                logger.info("⚡ Thread interrupted during reconnect: " + streamName);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                stats.recordError(e);
                reconnectAttempt++;

                if (!stopFlag.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        long delay = Math.min(RECONNECT_DELAY_MS * reconnectAttempt, MAX_RECONNECT_DELAY_MS);
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        logger.info("⚡ Thread interrupted during error recovery: " + streamName);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        logger.info("🏁 Stream task exiting: " + streamName);
        cleanupStreamState(streamName);
    }

    private void runStream(String rtspUrl, String streamName, StreamStats stats) throws Exception {
        FFmpegFrameGrabber grabber = null;
        FFmpegFrameRecorder recorder = null;
        boolean recorderStarted = false;

        try {
            File outputDir = new File(HLS_ROOT, streamName);
            if (!outputDir.exists()) outputDir.mkdirs();
            
            String hlsOutput = outputDir.getAbsolutePath() + "/stream.m3u8";

            grabber = connectRtspWithRetry(rtspUrl, streamName);
            
            double sourceFps = grabber.getFrameRate();
            if (sourceFps <= 0 || sourceFps > 60) sourceFps = 25;
            
            int width = grabber.getImageWidth();
            int height = grabber.getImageHeight();

            if (width <= 0 || height <= 0) {
                throw new RuntimeException("Invalid resolution: " + width + "x" + height);
            }

            stats.setResolution(width, height);
            stats.setSourceCodec(grabber.getVideoCodecName());
            stats.setSourceFps(sourceFps);

            recorder = createRecorder(hlsOutput, outputDir, width, height);
            Thread.sleep(200);
            recorder.start();
            recorderStarted = true;
            stats.setEncoder("libx264");

            StreamResources resources = new StreamResources(grabber, recorder);
            streamResources.put(streamName, resources);

            int frameSkipRatio = Math.max(1, (int) Math.round(sourceFps / TARGET_FPS));
            logger.info("🎯 " + streamName + ": " + sourceFps + "fps → " + TARGET_FPS + "fps (skip 1/" + frameSkipRatio + ")");
            
            streamFrames(grabber, recorder, streamName, stats, frameSkipRatio, sourceFps);

        } finally {
            safeCleanup(streamName, grabber, recorder, recorderStarted);
        }
    }

    /**
     * ✅ FIXED: Removed artificial frame pacing - let FFmpeg handle timing
     */
    private void streamFrames(FFmpegFrameGrabber grabber, FFmpegFrameRecorder recorder, 
                              String streamName, StreamStats stats, int frameSkipRatio, 
                              double sourceFps) throws Exception {
        long lastLogTime = System.currentTimeMillis();
        long lastStatsUpdate = System.currentTimeMillis();
        long lastSuccessfulEncode = System.currentTimeMillis();

        final int MAX_NULL_FRAMES = 500;
        final int MAX_ENCODE_FAILURES = 20;
        final long MAX_TIME_WITHOUT_ENCODE = 180000;  // 3 minutes
        
        int consecutiveNullFrames = 0;
        int consecutiveEncodeFailures = 0;
        int frameCounter = 0;
        int totalIgnoredErrors = 0;

        AtomicBoolean stopFlag = streamStopFlags.get(streamName);

        logger.info(streamName + ": Frame loop started (read: " + sourceFps + "fps, encode: " + TARGET_FPS + "fps)");

        while (!isShuttingDown.get() && !Thread.currentThread().isInterrupted() && 
               stopFlag != null && !stopFlag.get()) {
            Frame frame = null;
            
            try {
                long now = System.currentTimeMillis();
                
                // Check for encoding timeout
                if (now - lastSuccessfulEncode > MAX_TIME_WITHOUT_ENCODE) {
                    logger.warning(streamName + ": ❌ No encoding for 3min - reconnecting");
                    throw new RuntimeException("Encoding timeout");
                }

                // ✅ NO ARTIFICIAL PACING - just grab frames as fast as FFmpeg provides them
                frame = grabber.grabImage();

                if (frame == null) {
                    consecutiveNullFrames++;

                    // ✅ Progressive backoff - longer waits for persistent null frames
                    long sleepTime;
                    if (consecutiveNullFrames < 5) {
                        sleepTime = 100;  // 100ms - camera buffering
                    } else if (consecutiveNullFrames < 50) {
                        sleepTime = 200;  // 200ms - network issue
                    } else if (consecutiveNullFrames < 200) {
                        sleepTime = 500;  // 500ms - serious problem
                    } else {
                        sleepTime = 1000; // 1s - critical
                    }

                    if (consecutiveNullFrames == 10) {
                        logger.warning(streamName + ": ⚠ 10 null frames (camera buffering?)");
                    } else if (consecutiveNullFrames == 100) {
                        logger.warning(streamName + ": ⚠ 100 null frames (network issue?)");
                    } else if (consecutiveNullFrames == 300) {
                        logger.warning(streamName + ": ⚠ 300 null frames (will reconnect at 500)");
                    }

                    if (consecutiveNullFrames >= MAX_NULL_FRAMES) {
                        logger.warning(streamName + ": ❌ " + consecutiveNullFrames + " null frames - reconnecting");
                        throw new RuntimeException("Stream stalled");
                    }
                    
                    Thread.sleep(sleepTime);
                    continue;
                }

                // Got a valid frame
                consecutiveNullFrames = 0;
                stats.recordReadFrame();
                lastFrameTimes.put(streamName, now);
                frameCounter++;

                // Decide if we encode this frame (frame skipping for FPS reduction)
                boolean shouldEncode = (frameCounter % frameSkipRatio == 0);

                // Validate frame
                if (frame.image == null || frame.imageWidth <= 0 || frame.imageHeight <= 0) {
                    stats.recordSkippedFrame();
                    frame.close();
                    frame = null;
                    Thread.sleep(5);
                    continue;
                }

                if (shouldEncode) {
                    try {
                        recorder.record(frame);
                        stats.recordEncodedFrame();
                        consecutiveEncodeFailures = 0;
                        lastSuccessfulEncode = now;
                    } catch (Exception e) {
                        stats.recordError(e);
                        consecutiveEncodeFailures++;
                        
                        if (consecutiveEncodeFailures == 5) {
                            logger.warning(streamName + ": ⚠ 5 encode failures");
                        }
                        
                        if (consecutiveEncodeFailures >= MAX_ENCODE_FAILURES) {
                            logger.warning(streamName + ": ❌ " + consecutiveEncodeFailures + " encode failures");
                            throw new RuntimeException("Encoder failure");
                        }
                    }
                } else {
                    stats.recordSkippedFrame();
                }

                // ✅ ALWAYS close frames immediately
                frame.close();
                frame = null;

                // Logging
                now = System.currentTimeMillis();
                if (now - lastLogTime > 30000) {
                    logger.info(stats.getLogSummary());
                    lastLogTime = now;
                }

                if (now - lastStatsUpdate > 10000) {
                    stats.updateFps();
                    lastStatsUpdate = now;
                }

            } catch (Exception e) {
                if (frame != null) {
                    try { frame.close(); } catch (Exception ignored) {}
                    frame = null;
                }

                stats.recordError(e);
                String msg = e.getMessage();

                // Fatal errors - reconnect
                if (msg != null && (
                        msg.contains("Encoding timeout") ||
                        msg.contains("Stream stalled") ||
                        msg.contains("Encoder failure") ||
                        msg.contains("Connection") ||
                        msg.contains("refused"))) {
                    throw e;
                }

                // Ignore harmless codec errors
                if (msg != null && (
                        msg.contains("no frame!") ||
                        msg.contains("missing picture") ||
                        msg.contains("Could not find ref") ||
                        msg.contains("error while decoding MB") ||
                        msg.contains("corrupted frame") ||
                        msg.contains("bytestream") ||
                        msg.contains("concealing"))) {

                    totalIgnoredErrors++;
                    if (totalIgnoredErrors % 1000 == 0) {
                        logger.info(streamName + ": 📊 Ignored " + totalIgnoredErrors + " codec errors (normal)");
                    }
                    Thread.sleep(10);
                    continue;
                }

                logger.warning(streamName + ": ⚠ Error: " + msg);
                Thread.sleep(10);
            }
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
                        testFrame.close();
                        logger.info("✓ Connected: " + streamName);
                        return grabber;
                    }

                    grabber.release();
                } catch (Exception e) {
                    lastException = e;
                }
            }

            if (attempt < 3) Thread.sleep(1000 * attempt);
        }

        throw lastException != null ? lastException : new RuntimeException("RTSP connection failed");
    }

    private FFmpegFrameGrabber createGrabber(String url) {
        FFmpegFrameGrabber g = new FFmpegFrameGrabber(url);
        g.setFormat("rtsp");
        g.setImageMode(org.bytedeco.javacv.FrameGrabber.ImageMode.COLOR);
        
        g.setOption("threads", "1");
        g.setOption("thread_count", "0");
        g.setVideoOption("threads", "1");
        
        g.setOption("analyzeduration", "5000000");
        g.setOption("probesize", "5000000");
        g.setOption("max_delay", "1000000");
        
        g.setOption("reorder_queue_size", "8192");
        g.setOption("max_interleave_delta", "2000000");
        
        g.setOption("err_detect", "ignore_err");
        g.setOption("ec", "favor_inter+guess_mvs+deblock");
        
        g.setOption("skip_frame", "noref");
        g.setOption("skip_loop_filter", "noref");
        g.setOption("skip_idct", "noref");
        
        g.setOption("fflags", "+discardcorrupt+nobuffer+genpts+igndts+ignidx");
        g.setOption("flags", "low_delay");
        g.setOption("flags2", "+ignorecrop+showall");
        
        g.setOption("rtsp_transport", "tcp");
        g.setOption("rtsp_flags", "prefer_tcp");
        g.setOption("stimeout", "60000000");
        g.setOption("timeout", "60000000");

        g.setOption("buffer_size", "8192000");
        g.setOption("allowed_media_types", "video");
        
        g.setOption("max_error_rate", "1.0");
        g.setOption("rw_timeout", "60000000");
        g.setOption("use_wallclock_as_timestamps", "1");
        
        g.setOption("strict", "-2");
        g.setOption("err_detect", "compliant");

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
        } catch (Exception ignored) {}

        return candidates;
    }

    private FFmpegFrameRecorder createRecorder(String hlsOutput, File outputDir, int width, int height)
            throws Exception {
        int targetWidth = width;
        int targetHeight = height;

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

        recorder.setOption("hls_time", "4");
        recorder.setOption("hls_list_size", "3");
        recorder.setOption("hls_flags", "delete_segments");
        recorder.setOption("hls_segment_type", "mpegts");
        recorder.setOption("hls_allow_cache", "0");

        String segPath = outputDir.getAbsolutePath().replace('\\', '/') + "/s%d.ts";
        recorder.setOption("hls_segment_filename", segPath);

        recorder.setOption("threads", "1");
        recorder.setVideoOption("threads", "1");

        recorder.setOption("preset", "ultrafast");
        recorder.setOption("tune", "zerolatency");
        recorder.setOption("crf", "26");
        recorder.setOption("maxrate", "800k");
        recorder.setOption("bufsize", "1200k");

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
        recorder.setOption("flags", "+low_delay");
        recorder.setOption("fflags", "+genpts");
        recorder.setOption("fps_mode", "cfr");

        return recorder;
    }

    public void stopHLSStream(String streamName) {
        logger.info("■ Stopping: " + streamName);

        AtomicBoolean stopFlag = streamStopFlags.get(streamName);
        if (stopFlag != null) stopFlag.set(true);

        Future<?> future = streamTasks.remove(streamName);
        if (future != null && !future.isDone()) {
            future.cancel(true);

            try {
                future.get(3, TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }

        cleanupStreamState(streamName);
    }

    private void cleanupStreamState(String streamName) {
        logger.info("🧹 Starting cleanup for: " + streamName);
        
        streamLinks.remove(streamName);
        streamRtspUrls.remove(streamName);
        streamStopFlags.remove(streamName);
        lastFrameTimes.remove(streamName);
        streamReconnectAttempts.remove(streamName);

        StreamResources resources = streamResources.remove(streamName);
        if (resources != null) {
            logger.info("🔧 Releasing FFmpeg resources: " + streamName);
            safeCleanup(streamName, resources.grabber, resources.recorder, true);
        }

        StreamStats stats = streamStats.remove(streamName);
        if (stats != null) {
            logger.info("📊 Final: " + stats.getFinalSummary());
        }

        deleteStreamFiles(streamName);
        
        // ✅ Remove lock after everything is cleaned up
        streamLocks.remove(streamName);
        
        logger.info("✅ Cleanup complete for: " + streamName);
    }

    /**
     * ✅ FIXED: More aggressive cleanup to prevent resource leaks
     */
    private void safeCleanup(String streamName, FFmpegFrameGrabber grabber, FFmpegFrameRecorder recorder,
            boolean recorderWasStarted) {
        streamResources.remove(streamName);

        if (recorder != null && recorderWasStarted) {
            try {
                recorder.stop();
            } catch (Exception e) {
                logger.warning("Recorder stop error for " + streamName + ": " + e.getMessage());
            }

            try {
                recorder.release();
            } catch (Exception e) {
                logger.warning("Recorder release error for " + streamName + ": " + e.getMessage());
            }
        }

        if (grabber != null) {
            try {
                grabber.stop();
            } catch (Exception e) {
                logger.warning("Grabber stop error for " + streamName + ": " + e.getMessage());
            }

            try {
                grabber.release();
            } catch (Exception e) {
                logger.warning("Grabber release error for " + streamName + ": " + e.getMessage());
            }
        }

        logger.info("✓ Cleanup complete: " + streamName);
    }

    private void deleteStreamFiles(String streamName) {
        try {
            File dir = new File(HLS_ROOT, streamName);
            if (dir.exists()) {
                File[] files = dir.listFiles();
                if (files != null) {
                    for (File f : files) f.delete();
                }
                dir.delete();
            }
        } catch (Exception ignored) {}
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

    public Map<String, Object> getSystemStats() {
        Map<String, Object> stats = new HashMap<>();
        
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        
        stats.put("activeStreams", streamLinks.size());
        stats.put("startupQueue", startupQueue.get());
        stats.put("maxStreams", MAX_STREAMS);
        
        ThreadPoolExecutor pool = (ThreadPoolExecutor) streamExecutor;
        
        Map<String, Object> poolStats = new HashMap<>();
        poolStats.put("active", pool.getActiveCount());
        poolStats.put("total", WORKER_THREADS);
        poolStats.put("queueSize", pool.getQueue().size());
        stats.put("threadPool", poolStats);
        
        Map<String, Object> memoryStats = new HashMap<>();
        memoryStats.put("usedMB", usedMemory / (1024 * 1024));
        memoryStats.put("maxMB", maxMemory / (1024 * 1024));
        memoryStats.put("usedPercent", (usedMemory * 100.0) / maxMemory);
        stats.put("memory", memoryStats);
        
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            com.sun.management.OperatingSystemMXBean sunOsBean = 
                (com.sun.management.OperatingSystemMXBean) osBean;
            Map<String, Object> cpuStats = new HashMap<>();
            cpuStats.put("systemLoad", sunOsBean.getSystemCpuLoad() * 100);
            cpuStats.put("processLoad", sunOsBean.getProcessCpuLoad() * 100);
            stats.put("cpu", cpuStats);
        }
        
        return stats;
    }

    @PreDestroy
    public void shutdown() {
        logger.info("╔════════════════════════════════════════════╗");
        logger.info("║  Shutting down HLS Service...             ║");
        logger.info("╚════════════════════════════════════════════╝");

        isShuttingDown.set(true);

        healthCheckScheduler.shutdown();
        memoryMonitor.shutdown();
        csvLogger.shutdown();
        
        try {
            healthCheckScheduler.awaitTermination(2, TimeUnit.SECONDS);
            memoryMonitor.awaitTermination(2, TimeUnit.SECONDS);
            csvLogger.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        streamStopFlags.values().forEach(flag -> flag.set(true));

        startupScheduler.shutdown();
        try {
            startupScheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        List<String> streamsToStop = new ArrayList<>(streamLinks.keySet());
        for (String streamName : streamsToStop) {
            try {
                stopHLSStream(streamName);
            } catch (Exception ignored) {}
        }

        streamExecutor.shutdown();
        try {
            if (!streamExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                streamExecutor.shutdownNow();
                streamExecutor.awaitTermination(10, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            streamExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (csvWriter != null) {
            try {
                csvWriter.close();
            } catch (Exception ignored) {}
        }

        logger.info("  ✓ Shutdown complete");
    }

    private static class StreamResources {
        final FFmpegFrameGrabber grabber;
        final FFmpegFrameRecorder recorder;

        StreamResources(FFmpegFrameGrabber grabber, FFmpegFrameRecorder recorder) {
            this.grabber = grabber;
            this.recorder = recorder;
        }
    }

    public static class StreamStats {
        final String streamName;
        final long startTime;
        private final LongAdder totalReadFrames = new LongAdder();
        private final LongAdder totalEncodedFrames = new LongAdder();
        private final LongAdder skippedFrames = new LongAdder();
        private final LongAdder errors = new LongAdder();
        private final AtomicInteger startAttempts = new AtomicInteger(0);
        private volatile String encoder = "unknown";
        private volatile String sourceCodec = "unknown";
        private volatile String resolution = "unknown";
        private volatile double sourceFps = 0.0;
        private volatile double currentFps = 0.0;
        private volatile long lastFrameCount = 0;
        private volatile long lastFpsUpdate = System.currentTimeMillis();

        public StreamStats(String streamName) {
            this.streamName = streamName;
            this.startTime = System.currentTimeMillis();
        }

        public void recordReadFrame() { totalReadFrames.increment(); }
        public void recordEncodedFrame() { totalEncodedFrames.increment(); }
        public void recordSkippedFrame() { skippedFrames.increment(); }
        public void recordError(Exception e) { errors.increment(); }
        public void recordStartAttempt() { startAttempts.incrementAndGet(); }
        public void setEncoder(String encoder) { this.encoder = encoder; }
        public void setSourceCodec(String codec) { this.sourceCodec = codec; }
        public void setSourceFps(double fps) { this.sourceFps = fps; }
        public void setResolution(int width, int height) { this.resolution = width + "x" + height; }
        public double getSourceFps() { return sourceFps; }

        public void updateFps() {
            long now = System.currentTimeMillis();
            long elapsed = now - lastFpsUpdate;
            if (elapsed > 0) {
                long current = totalEncodedFrames.sum();
                long framesSinceLastUpdate = current - lastFrameCount;
                currentFps = (framesSinceLastUpdate * 1000.0) / elapsed;
                lastFrameCount = current;
                lastFpsUpdate = now;
            }
        }

        public String getLogSummary() {
            long uptime = (System.currentTimeMillis() - startTime) / 1000;
            long read = totalReadFrames.sum();
            long encoded = totalEncodedFrames.sum();
            
            return String.format(
                "%s | %s→%s | %s | read:%d | enc:%d (%.1ffps) | err:%d | up:%ds",
                streamName, sourceCodec, encoder, resolution, 
                read, encoded, currentFps, errors.sum(), uptime);
        }

        public String getFinalSummary() {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;
            long read = totalReadFrames.sum();
            long encoded = totalEncodedFrames.sum();
            double avgFps = totalTime > 0 ? encoded / (double) totalTime : 0;
            return String.format(
                    "%s | Read:%d Enc:%d (%.1ffps) | Err:%d | Runtime:%ds",
                    streamName, read, encoded, avgFps, errors.sum(), totalTime);
        }

        public String getStreamName() { return streamName; }
        public int getTotalReadFrames() { return totalReadFrames.intValue(); }
        public int getTotalEncodedFrames() { return totalEncodedFrames.intValue(); }
        public int getSkippedFrames() { return skippedFrames.intValue(); }
        public int getErrors() { return errors.intValue(); }
        public String getEncoder() { return encoder; }
        public String getSourceCodec() { return sourceCodec; }
        public String getResolution() { return resolution; }
        public double getCurrentFps() { return currentFps; }
        public long getUptimeSeconds() { return (System.currentTimeMillis() - startTime) / 1000; }
    }
}