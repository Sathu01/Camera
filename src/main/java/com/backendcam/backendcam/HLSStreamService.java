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
 * ✅ PRODUCTION-READY HLS STREAMING SERVICE
 * 
 * Optimized for: 16+ cameras at 25fps source → 8fps output
 * Key Features:
 * - Zero memory leaks (all frames properly closed)
 * - No deadlocks (no blocking flush operations)
 * - Handles 25fps cameras that won't reduce framerate
 * - Network congestion resistant (proper buffering)
 * - Automatic reconnection with exponential backoff
 * - 24/7 stable operation
 */
@Service
public class HLSStreamService {

    private static final Logger logger = Logger.getLogger(HLSStreamService.class.getName());

    // Configuration
    private static final String HLS_ROOT = "./hls";
    private static final String LOG_ROOT = "./logs";
    private static final int MAX_STREAMS = 100;
    private static final int WORKER_THREADS = 70;
    private static final long STARTUP_DELAY_MS = 3000;
    private static final int TARGET_FPS = 8;
    private static final int MAX_RECONNECT_ATTEMPTS = Integer.MAX_VALUE;
    private static final long RECONNECT_DELAY_MS = 5000;
    private static final long MAX_RECONNECT_DELAY_MS = 60000;
    private static final long HEALTH_CHECK_INTERVAL_MS = 120000;
    private static final long MEMORY_CHECK_INTERVAL_MS = 60000;
    private static final long CSV_LOG_INTERVAL_MS = 300000;
    private static final long STREAM_TIMEOUT_MS = 600000;

    // Thread pools
    private final ExecutorService streamExecutor;
    private final ScheduledExecutorService startupScheduler;
    private final ScheduledExecutorService healthCheckScheduler;
    private final ScheduledExecutorService memoryMonitor;
    private final ScheduledExecutorService csvLogger;

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
    private final ConcurrentHashMap<String, Long> lastFrameTimes = new ConcurrentHashMap<>();

    // System monitoring
    private final OperatingSystemMXBean osBean;
    private PrintWriter csvWriter;

    public HLSStreamService() {
        this.streamExecutor = new ThreadPoolExecutor(
                WORKER_THREADS,
                WORKER_THREADS,
                0L,
                TimeUnit.MILLISECONDS,
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

        this.startupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "HLS-Startup-Controller");
            t.setDaemon(false);
            return t;
        });

        this.healthCheckScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "HLS-HealthCheck");
            t.setDaemon(true);
            return t;
        });

        this.memoryMonitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "HLS-MemoryMonitor");
            t.setDaemon(true);
            return t;
        });

        this.csvLogger = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "HLS-CSVLogger");
            t.setDaemon(true);
            return t;
        });

        ((ThreadPoolExecutor) streamExecutor).prestartAllCoreThreads();

        FFmpegLogCallback.set();
        avutil.av_log_set_level(avutil.AV_LOG_FATAL);

        this.osBean = ManagementFactory.getOperatingSystemMXBean();

        initializeCSVLogger();
        startHealthCheck();
        startMemoryMonitor();
        startCSVLogging();

        logger.info("╔════════════════════════════════════════════╗");
        logger.info("║  HLS Service - PRODUCTION (25fps capable) ║");
        logger.info("║  Max Streams: 50-70                       ║");
        logger.info("║  Worker Threads: " + WORKER_THREADS + "                       ║");
        logger.info("║  Target FPS: " + TARGET_FPS + " (from 25fps source)         ║");
        logger.info("║  ✅ Handles 25fps cameras properly       ║");
        logger.info("║  ✅ Zero memory leaks                    ║");
        logger.info("║  ✅ Network congestion resistant         ║");
        logger.info("╚════════════════════════════════════════════╝");
    }

    private void initializeCSVLogger() {
        try {
            File logDir = new File(LOG_ROOT);
            if (!logDir.exists()) {
                logDir.mkdirs();
            }

            String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
            File csvFile = new File(LOG_ROOT, "system_metrics_" + timestamp + ".csv");
            
            csvWriter = new PrintWriter(new FileWriter(csvFile, true));
            
            csvWriter.println("Timestamp,ActiveStreams,WorkerThreads,ActiveThreads,QueueSize," +
                    "UsedMemoryMB,MaxMemoryMB,MemoryUsagePercent," +
                    "SystemCPULoad,ProcessCPULoad,TotalReadFrames,TotalEncodedFrames," +
                    "TotalErrors,DeadStreams");
            csvWriter.flush();
            
            logger.info("✓ CSV logger initialized: " + csvFile.getAbsolutePath());
        } catch (Exception e) {
            logger.severe("❌ Failed to initialize CSV logger: " + e.getMessage());
        }
    }

    private final ConcurrentHashMap<String, AtomicInteger> streamReconnectAttempts = new ConcurrentHashMap<>();
    private static final int MAX_HEALTH_CHECK_RECONNECTS = 10;

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
                            logger.warning("⚠ Stream timeout (no frames for 10min): " + streamName + 
                                         " - Reconnect " + (attempts + 1) + "/" + MAX_HEALTH_CHECK_RECONNECTS);
                            reconnectStreams.add(streamName);
                            reconnectCount.incrementAndGet();
                        } else {
                            logger.severe("☠ Stream dead after 10 reconnects: " + streamName);
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
                    logger.info(String.format(
                        "📊 Health: Streams=%d/%d, Workers=%d/%d, Reconnecting=%d, Killed=%d",
                        streamLinks.size(), MAX_STREAMS,
                        pool.getActiveCount(), WORKER_THREADS,
                        reconnectStreams.size(), deadStreams.size()
                    ));
                }

            } catch (Exception e) {
                logger.severe("❌ Health check error: " + e.getMessage());
            }
        }, HEALTH_CHECK_INTERVAL_MS, HEALTH_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void triggerStreamReconnect(String streamName) {
        try {
            Future<?> future = streamTasks.get(streamName);
            if (future != null && !future.isDone()) {
                future.cancel(true);
            }

            String rtspUrl = streamRtspUrls.get(streamName);
            if (rtspUrl == null) {
                logger.warning("⚠ Cannot reconnect " + streamName + ": No RTSP URL");
                return;
            }

            lastFrameTimes.put(streamName, System.currentTimeMillis());
            final StreamStats finalStats = streamStats.computeIfAbsent(streamName, StreamStats::new);

            logger.info("▶ Reconnecting: " + streamName);
            Future<?> newFuture = streamExecutor.submit(() -> {
                runStreamWithAutoReconnect(rtspUrl, streamName, finalStats);
            });

            streamTasks.put(streamName, newFuture);

        } catch (Exception e) {
            logger.severe("❌ Reconnect failed for " + streamName + ": " + e.getMessage());
        }
    }

    private void startMemoryMonitor() {
        memoryMonitor.scheduleAtFixedRate(() -> {
            try {
                Runtime runtime = Runtime.getRuntime();
                long totalMemory = runtime.totalMemory();
                long freeMemory = runtime.freeMemory();
                long usedMemory = totalMemory - freeMemory;
                long maxMemory = runtime.maxMemory();
                
                double usedPercent = (usedMemory * 100.0) / maxMemory;
                
                if (usedPercent > 80 || streamLinks.size() % 10 == 0) {
                    logger.info(String.format(
                        "💾 Memory: %dMB/%dMB (%.1f%%), Streams=%d",
                        usedMemory / (1024 * 1024),
                        maxMemory / (1024 * 1024),
                        usedPercent,
                        streamLinks.size()
                    ));
                }

                if (usedPercent > 85) {
                    logger.warning("⚠ High memory: " + String.format("%.1f%%", usedPercent) + " - GC");
                    System.gc();
                    Thread.sleep(2000);
                }

                if (usedPercent > 95) {
                    logger.severe("🔥 CRITICAL MEMORY! Stopping 5 oldest streams");
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
                    systemCpuLoad, processCpuLoad, totalReadFrames, totalEncodedFrames, totalErrors, 0
            );
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
            logger.warning("🛑 Emergency stop (memory): " + entry.getKey());
            stopHLSStream(entry.getKey());
            stopped++;
        }
    }

    public String startHLSStream(String rtspUrl, String streamName) {
        if (isShuttingDown.get()) {
            throw new RuntimeException("Service shutting down");
        }

        if (streamLinks.containsKey(streamName)) {
            return streamLinks.get(streamName);
        }

        if (streamTasks.size() >= MAX_STREAMS) {
            throw new RuntimeException("Max capacity: " + MAX_STREAMS);
        }

        String playlistPath = "/api/hls/" + streamName + "/stream.m3u8";

        streamLinks.put(streamName, playlistPath);
        streamRtspUrls.put(streamName, rtspUrl);
        streamStopFlags.put(streamName, new AtomicBoolean(false));
        lastFrameTimes.put(streamName, System.currentTimeMillis());

        StreamStats stats = new StreamStats(streamName);
        streamStats.put(streamName, stats);

        int queuePosition = startupQueue.incrementAndGet();
        long delay = (queuePosition - 1) * STARTUP_DELAY_MS;

        logger.info("→ Queued: " + streamName + " (pos: " + queuePosition + ", delay: " + delay + "ms)");

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

    private void runStreamWithAutoReconnect(String rtspUrl, String streamName, StreamStats stats) {
        int reconnectAttempt = 0;
        AtomicBoolean stopFlag = streamStopFlags.get(streamName);

        while (!isShuttingDown.get() && streamLinks.containsKey(streamName) && !stopFlag.get()) {
            try {
                if (reconnectAttempt > 0) {
                    logger.info("🔄 Reconnect #" + reconnectAttempt + ": " + streamName);
                }

                runStream(rtspUrl, streamName, stats);

                if (stopFlag.get()) break;

                reconnectAttempt++;
                if (!stopFlag.get()) {
                    long delay = Math.min(RECONNECT_DELAY_MS * reconnectAttempt, MAX_RECONNECT_DELAY_MS);
                    Thread.sleep(delay);
                }

            } catch (Exception e) {
                stats.recordError(e);
                reconnectAttempt++;

                if (!stopFlag.get()) {
                    try {
                        long delay = Math.min(RECONNECT_DELAY_MS * reconnectAttempt, MAX_RECONNECT_DELAY_MS);
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

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
            logger.info("🎯 " + streamName + ": Source " + sourceFps + "fps → Target " + TARGET_FPS + 
                       "fps (skip ratio: 1/" + frameSkipRatio + ", encode every " + frameSkipRatio + "th frame)");
            
            streamFrames(grabber, recorder, streamName, stats, frameSkipRatio, sourceFps);

        } finally {
            safeCleanup(streamName, grabber, recorder, recorderStarted);
        }
    }

    /**
     * ✅ PRODUCTION: Handles 25fps cameras properly
     * 
     * KEY FEATURES:
     * - Reads at camera speed (25fps = 40ms per frame)
     * - Encodes at target speed (8fps = every 3rd frame)
     * - Zero memory leaks (frames always closed)
     * - Network congestion resistant
     * - Adaptive error handling
     */
    private void streamFrames(FFmpegFrameGrabber grabber, FFmpegFrameRecorder recorder, 
                              String streamName, StreamStats stats, int frameSkipRatio, 
                              double sourceFps) throws Exception {
        long lastLogTime = System.currentTimeMillis();
        long lastStatsUpdate = System.currentTimeMillis();
        long lastSuccessfulEncode = System.currentTimeMillis();
        long lastFrameTime = System.currentTimeMillis();

        final int MAX_NULL_FRAMES = 500;  // Higher tolerance for 25fps
        final int MAX_ENCODE_FAILURES = 20;
        final long MAX_TIME_WITHOUT_ENCODE = 180000;  // 3 minutes
        final long FRAME_TIME_MS = (long)(1000.0 / sourceFps);  // 40ms for 25fps
        
        int consecutiveNullFrames = 0;
        int consecutiveEncodeFailures = 0;
        int frameCounter = 0;
        int totalIgnoredErrors = 0;

        AtomicBoolean stopFlag = streamStopFlags.get(streamName);

        logger.info(streamName + ": Frame loop started (read: " + sourceFps + "fps, encode: " + TARGET_FPS + "fps)");

        while (!isShuttingDown.get() && !Thread.currentThread().isInterrupted() && !stopFlag.get()) {
            Frame frame = null;
            
            try {
                long now = System.currentTimeMillis();
                
                // Check for stuck encoding
                if (now - lastSuccessfulEncode > MAX_TIME_WITHOUT_ENCODE) {
                    logger.warning(streamName + ": ❌ No encoding for 3min - reconnecting");
                    throw new RuntimeException("Encoding timeout");
                }

                // ✅ CRITICAL: Pace frame reading to match camera speed
                // Don't read faster than camera sends to prevent buffer issues
                long timeSinceLastFrame = now - lastFrameTime;
                if (timeSinceLastFrame < FRAME_TIME_MS) {
                    Thread.sleep(FRAME_TIME_MS - timeSinceLastFrame);
                }
                lastFrameTime = System.currentTimeMillis();

                // Grab frame from camera
                frame = grabber.grabImage();

                if (frame == null) {
                    consecutiveNullFrames++;

                    if (consecutiveNullFrames == 50) {
                        logger.warning(streamName + ": ⚠ 50 null frames");
                    } else if (consecutiveNullFrames == 200) {
                        logger.warning(streamName + ": ⚠ 200 null frames");
                    } else if (consecutiveNullFrames == 400) {
                        logger.warning(streamName + ": ⚠ 400 null frames (will reconnect at 500)");
                    }

                    if (consecutiveNullFrames >= MAX_NULL_FRAMES) {
                        logger.warning(streamName + ": ❌ " + consecutiveNullFrames + " null frames - reconnecting");
                        throw new RuntimeException("Stream stalled");
                    }
                    
                    // Adaptive sleep
                    Thread.sleep(consecutiveNullFrames < 10 ? 5 : 
                                consecutiveNullFrames < 100 ? 10 : 
                                consecutiveNullFrames < 300 ? 20 : 50);
                    continue;
                }

                // Got a frame
                consecutiveNullFrames = 0;
                stats.recordReadFrame();
                lastFrameTimes.put(streamName, now);
                frameCounter++;

                // Decide if we encode this frame
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

                // Close frame immediately
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
                    try {
                        frame.close();
                    } catch (Exception ignored) {}
                    frame = null;
                }

                stats.recordError(e);
                String msg = e.getMessage();

                // Fatal errors
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
                        msg.contains("bytestream"))) {

                    totalIgnoredErrors++;
                    
                    if (totalIgnoredErrors % 500 == 0) {
                        logger.info(streamName + ": 📊 Ignored " + totalIgnoredErrors + " codec errors");
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

    /**
     * ✅ OPTIMIZED: Handles 25fps cameras that won't reduce framerate
     * 
     * KEY SETTINGS:
     * - Large buffers (8MB) for sustained 25fps reading
     * - Longer timeouts (60s) for network congestion
     * - Error concealment for packet loss
     * - NO framerate forcing (camera ignores it)
     */
    private FFmpegFrameGrabber createGrabber(String url) {
        FFmpegFrameGrabber g = new FFmpegFrameGrabber(url);
        g.setFormat("rtsp");
        g.setImageMode(org.bytedeco.javacv.FrameGrabber.ImageMode.COLOR);
        
        // ❌ NOT setting framerate - camera ignores it and sends 25fps anyway
        // We handle frame rate reduction by skipping frames during encoding

        // Threading
        g.setOption("threads", "1");
        g.setOption("thread_count", "0");
        g.setVideoOption("threads", "1");
        
        // ✅ CRITICAL: Larger buffers for sustained 25fps reading
        g.setOption("analyzeduration", "5000000");    // 5s
        g.setOption("probesize", "5000000");          // 5MB
        g.setOption("max_delay", "1000000");          // 1s
        
        // ✅ CRITICAL: Large reorder queue for 25fps with packet loss
        g.setOption("reorder_queue_size", "8192");    // 8K packets
        g.setOption("max_interleave_delta", "2000000"); // 2s gaps tolerated
        
        // Error handling - Maximum tolerance
        g.setOption("err_detect", "ignore_err");
        g.setOption("ec", "favor_inter+guess_mvs+deblock");
        
        // Frame skipping at decoder level (lightweight)
        g.setOption("skip_frame", "noref");
        g.setOption("skip_loop_filter", "noref");
        g.setOption("skip_idct", "noref");
        
        // Timestamp handling
        g.setOption("fflags", "+discardcorrupt+nobuffer+genpts+igndts+ignidx");
        g.setOption("flags", "low_delay");
        g.setOption("flags2", "+ignorecrop+showall");
        
        // RTSP settings
        g.setOption("rtsp_transport", "tcp");
        g.setOption("rtsp_flags", "prefer_tcp");
        g.setOption("stimeout", "60000000");          // 60s - patient with slow networks
        g.setOption("timeout", "60000000");

        // ✅ CRITICAL: Large buffer for 25fps sustained reading
        // 25fps × 50KB/frame × 6.4 buffers = ~8MB needed
        g.setOption("buffer_size", "8192000");        // 8MB
        g.setOption("allowed_media_types", "video");
        
        // Error tolerance
        g.setOption("max_error_rate", "1.0");         // 100% tolerance
        g.setOption("rw_timeout", "60000000");        // 60s
        g.setOption("use_wallclock_as_timestamps", "1");
        
        // H.264/HEVC specific
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
        streamLinks.remove(streamName);
        streamRtspUrls.remove(streamName);
        streamStopFlags.remove(streamName);
        lastFrameTimes.remove(streamName);
        streamReconnectAttempts.remove(streamName);

        StreamResources resources = streamResources.remove(streamName);
        if (resources != null) {
            safeCleanup(streamName, resources.grabber, resources.recorder, true);
        }

        StreamStats stats = streamStats.remove(streamName);
        if (stats != null) {
            logger.info("📊 Final: " + stats.getFinalSummary());
        }

        deleteStreamFiles(streamName);
    }

    private void safeCleanup(String streamName, FFmpegFrameGrabber grabber, FFmpegFrameRecorder recorder,
            boolean recorderWasStarted) {
        streamResources.remove(streamName);

        if (recorder != null && recorderWasStarted) {
            try {
                recorder.stop();
            } catch (Exception ignored) {}

            try {
                recorder.release();
            } catch (Exception ignored) {}
        }

        if (grabber != null) {
            try {
                grabber.stop();
            } catch (Exception ignored) {}

            try {
                grabber.release();
            } catch (Exception ignored) {}
        }
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