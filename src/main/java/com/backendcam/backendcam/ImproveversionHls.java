package com.backendcam.backendcam;

import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.javacv.*;
import org.springframework.stereotype.Service;

import java.io.File;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * HLSStreamServiceEventDriven
 *
 * Hybrid "event-driven-like" implementation using:
 * - a single eventLoop (ScheduledExecutorService) that polls active streams
 * - a grabExecutor (pool) which performs blocking grab() calls inside Futures
 * with timeout
 * - a recorderExecutor (pool) which writes frames to recorder asynchronously
 *
 * This avoids one-thread-per-stream while using JavaCV/FFmpeg.
 */
@Service
public class ImproveversionHls {

    private static final Logger logger = Logger.getLogger(ImproveversionHls.class.getName());
    private static final String HLS_ROOT = "./hls";

    // Active streams map
    private final ConcurrentHashMap<String, StreamContext> activeStreams = new ConcurrentHashMap<>();

    // Event loop: 1 thread polling cycle
    private final ScheduledExecutorService eventLoop = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "hls-event-loop");
        t.setDaemon(true);
        return t;
    });

    // Worker pools
    private final ExecutorService grabExecutor; // for blocking grab() wrapped in Future
    private final ExecutorService recorderExecutor; // for recorder.record(frame) tasks

    // Configs
    private final int grabTimeoutMs = 25; // how long to wait for blocking grab before cancel (tune)
    private final int pollIntervalMs = 10; // event loop tick interval (tune)
    private final int maxStaleSeconds = 10; // if no frames in N sec -> consider reconnect

    public ImproveversionHls() {
        int cores = Math.max(2, Runtime.getRuntime().availableProcessors());
        // grabExecutor size: some parallelism for blocking grabs
        this.grabExecutor = Executors.newFixedThreadPool(cores, r -> {
            Thread t = new Thread(r, "hls-grab-worker");
            t.setDaemon(true);
            return t;
        });
        // recorderExecutor: small pool to perform writes
        this.recorderExecutor = Executors.newFixedThreadPool(Math.max(2, cores / 2), r -> {
            Thread t = new Thread(r, "hls-recorder-worker");
            t.setDaemon(true);
            return t;
        });

        // start event loop
        eventLoop.scheduleAtFixedRate(this::pollActiveStreams, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Start a stream (async-ish). Returns m3u8 path (relative).
     */
    public String startHLSStream(String rtspUrl, String streamName) {
        try {
            if (activeStreams.containsKey(streamName)) {
                logger.info("Stream already active: " + streamName);
                return "/hls/" + streamName + "/stream.m3u8";
            }

            logger.info("Starting async HLS stream: " + streamName + " <- " + rtspUrl);

            // Connect using helper (blocking) — you may want to start grabber connection in
            // async too
            FFmpegFrameGrabber grabber = tryStartRtspWithFallback(rtspUrl);
            int width = grabber.getImageWidth();
            int height = grabber.getImageHeight();
            int audioChannels = Math.max(0, grabber.getAudioChannels());

            // prepare output dir & recorder
            File outDir = new File(HLS_ROOT, streamName);
            if (!outDir.exists())
                outDir.mkdirs();
            String output = outDir.getAbsolutePath() + "/stream.m3u8";

            FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(output, width, height, audioChannels);
            recorder.setFormat("hls");
            recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
            recorder.setFrameRate(
                    Math.max(15, (int) Math.round(grabber.getFrameRate() > 0 ? grabber.getFrameRate() : 25)));
            int fps = Math.max(15, grabber.getFrameRate() > 0 ? (int) Math.round(grabber.getFrameRate()) : 25);
            recorder.setFrameRate(fps);
            recorder.setGopSize(2 * fps);

            if (audioChannels > 0) {
                recorder.setAudioCodec(avcodec.AV_CODEC_ID_AAC);
                recorder.setSampleRate(grabber.getSampleRate() > 0 ? grabber.getSampleRate() : 8000);
                recorder.setAudioBitrate(128000);
            }

            // HLS options
            recorder.setOption("hls_time", "2");
            recorder.setOption("hls_list_size", "3");
            recorder.setOption("hls_flags", "delete_segments+independent_segments+program_date_time");
            recorder.setOption("hls_segment_type", "mpegts");
            String seg = outDir.getAbsolutePath().replace('\\', '/') + "/seg%05d.ts";
            recorder.setOption("hls_segment_filename", seg);
            recorder.setOption("hls_allow_cache", "0");

            // encoder tuning
            recorder.setOption("preset", "ultrafast");
            recorder.setOption("tune", "zerolatency");
            recorder.setOption("crf", "23");
            recorder.setOption("maxrate", "2M");
            recorder.setOption("bufsize", "4M");
            recorder.setOption("fflags", "+genpts+igndts");
            recorder.setOption("avoid_negative_ts", "make_zero");
            recorder.setOption("fps_mode", "cfr");

            recorder.start();

            // create context and register
            StreamContext ctx = new StreamContext(streamName, rtspUrl, grabber, recorder);
            activeStreams.put(streamName, ctx);

            logger.info("Registered stream: " + streamName + " -> " + output);
            return "/hls/" + streamName + "/stream.m3u8";
        } catch (Exception e) {
            logger.severe("Failed to start stream " + streamName + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Poll active streams (event loop). For each stream submit a grab task with
     * timeout.
     */
    private void pollActiveStreams() {
        for (Map.Entry<String, StreamContext> e : activeStreams.entrySet()) {
            StreamContext ctx = e.getValue();
            if (!ctx.running.get())
                continue;

            // Submit a grab task to grabExecutor and wait with timeout
            Future<Frame> f = grabExecutor.submit(() -> {
                try {
                    // blocking grab (native) — run inside grabExecutor thread
                    return ctx.grabber.grab();
                } catch (Exception ex) {
                    throw ex;
                }
            });

            try {
                Frame frame = f.get(grabTimeoutMs, TimeUnit.MILLISECONDS);
                if (frame != null) {
                    ctx.lastFrameTime = Instant.now();
                    // hand off frame to recorderExecutor to not block grab workers
                    recorderExecutor.submit(() -> {
                        try {
                            ctx.recorder.record(frame);
                        } catch (Exception recEx) {
                            logger.warning("Recorder error for " + ctx.streamName + ": " + recEx.getMessage());
                        }
                    });
                } else {
                    // null frame may indicate EOF/disconnect
                    logger.fine("Received null frame for " + ctx.streamName);
                    checkStaleAndReconnect(ctx);
                }
            } catch (TimeoutException te) {
                // no frame within timeout -> cancel the grab task and move on
                f.cancel(true); // attempt interrupt
                logger.finest("Grab timeout for " + ctx.streamName + ", skipping this tick");
                // no frame this tick — not necessarily error
                checkStaleAndReconnect(ctx);
            } catch (ExecutionException ee) {
                logger.warning("Grabber exception for " + ctx.streamName + ": " + ee.getCause());
                checkStaleAndReconnect(ctx);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.warning("Event loop interrupted");
            }
        }
    }

    /**
     * If a stream hasn't produced frames for a while, try to reconnect or stop.
     */
    private void checkStaleAndReconnect(StreamContext ctx) {
        Instant now = Instant.now();
        if (ctx.lastFrameTime == null) {
            // first frame not arrived yet — allow some startup time
            if (ctx.startedAt.plusSeconds(maxStaleSeconds).isBefore(now)) {
                logger.warning("No frames received after startup for " + ctx.streamName + ", restarting");
                restartStream(ctx);
            }
            return;
        }
        if (ctx.lastFrameTime.plusSeconds(maxStaleSeconds).isBefore(now)) {
            logger.warning("Stream stale for " + ctx.streamName + ", reconnecting");
            restartStream(ctx);
        }
    }

    /**
     * Restart a failing stream: cleanup and re-open resources asynchronously.
     */
    private void restartStream(StreamContext ctx) {
        // mark not running to avoid duplicate reconnect attempts
        if (!ctx.running.compareAndSet(true, false))
            return;
        recorderExecutor.submit(() -> {
            try {
                cleanupContext(ctx);
            } catch (Exception ignored) {
            }
            // attempt reconnect with small backoff
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            try {
                FFmpegFrameGrabber newGrab = tryStartRtspWithFallback(ctx.rtspUrl);
                ctx.grabber = newGrab;
                ctx.recorder.start(); // recorder already started; if stopped, handle accordingly
                ctx.lastFrameTime = null;
                ctx.startedAt = Instant.now();
                ctx.running.set(true);
                logger.info("Reconnected stream " + ctx.streamName);
            } catch (Exception ex) {
                logger.warning("Reconnect failed for " + ctx.streamName + ": " + ex.getMessage());
                // final cleanup
                activeStreams.remove(ctx.streamName);
                try {
                    ctx.recorder.stop();
                    ctx.recorder.release();
                } catch (Exception ignored) {
                }
            }
        });
    }

    /**
     * Stop and cleanup one stream
     */
    public void stopHLSStream(String streamName) {
        StreamContext ctx = activeStreams.remove(streamName);
        if (ctx == null)
            return;
        ctx.running.set(false);
        recorderExecutor.submit(() -> cleanupContext(ctx));
        // remove HLS files
        File dir = new File(HLS_ROOT, streamName);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null)
                for (File f : files)
                    f.delete();
            dir.delete();
        }
        logger.info("Stopped stream " + streamName);
    }

    private void cleanupContext(StreamContext ctx) {
        try {
            if (ctx.recorder != null) {
                try {
                    ctx.recorder.stop();
                } catch (Exception ignored) {
                }
                try {
                    ctx.recorder.release();
                } catch (Exception ignored) {
                }
            }
        } finally {
            try {
                if (ctx.grabber != null) {
                    try {
                        ctx.grabber.stop();
                    } catch (Exception ignored) {
                    }
                    try {
                        ctx.grabber.release();
                    } catch (Exception ignored) {
                    }
                }
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Helper: Connect RTSP with fallback variants (same idea as your original)
     */
    private FFmpegFrameGrabber tryStartRtspWithFallback(String rtspUrl) throws Exception {
        FFmpegLogCallback.set();
        List<String> candidates = new ArrayList<>();
        candidates.add(rtspUrl);
        try {
            URI u = new URI(rtspUrl);
            String userInfo = u.getUserInfo();
            String base = u.getScheme() + "://" + (userInfo != null ? userInfo + "@" : "") + u.getHost()
                    + (u.getPort() > 0 ? ":" + u.getPort() : "");
            String path = u.getPath() != null ? u.getPath() : "/";
            if (path.matches("/.+/.*")) {
                candidates.add(base + "/Streaming/Channels/101");
                candidates.add(base + "/Streaming/Channels/102");
                candidates.add(base + "/cam/realmonitor?channel=1&subtype=0");
                candidates.add(base + "/live");
                candidates.add(base + "/live.sdp");
                candidates.add(base + "/h264");
            }
            candidates.add(base + path + (path.endsWith("/") ? "" : "/") + "trackID=0");
            candidates.add(base + path + (path.endsWith("/") ? "" : "/") + "trackID=1");
        } catch (Exception ignore) {
        }

        Exception last = null;
        for (String cand : candidates) {
            for (String transport : new String[] { "tcp", "udp", "http" }) {
                logger.info("Trying " + cand + " via " + transport);
                FFmpegFrameGrabber g = new FFmpegFrameGrabber(cand);
                g.setFormat("rtsp");
                g.setOption("rtsp_transport", transport);
                if ("tcp".equals(transport))
                    g.setOption("rtsp_flags", "prefer_tcp");
                g.setOption("stimeout", "20000000");
                g.setOption("rw_timeout", "20000000");
                g.setOption("analyzeduration", "1000000");
                g.setOption("probesize", "1000000");
                g.setOption("fflags", "+nobuffer+genpts+igndts");
                g.setOption("max_delay", "0");
                g.setOption("reorder_queue_size", "0");
                g.setOption("use_wallclock_as_timestamps", "1");
                g.setOption("allowed_media_types", "video");
                try {
                    g.start();
                    logger.info("Connected to RTSP: " + cand + " size=" + g.getImageWidth() + "x" + g.getImageHeight());
                    return g;
                } catch (Exception ex) {
                    last = ex;
                    logger.warning("Failed connect " + cand + " (" + transport + "): " + ex.getMessage());
                    try {
                        g.release();
                    } catch (Exception ignored) {
                    }
                }
            }
        }
        if (last != null)
            throw last;
        throw new RuntimeException("RTSP connection failed for all candidates");
    }

    // Stream context holder
    private static class StreamContext {
        final String streamName;
        final String rtspUrl;
        volatile FFmpegFrameGrabber grabber;
        final FFmpegFrameRecorder recorder;
        final AtomicBoolean running = new AtomicBoolean(true);
        Instant lastFrameTime = null;
        Instant startedAt = Instant.now();

        StreamContext(String name, String url, FFmpegFrameGrabber g, FFmpegFrameRecorder r) {
            this.streamName = name;
            this.rtspUrl = url;
            this.grabber = g;
            this.recorder = r;
            this.startedAt = Instant.now();
        }
    }
}
