package com.backendcam.backendcam; import org.bytedeco.ffmpeg.global.avcodec; import org.bytedeco.javacv.*; import org.springframework.stereotype.Service; import java.io.File; import java.util.logging.Logger; @Service public class HLSStreamService { private static final Logger logger = Logger.getLogger(HLSStreamService.class.getName()); private static final String HLS_ROOT = "./hls"; // root folder inside project public String startHLSStream(String rtspUrl, String streamName) { try { logger.info("Starting HLS stream: " + rtspUrl + " as " + streamName); // RTSP grabber FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(rtspUrl); grabber.setOption("rtsp_transport", "tcp"); grabber.setOption("rtsp_flags", "prefer_tcp"); // TCP transport for reliability grabber.setOption("stimeout", "10000000"); // timeout in microseconds grabber.setOption("fflags", "+nobuffer+genpts"); grabber.setOption("analyzeduration", "1000000"); // smaller probe duration for low latency grabber.setOption("probesize", "32"); grabber.setOption("loglevel", "debug"); // smaller probe size grabber.start(); logger.info("Grabber started. Resolution: " + grabber.getImageWidth() + "x" + grabber.getImageHeight()); // Create output folder File outputDir = new File(HLS_ROOT + "/" + streamName); if (!outputDir.exists()) { boolean created = outputDir.mkdirs(); logger.info("Created folder: " + outputDir.getAbsolutePath() + " - Success: " + created); } String hlsOutput = outputDir.getAbsolutePath() + "/stream.m3u8"; // Recorder for HLS FFmpegFrameRecorder recorder = new FFmpegFrameRecorder( hlsOutput, grabber.getImageWidth(), grabber.getImageHeight(), grabber.getAudioChannels() ); // Video settings recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264); recorder.setFormat("hls"); recorder.setFrameRate(30); recorder.setGopSize(60); // Audio settings if (grabber.getAudioChannels() > 0) { recorder.setAudioCodec(avcodec.AV_CODEC_ID_AAC); recorder.setSampleRate(grabber.getSampleRate()); recorder.setAudioBitrate(128000); } // HLS options recorder.setOption("hls_time", "2"); // 2s segments recorder.setOption("hls_list_size", "3"); // small playlist for low latency recorder.setOption("hls_flags", "delete_segments+append_list+independent_segments"); recorder.setOption("hls_segment_type", "mpegts"); recorder.setOption("hls_allow_cache", "0"); // Encoding optimization recorder.setOption("preset", "ultrafast"); recorder.setOption("tune", "zerolatency"); recorder.setOption("crf", "23"); recorder.setOption("maxrate", "2M"); recorder.setOption("bufsize", "4M"); recorder.setOption("fflags", "+genpts+igndts"); recorder.setOption("avoid_negative_ts", "make_zero"); recorder.setOption("fps_mode", "cfr"); recorder.start(); logger.info("Recorder started at " + hlsOutput); // Start streaming in a separate thread new Thread(() -> streamToHLS(grabber, recorder, streamName)).start(); return "/hls/" + streamName + "/stream.m3u8"; // relative path for controller } catch (Exception e) { logger.severe("Failed to start HLS stream: " + e.getMessage()); e.printStackTrace(); throw new RuntimeException("Failed to start HLS stream", e); } } private void streamToHLS(FFmpegFrameGrabber grabber, FFmpegFrameRecorder recorder, String streamName) { try { Frame frame; int count = 0; while ((frame = grabber.grab()) != null) { recorder.record(frame); count++; if (count % 100 == 0) logger.info("Processed " + count + " frames for " + streamName); } } catch (Exception e) { logger.severe("Error streaming " + streamName + ": " + e.getMessage()); stopHLSStream(streamName); } finally { try { if (recorder != null) { recorder.stop(); recorder.release(); } if (grabber != null) { grabber.stop(); grabber.release(); } } catch (Exception e) { logger.warning("Cleanup error: " + e.getMessage()); } } } public void stopHLSStream(String streamName) { File dir = new File(HLS_ROOT + "/" + streamName); if (dir.exists()) { File[] files = dir.listFiles(); if (files != null) for (File f : files) f.delete(); dir.delete(); } } }


//2

package com.backendcam.backendcam;

import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FFmpegLogCallback;
import org.springframework.stereotype.Service;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class HLSStreamService {

    private static final Logger logger = Logger.getLogger(HLSStreamService.class.getName());

    private static final String HLS_ROOT = "./hls"; // root folder inside project

    public String startHLSStream(String rtspUrl, String streamName) {
        try {
            logger.info("Starting HLS stream: " + rtspUrl + " as " + streamName);
            // Try to connect with fallback variants
            FFmpegFrameGrabber grabber = tryStartRtspWithFallback(rtspUrl);

            logger.info("Grabber started. Initial resolution: " + grabber.getImageWidth() + "x" + grabber.getImageHeight());

            // Create output folder
            File outputDir = new File(HLS_ROOT + "/" + streamName);
            if (!outputDir.exists()) {
                boolean created = outputDir.mkdirs();
                logger.info("Created folder: " + outputDir.getAbsolutePath() + " - Success: " + created);
            }

            String hlsOutput = outputDir.getAbsolutePath() + "/stream.m3u8";

            // Warm up: grab a few frames until we get a video frame with dimensions
            int warmupMax = 200; // limit to avoid infinite loop
            int warmCount = 0;
            Frame firstVideoFrame = null;
            while (warmCount < warmupMax) {
                Frame f = grabber.grab();
                if (f == null) break;
                if (f.image != null) { firstVideoFrame = f; break; }
                warmCount++;
            }
            int width = grabber.getImageWidth();
            int height = grabber.getImageHeight();
            if ((width <= 0 || height <= 0) && firstVideoFrame != null) {
                width = firstVideoFrame.imageWidth;
                height = firstVideoFrame.imageHeight;
            }
            if (width <= 0 || height <= 0) {
                throw new RuntimeException("Could not determine video resolution from RTSP stream");
            }

            // Recorder for HLS
            FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(
                    hlsOutput,
                    width,
                    height,
                    Math.max(0, grabber.getAudioChannels())
            );

            // Video settings
            recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
            recorder.setFormat("hls");
            int fps = Math.max(15, grabber.getFrameRate() > 0 ? (int)Math.round(grabber.getFrameRate()) : 25);
            recorder.setFrameRate(fps);
            recorder.setGopSize(2 * fps); // keyframe every ~2s

            // Audio settings
            if (grabber.getAudioChannels() > 0) {
                recorder.setAudioCodec(avcodec.AV_CODEC_ID_AAC);
                recorder.setSampleRate(grabber.getSampleRate() > 0 ? grabber.getSampleRate() : 8000);
                recorder.setAudioBitrate(128000);
            }

            // HLS options
            recorder.setOption("hls_time", "2"); // 2s segments
            recorder.setOption("hls_list_size", "6"); // slightly larger playlist window
            recorder.setOption("hls_flags", "delete_segments+independent_segments+program_date_time");
            recorder.setOption("hls_segment_type", "mpegts");
            recorder.setOption("hls_allow_cache", "0");
            // Explicit segment file pattern (relative to playlist directory) so playlist uses HTTP-friendly URLs
            recorder.setOption("hls_segment_filename", "seg%05d.ts");
            // For live streaming playlists
            recorder.setOption("hls_playlist_type", "event");
            // Try to keep timestamps monotonic for players
            recorder.setOption("reset_timestamps", "1");

            // Encoding optimization
            recorder.setOption("preset", "ultrafast");
            recorder.setOption("tune", "zerolatency");
            recorder.setOption("crf", "23");
            recorder.setOption("maxrate", "2M");
            recorder.setOption("bufsize", "4M");
            recorder.setOption("fflags", "+genpts+igndts");
            recorder.setOption("avoid_negative_ts", "make_zero");
            recorder.setOption("fps_mode", "cfr");

            recorder.start();
            logger.info("Recorder started at " + hlsOutput + " with size " + width + "x" + height);

            // Start streaming in a separate thread
            new Thread(() -> streamToHLS(grabber, recorder, streamName)).start();

            return "/hls/" + streamName + "/stream.m3u8"; // relative path for controller
        } catch (Exception e) {
            logger.severe("Failed to start HLS stream: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to start HLS stream", e);
        }
    }

    private void streamToHLS(FFmpegFrameGrabber grabber, FFmpegFrameRecorder recorder, String streamName) {
        try {
            Frame frame;
            int count = 0;
            while ((frame = grabber.grab()) != null) {
                recorder.record(frame);
                count++;
                if (count % 100 == 0) logger.info("Processed " + count + " frames for " + streamName);
            }
        } catch (Exception e) {
            logger.severe("Error streaming " + streamName + ": " + e.getMessage());
            stopHLSStream(streamName);
        } finally {
            try {
                if (recorder != null) { recorder.stop(); recorder.release(); }
                if (grabber != null) { grabber.stop(); grabber.release(); }
            } catch (Exception e) { logger.warning("Cleanup error: " + e.getMessage()); }
        }
    }

    public void stopHLSStream(String streamName) {
        File dir = new File(HLS_ROOT + "/" + streamName);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) for (File f : files) f.delete();
            dir.delete();
        }
    }

    // --- Helpers ---
    private FFmpegFrameGrabber tryStartRtspWithFallback(String rtspUrl) throws Exception {
        FFmpegLogCallback.set();

        List<String> candidates = new ArrayList<>();
        candidates.add(rtspUrl);
        try {
            URI u = new URI(rtspUrl);
            String userInfo = u.getUserInfo();
            String base = u.getScheme() + "://" + (userInfo != null ? userInfo + "@" : "") + u.getHost() + (u.getPort() > 0 ? ":" + u.getPort() : "");
            String path = u.getPath() != null ? u.getPath() : "/";

            // If path looks like vendor indices (e.g., /5/1) try common variants
            if (path.matches("/.+/.*")) {
                // Hikvision-like: /Streaming/Channels/101
                candidates.add(base + "/Streaming/Channels/101");
                candidates.add(base + "/Streaming/Channels/102");
                // Dahua-like: /cam/realmonitor?channel=1&subtype=0
                candidates.add(base + "/cam/realmonitor?channel=1&subtype=0");
                // Other common variants
                candidates.add(base + "/live");
                candidates.add(base + "/live.sdp");
                candidates.add(base + "/h264");
                candidates.add(base + "/ch1-s1");
                candidates.add(base + "/unicast/c1/s0/live");
                candidates.add(base + "/avstream/channel=1/stream=0.sdp");
            }
            // Try trackID suffixes
            candidates.add(base + path + (path.endsWith("/") ? "" : "/") + "trackID=0");
            candidates.add(base + path + (path.endsWith("/") ? "" : "/") + "trackID=1");
        } catch (Exception ignore) {}

        Exception last = null;
        for (String cand : candidates) {
            for (String transport : new String[]{"tcp","udp","http"}) {
                logger.info("Trying RTSP URL: " + cand + " via " + transport);
                FFmpegFrameGrabber g = new FFmpegFrameGrabber(cand);
                g.setFormat("rtsp");
                g.setOption("rtsp_transport", transport);
                if ("tcp".equals(transport)) g.setOption("rtsp_flags", "prefer_tcp");
                g.setOption("stimeout", "20000000");
                g.setOption("rw_timeout", "20000000");
                g.setOption("analyzeduration", "5000000");
                g.setOption("probesize", "5000000");
                g.setOption("fflags", "+nobuffer+genpts+igndts");
                g.setOption("max_delay", "500000");
                g.setOption("reorder_queue_size", "0");
                g.setOption("use_wallclock_as_timestamps", "1");
                g.setOption("allowed_media_types", "video");
                // Some cameras only accept known User-Agents; emulate VLC
                g.setOption("user_agent", "LibVLC/3.0.18 (LIVE555 Streaming Media v2021.06.08)");
                g.setOption("loglevel", "info");
                try {
                    g.start();
                    logger.info("Connected to RTSP: " + cand + ", size=" + g.getImageWidth() + "x" + g.getImageHeight());
                    return g;
                } catch (Exception e) {
                    last = e;
                    logger.warning("Failed URL/transport: " + cand + " (" + transport + ") - " + e.getMessage());
                    try { g.release(); } catch (Exception ignore) {}
                }
            }
        }
        if (last != null) throw last;
        throw new RuntimeException("RTSP connection failed for all candidates");
    }
}