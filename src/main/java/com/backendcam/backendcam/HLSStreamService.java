package com.backendcam.backendcam;

import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.javacv.*;
import org.springframework.stereotype.Service;
import java.io.File;
import java.util.logging.Logger;

@Service
public class HLSStreamService {

    private static final Logger logger = Logger.getLogger(HLSStreamService.class.getName());

    private static final String HLS_ROOT = "./hls"; // root folder inside project

    public String startHLSStream(String rtspUrl, String streamName) {
        try {
            logger.info("Starting HLS stream: " + rtspUrl + " as " + streamName);

            // RTSP grabber
            FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(rtspUrl);
            grabber.setOption("rtsp_transport", "tcp"); 
            grabber.setOption("rtsp_flags", "prefer_tcp");   // TCP transport for reliability
            grabber.setOption("stimeout", "10000000");     // timeout in microseconds
            grabber.setOption("fflags", "+nobuffer+genpts");
            grabber.setOption("analyzeduration", "1000000"); // smaller probe duration for low latency
            grabber.setOption("probesize", "32");
            grabber.setOption("loglevel", "debug");
           // smaller probe size
            grabber.start();

            logger.info("Grabber started. Resolution: " + grabber.getImageWidth() + "x" + grabber.getImageHeight());

            // Create output folder
            File outputDir = new File(HLS_ROOT + "/" + streamName);
            if (!outputDir.exists()) {
                boolean created = outputDir.mkdirs();
                logger.info("Created folder: " + outputDir.getAbsolutePath() + " - Success: " + created);
            }

            String hlsOutput = outputDir.getAbsolutePath() + "/stream.m3u8";

            // Recorder for HLS
            FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(
                    hlsOutput,
                    grabber.getImageWidth(),
                    grabber.getImageHeight(),
                    grabber.getAudioChannels()
            );

            // Video settings
            recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
            recorder.setFormat("hls");
            recorder.setFrameRate(30);
            recorder.setGopSize(60);

            // Audio settings
            if (grabber.getAudioChannels() > 0) {
                recorder.setAudioCodec(avcodec.AV_CODEC_ID_AAC);
                recorder.setSampleRate(grabber.getSampleRate());
                recorder.setAudioBitrate(128000);
            }

            // HLS options
            recorder.setOption("hls_time", "2"); // 2s segments
            recorder.setOption("hls_list_size", "3"); // small playlist for low latency
            recorder.setOption("hls_flags", "delete_segments+append_list+independent_segments");
            recorder.setOption("hls_segment_type", "mpegts");
            recorder.setOption("hls_allow_cache", "0");

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
            logger.info("Recorder started at " + hlsOutput);

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
}