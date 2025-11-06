package com.backendcam.backendcam;

import java.util.concurrent.CompletableFuture;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class CameraStatusChecker {

    /** @return true = online, false = offline */
    public boolean isCameraOnline(String rtspUrl) {
        try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(rtspUrl)) {
            grabber.setOption("rtsp_transport", "tcp");
            grabber.setOption("stimeout", "5000000"); // 5s
            grabber.start();
            grabber.stop();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Async
    public CompletableFuture<Boolean> isCameraOnlineAsync(String url) {
        return CompletableFuture.completedFuture(isCameraOnline(url));
    }
}
