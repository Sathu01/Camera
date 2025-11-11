package com.backendcam.backendcam;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class CameraStatusChecker {

    /** @return true = online, false = offline */
    public boolean isCameraOnline(String rtspUrl) {
        try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(rtspUrl)) {
            grabber.setOption("rtsp_transport", "tcp");
            grabber.setOption("stimeout", "2000000"); // ลดเป็น 2 วินาที
            grabber.setOption("analyzeduration", "1000000"); // 1 วินาที
            grabber.setOption("probesize", "32768"); // ลดขนาด probe
            
            grabber.start();
            // ลองอ่าน frame เดียวเพื่อยืนยันว่าเชื่อมต่อได้
            grabber.grab();
            grabber.stop();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Async
    public CompletableFuture<Boolean> isCameraOnlineAsync(String url) {
        // ใช้ CompletableFuture.supplyAsync เพื่อรันจริงๆ ใน thread pool
        return CompletableFuture.supplyAsync(() -> isCameraOnline(url))
            .orTimeout(3, TimeUnit.SECONDS) // timeout หลังจาก 3 วินาที
            .exceptionally(throwable -> {
                // ถ้า timeout หรือ error = offline
                return false;
            });
    }
}
