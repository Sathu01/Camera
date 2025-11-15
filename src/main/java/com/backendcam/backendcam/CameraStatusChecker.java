package com.backendcam.backendcam;

import java.util.concurrent.CompletableFuture;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class CameraStatusChecker {

    /**
     * @return true = online, false = offline
     */
    public boolean isCameraOnline(String rtspUrl) {
        long start = System.nanoTime();

        try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(rtspUrl)) {
            // บังคับ format / transport ให้ชัด ๆ แบบเดียวกับที่ ffplay ใช้
            grabber.setFormat("rtsp");
            grabber.setOption("rtsp_transport", "tcp");
            // stimeout หน่วยเป็น microseconds → 5,000,000 = 5 วินาที
            grabber.setOption("stimeout", "5000000");

            // เริ่ม connect
            grabber.start();

            // ลอง grab มาสักเฟรมให้แน่ใจว่ามี stream จริง
            grabber.grab();

            long ms = (System.nanoTime() - start) / 1_000_000L;
            // System.out.printf("[CamCheck] ONLINE after %d ms | url=%s%n", ms, rtspUrl);
            return true;

        } catch (Exception e) {
            long ms = (System.nanoTime() - start) / 1_000_000L;
            System.out.printf("[CamCheck] OFFLINE after %d ms | url=%s | err=%s%n",
                    ms, rtspUrl, e.getMessage());
            return false;
        }
    }

    /**
     * รันเช็คแบบ async โดยใช้ Spring @Async อย่างเดียวพอ
     * ไม่ต้อง supplyAsync + orTimeout ให้มันตัดเร็วเกินไป
     */
    @Async
    public CompletableFuture<Boolean> isCameraOnlineAsync(String url) {
        boolean result = isCameraOnline(url);
        return CompletableFuture.completedFuture(result);
    }
}