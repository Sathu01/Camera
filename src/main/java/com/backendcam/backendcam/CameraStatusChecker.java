package com.backendcam.backendcam;

import java.util.concurrent.CompletableFuture;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class CameraStatusChecker {

    private static final Logger logger = LoggerFactory.getLogger(CameraStatusChecker.class);

    /**
     * ตรวจสอบว่า RTSP stream ใช้งานได้ไหม
     * @param rtspUrl RTSP URL ของกล้อง
     * @return true = online, false = offline
     */
    public boolean isCameraOnline(String rtspUrl) {
        FFmpegFrameGrabber grabber = null;
        try {
            grabber = new FFmpegFrameGrabber(rtspUrl);
            grabber.setOption("rtsp_transport", "tcp");
            grabber.setOption("stimeout", "5000000"); // 5 วินาที
            grabber.start(); // จะ throw exception ถ้า connect ไม่ได้

            // ถ้าถึงตรงนี้ได้ แสดงว่าเชื่อมต่อสำเร็จ
            logger.info("Camera online: {}", rtspUrl);
            return true;
        } catch (Exception e) {
            logger.warn("Camera offline: {} ({})", rtspUrl, e.getMessage());
            return false;
        } finally {
            try {
                if (grabber != null) grabber.stop();
            } catch (Exception ignore) {}
        }
    }

    @Async
    public CompletableFuture<Boolean> isCameraOnlineAsync(String url) {
        boolean result = isCameraOnline(url);
        return CompletableFuture.completedFuture(result);
    }
}
