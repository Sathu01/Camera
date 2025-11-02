package com.example.camerastatus;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class CameraStatusScheduler {

  @Autowired
  private CameraStatusChecker cameraStatusChecker;

  private final List<String> rtspUrls = Arrays.asList(
    "rtsp://127.0.0.1:8554/cam1",
    "rtsp://192.168.1.10:554/live"
  );

  // รันทุก 60 วินาที
  @Scheduled(fixedRate = 60000)
  public void checkCameras() {
    System.out.println("=== Checking cameras ===");
    for (String url : rtspUrls) {
      boolean online = cameraStatusChecker.isCameraOnline(url);
      System.out.printf("%s -> %s%n", url, online ? "ONLINE" : "OFFLINE");
    }
  }
}
