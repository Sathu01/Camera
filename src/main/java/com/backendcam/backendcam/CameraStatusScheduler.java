package com.backendcam.backendcam;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class CameraStatusScheduler {

  @Autowired
  private CameraStatusChecker cameraStatusChecker;

  private final List<String> rtspUrls = new java.util.ArrayList<>();
  {
    for (int i = 1; i <= 16; i++) {
      rtspUrls.add("rtsp://127.0.0.1:8554/cam" + i);
    }
  }

  // รันทุก 60 วินาที
  @Scheduled(fixedRate = 60000)
  public void checkCameras() {
    List<CompletableFuture<Boolean>> futures = new ArrayList<>();

    for (String url : rtspUrls) {
      futures.add(cameraStatusChecker.isCameraOnlineAsync(url));
    }

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    for (int i = 0; i < rtspUrls.size(); i++) {
      try {
        boolean online = futures.get(i).get();
        System.out.printf("%s -> %s%n", rtspUrls.get(i), online ? "ONLINE" : "OFFLINE");
      } catch (Exception e) {
        System.out.printf("%s -> ERROR%n", rtspUrls.get(i));
      }
    }
  }
}
