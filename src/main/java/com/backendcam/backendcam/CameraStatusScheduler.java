package com.backendcam.backendcam;

import com.backendcam.backendcam.firestore.FirestoreService;
import com.backendcam.backendcam.util.TimeAgoFormatter;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;

@Component
public class CameraStatusScheduler {

  private final CameraStatusChecker cameraStatusChecker;
  private final FirestoreService firestoreService;

  public CameraStatusScheduler(CameraStatusChecker checker, FirestoreService fsService) {
    this.cameraStatusChecker = checker;
    this.firestoreService = fsService;
  }

  // รันทุก 60 วินาที
  @Scheduled(fixedRate = 60_000)
  public void checkCameras() {
    long t0 = System.nanoTime();

    List<QueryDocumentSnapshot> docs = firestoreService.fetchAllCameras();
    if (docs.isEmpty()) {
      System.out.println("[Cameras] Checked 0 docs in 0.000 s → online=0, offline=0");
      return;
    }

    Map<String, String> idToUrl = new HashMap<>();
    for (QueryDocumentSnapshot d : docs) {
      String url = FirestoreService.pickRtspUrl(d.getData()).orElse(null);
      idToUrl.put(d.getId(), url);
    }

    // ทำงานขนานด้วย CompletableFuture (ใช้ @Async ภายใน checker)
    Map<String, CompletableFuture<Boolean>> tasks = new LinkedHashMap<>();
    idToUrl.forEach((docId, url) -> {
      if (url == null || url.isBlank()) {
        // ไม่มี URL → ถือว่า offline ทันที
        tasks.put(docId, CompletableFuture.completedFuture(false));
      } else {
        tasks.put(docId, cameraStatusChecker.isCameraOnlineAsync(url));
      }
    });

    // รอทุกงานเสร็จ (หรือ timeout ภายใน 10 วินาที)
    try {
      CompletableFuture.allOf(tasks.values().toArray(new CompletableFuture[0]))
        .get(10, java.util.concurrent.TimeUnit.SECONDS);
    } catch (Exception e) {
      System.out.println("[Cameras] Some checks timed out: " + e.getMessage());
    }

    int online = 0, offline = 0;

    // อัปเดตผลลัพธ์กลับ Firestore ตามโจทย์
    for (Map.Entry<String, CompletableFuture<Boolean>> e : tasks.entrySet()) {
      String docId = e.getKey();
      boolean ok = false;
      try {
        ok = e.getValue().get();
      } catch (Exception ignore) {
        ok = false;
      }

      if (ok) {
        online++;
        // online ตอนนี้ → message เป็น "just now"
        String message = TimeAgoFormatter.humanizeSinceSeconds(0);
        firestoreService.updateOnline(docId, message);
      } else {
        offline++;
        // offline → ไม่แตะ lastSeen
        firestoreService.updateOffline(docId);
      }
    }

    long ms = (System.nanoTime() - t0) / 1_000_000L;
    System.out.printf("[Cameras] Checked %d docs in %.3f s → online=%d, offline=%d%n",
        docs.size(), ms / 1000.0, online, offline);
  }
}