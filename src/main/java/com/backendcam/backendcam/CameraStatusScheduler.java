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

    Map<String, String> idToUrl = new LinkedHashMap<>();
    for (QueryDocumentSnapshot d : docs) {
      String url = FirestoreService.pickRtspUrl(d.getData()).orElse(null);
      idToUrl.put(d.getId(), url);
    }

    Map<String, CompletableFuture<Boolean>> tasks = new LinkedHashMap<>();
    idToUrl.forEach((docId, url) -> {
      if (url == null || url.isBlank()) {
        tasks.put(docId, CompletableFuture.completedFuture(false));
      } else {
        tasks.put(docId, cameraStatusChecker.isCameraOnlineAsync(url));
      }
    });

    // รอให้ future ทุกตัวจบ
    CompletableFuture
        .allOf(tasks.values().toArray(new CompletableFuture[0]))
        .join();

    int online = 0, offline = 0;

    for (Map.Entry<String, CompletableFuture<Boolean>> e : tasks.entrySet()) {
      String docId = e.getKey();
      boolean ok;
      try {
        ok = e.getValue().get();
      } catch (Exception ex) {
        System.out.printf("[Cameras] Future error for doc=%s | err=%s%n",
            docId, ex.getMessage());
        ok = false;
      }

      if (ok) {
        online++;
        String message = TimeAgoFormatter.humanizeSinceSeconds(0);
        firestoreService.updateOnline(docId, message);
      } else {
        offline++;
        firestoreService.updateOffline(docId);
      }
    }

    long ms = (System.nanoTime() - t0) / 1_000_000L;
    System.out.printf(
        "[Cameras] Checked %d docs in %.3f s → online=%d, offline=%d%n",
        docs.size(), ms / 1000.0, online, offline
    );
  }
}