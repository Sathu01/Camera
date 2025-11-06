package com.backendcam.backendcam.firestore;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
import com.google.firebase.cloud.FirestoreClient;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class FirestoreService {

    private static final String COLLECTION = "cameras";

    public List<QueryDocumentSnapshot> fetchAllCameras() {
        try {
            Firestore db = FirestoreClient.getFirestore();
            ApiFuture<QuerySnapshot> fut = db.collection(COLLECTION).get();
            return fut.get().getDocuments();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to fetch cameras", e);
        }
    }

    /**
     * อัปเดตสถานะ ONLINE: เขียน status และ lastSeen (timestamp + message)
     * ใช้ merge เสมอ (มีทับ, ไม่มีสร้าง)
     */
    public void updateOnline(String docId, String message, Instant now) {
        Firestore db = FirestoreClient.getFirestore();
        Map<String, Object> lastSeen = new HashMap<>();
        lastSeen.put("timestamp", now.toString()); // ISO-8601
        lastSeen.put("message", message);

        Map<String, Object> payload = new HashMap<>();
        payload.put("status", "online");
        payload.put("lastSeen", lastSeen);

        db.collection(COLLECTION).document(docId).set(payload, SetOptions.merge());
    }

    /**
     * อัปเดตสถานะ OFFLINE: เขียนเฉพาะ status (ไม่แตะ lastSeen)
     */
    public void updateOffline(String docId) {
        Firestore db = FirestoreClient.getFirestore();
        Map<String, Object> payload = new HashMap<>();
        payload.put("status", "offline");

        db.collection(COLLECTION).document(docId).set(payload, SetOptions.merge());
    }

    public static Optional<String> pickRtspUrl(Map<String, Object> data) {
        Object u1 = data.get("url");
        Object u2 = data.get("URL");
        String url = u1 instanceof String ? (String) u1 : (u2 instanceof String ? (String) u2 : null);
        return Optional.ofNullable(url).map(String::trim).filter(s -> !s.isEmpty());
    }
}