package com.backendcam.backendcam.firestore;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;

@Configuration
public class FirebaseAdminBootstrap {
    @PostConstruct
    public void init() {
        try {
            if (!FirebaseApp.getApps().isEmpty()) return;

            String saPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
            if (saPath == null || saPath.isBlank()) {
                throw new IllegalStateException("Set env GOOGLE_APPLICATION_CREDENTIALS to your serviceAccount.json");
            }
            FileInputStream in = new FileInputStream(saPath);
            FirebaseOptions options = FirebaseOptions.builder()
                    .setCredentials(GoogleCredentials.fromStream(in))
                    .build();

            FirebaseApp.initializeApp(options);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Firebase Admin SDK", e);
        }
    }
}