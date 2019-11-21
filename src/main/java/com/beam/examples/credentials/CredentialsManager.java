package com.beam.examples.credentials;

import com.beam.examples.DataflowPipeline;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import java.io.File;
import java.io.FileInputStream;

import static com.beam.examples.DataflowPipeline.logger;

public class CredentialsManager {

    public static GoogleCredentials loadGoogleCredentials(String credentialsFile) {
        File file = new File(credentialsFile);
        try {
            FileInputStream inputStream = new FileInputStream(file);
            ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(inputStream).toBuilder().build();
            String projectId = credentials.getProjectId();
            System.out.println(projectId);
            logger.info("[CredentialsManager] - Loaded Google Credentials from Service Account Key");
            return credentials;
        } catch (Exception e) {
            logger.error("[CredentialsManager] - Error while loading Google Crdentials from Service Account Key");
            e.printStackTrace();
        }

        return null;
    }

}
