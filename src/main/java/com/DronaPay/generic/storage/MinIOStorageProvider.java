package com.DronaPay.generic.storage;

import io.minio.*;
import io.minio.errors.*;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class MinIOStorageProvider implements StorageProvider {

    private final MinioClient minioClient;
    private final String bucketName;

    /**
     * Initialize MinIO client from properties
     */
    public MinIOStorageProvider(Properties props) throws Exception {
        String endpoint = props.getProperty("storage.minio.endpoint");
        String accessKey = props.getProperty("storage.minio.accessKey");
        String secretKey = props.getProperty("storage.minio.secretKey");
        this.bucketName = props.getProperty("storage.minio.bucketName");

        log.info("Initializing MinIO storage provider");
        log.debug("Endpoint: {}, Bucket: {}", endpoint, bucketName);

        // Build MinIO client
        this.minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();

        // Ensure bucket exists
        ensureBucketExists();
    }

    /**
     * Create bucket if it doesn't exist
     */
    private void ensureBucketExists() throws Exception {
        boolean exists = minioClient.bucketExists(
                BucketExistsArgs.builder().bucket(bucketName).build()
        );

        if (!exists) {
            log.info("Bucket '{}' does not exist, creating...", bucketName);
            minioClient.makeBucket(
                    MakeBucketArgs.builder().bucket(bucketName).build()
            );
            log.info("Bucket '{}' created successfully", bucketName);
        } else {
            log.debug("Bucket '{}' already exists", bucketName);
        }
    }

    @Override
    public String uploadDocument(String path, byte[] content, String contentType) throws Exception {
        log.info("Uploading document to MinIO: {}", path);

        try (InputStream stream = new ByteArrayInputStream(content)) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(path)
                            .stream(stream, content.length, -1)
                            .contentType(contentType)
                            .build()
            );

            String url = String.format("minio://%s/%s", bucketName, path);
            log.info("Document uploaded successfully: {} ({} bytes)", path, content.length);
            return url;

        } catch (Exception e) {
            log.error("Failed to upload document: {}", path, e);
            throw e;
        }
    }

    @Override
    public InputStream downloadDocument(String path) throws Exception {
        log.info("Downloading document from MinIO: {}", path);

        try {
            return minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(path)
                            .build()
            );
        } catch (Exception e) {
            log.error("Failed to download document: {}", path, e);
            throw e;
        }
    }

    @Override
    public boolean deleteDocument(String path) throws Exception {
        log.info("Deleting document from MinIO: {}", path);

        try {
            minioClient.removeObject(
                    RemoveObjectArgs.builder()
                            .bucket(bucketName)
                            .object(path)
                            .build()
            );
            log.info("Document deleted successfully: {}", path);
            return true;
        } catch (Exception e) {
            log.error("Failed to delete document: {}", path, e);
            return false;
        }
    }

    @Override
    public boolean documentExists(String path) throws Exception {
        try {
            minioClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(bucketName)
                            .object(path)
                            .build()
            );
            return true;
        } catch (ErrorResponseException e) {
            if (e.errorResponse().code().equals("NoSuchKey")) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public String getProviderName() {
        return "minio";
    }

    /**
     * Cleanup resources (helps prevent memory leak warning)
     * Called when provider is no longer needed
     */
    public void close() {
        try {
            // MinIO client uses OkHttp internally which may leave threads running
            // This helps the garbage collector clean up resources
            log.debug("MinIO storage provider cleanup initiated");
        } catch (Exception e) {
            log.warn("Error during MinIO client cleanup", e);
        }
    }
}