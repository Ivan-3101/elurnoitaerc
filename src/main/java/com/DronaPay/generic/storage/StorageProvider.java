package com.DronaPay.generic.storage;

import java.io.InputStream;

/**
 * Interface for object storage providers (MinIO, S3, Azure Blob, GCS)
 */
public interface StorageProvider {

    /**
     * Upload a document to storage
     * @param path - full path including filename (e.g., "tenant1/HealthClaim/12345/doc.pdf")
     * @param content - document content as byte array
     * @param contentType - MIME type
     * @return URL or path to access the document
     */
    String uploadDocument(String path, byte[] content, String contentType) throws Exception;

    /**
     * Download a document from storage
     * @param path - full path to the document
     * @return InputStream of document content
     */
    InputStream downloadDocument(String path) throws Exception;

    /**
     * Delete a document from storage
     * @param path - full path to the document
     * @return true if deleted successfully
     */
    boolean deleteDocument(String path) throws Exception;

    /**
     * Check if a document exists
     * @param path - full path to the document
     * @return true if exists
     */
    boolean documentExists(String path) throws Exception;

    /**
     * Get the provider name (e.g., "minio", "s3", "azure")
     */
    String getProviderName();
}