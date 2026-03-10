package com.DronaPay.generic.services;

import com.DronaPay.generic.storage.StorageProvider;
import lombok.extern.slf4j.Slf4j;
import org.cibseven.bpm.engine.variable.Variables;
import org.cibseven.bpm.engine.variable.value.FileValue;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

@Slf4j
public class DocumentProcessingService {

    public static Map<String, String> processAndUploadDocuments(
            Object docsObject, String tenantId, String workflowKey, String ticketId, String stageName) {

        Map<String, String> documentPaths = new HashMap<>();

        if (docsObject == null) {
            log.warn("No documents provided in 'docs' variable");
            return documentPaths;
        }

        try {
            StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);

            Properties props = ConfigurationService.getTenantProperties(tenantId);
            String pathPattern = props.getProperty("storage.pathPattern",
                    "{tenantId}/{workflowKey}/{ticketId}/{stageName}/");

            List<Map<String, Object>> docsList = convertToDocsList(docsObject);

            for (Map<String, Object> doc : docsList) {
                String filename = doc.get("filename").toString();
                String mimetype = doc.get("mimetype").toString();
                String base64Content = doc.get("content").toString();

                byte[] fileContent = Base64.getDecoder().decode(base64Content);

                String storagePath = ObjectStorageService.buildStoragePath(
                        pathPattern, tenantId, workflowKey, ticketId, stageName, filename
                );

                String documentUrl = storage.uploadDocument(storagePath, fileContent, mimetype);
                documentPaths.put(filename, storagePath);

                log.info("Uploaded document: {} -> {} ({} bytes)",
                        filename, storagePath, fileContent.length);
            }

            log.info("Successfully uploaded {} documents to storage", documentPaths.size());

        } catch (Exception e) {
            log.error("Error processing and uploading documents", e);
        }

        return documentPaths;
    }

    public static Map<String, FileValue> processDocuments(Object docsObject) {
        Map<String, FileValue> fileMap = new HashMap<>();

        if (docsObject == null) {
            log.warn("No documents provided in 'docs' variable");
            return fileMap;
        }

        try {
            List<Map<String, Object>> docsList = convertToDocsList(docsObject);

            for (Map<String, Object> doc : docsList) {
                String filename = doc.get("filename").toString();
                String mimetype = doc.get("mimetype").toString();
                String encoding = doc.get("encoding").toString();
                String base64Content = doc.get("content").toString();

                byte[] fileContent = Base64.getDecoder().decode(base64Content);

                FileValue fileValue = Variables.fileValue(filename)
                        .file(fileContent)
                        .mimeType(mimetype)
                        .encoding(encoding)
                        .create();

                fileMap.put(filename, fileValue);
                log.debug("Processed document: {} (size: {} bytes)", filename, fileContent.length);
            }

            log.info("Successfully processed {} documents", fileMap.size());

        } catch (Exception e) {
            log.error("Error processing documents", e);
        }

        return fileMap;
    }

    public static FileValue downloadDocumentAsFileValue(
            String filename, String storagePath, String tenantId) throws Exception {

        StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);

        byte[] content = storage.downloadDocument(storagePath).readAllBytes();

        String mimeType = guessMimeType(filename);

        return Variables.fileValue(filename)
                .file(content)
                .mimeType(mimeType)
                .encoding("UTF-8")
                .create();
    }

    private static String guessMimeType(String filename) {
        String lower = filename.toLowerCase();
        if (lower.endsWith(".pdf")) return "application/pdf";
        if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
        if (lower.endsWith(".png")) return "image/png";
        if (lower.endsWith(".doc")) return "application/msword";
        if (lower.endsWith(".docx")) return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
        return "application/octet-stream";
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> convertToDocsList(Object docsObject) {
        if (docsObject instanceof List) {
            return (List<Map<String, Object>>) docsObject;
        } else if (docsObject instanceof String) {
            JSONArray jsonArray = new JSONArray((String) docsObject);
            List<Map<String, Object>> result = new ArrayList<>();
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonDoc = jsonArray.getJSONObject(i);
                Map<String, Object> docMap = new HashMap<>();
                docMap.put("filename", jsonDoc.getString("filename"));
                docMap.put("mimetype", jsonDoc.getString("mimetype"));
                docMap.put("encoding", jsonDoc.getString("encoding"));
                docMap.put("content", jsonDoc.getString("content"));
                result.add(docMap);
            }
            return result;
        }
        return new ArrayList<>();
    }

    public static Map<String, Map<String, Object>> initializeFileProcessMap(Set<String> filenames) {
        Map<String, Map<String, Object>> fileProcessMap = new HashMap<>();
        for (String filename : filenames) {
            fileProcessMap.put(filename, new HashMap<>());
        }
        log.debug("Initialized file process map for {} files", filenames.size());
        return fileProcessMap;
    }

    public static String getDocumentAsBase64(String filename,
                                             Map<String, String> documentPaths,
                                             String tenantId) throws Exception {
        String storagePath = documentPaths.get(filename);
        if (storagePath == null) {
            throw new IllegalArgumentException("Document not found: " + filename);
        }

        StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);
        byte[] content = storage.downloadDocument(storagePath).readAllBytes();

        return Base64.getEncoder().encodeToString(content);
    }
}