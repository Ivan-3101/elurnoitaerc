package com.DronaPay.generic.services;

import com.DronaPay.generic.storage.StorageProvider;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class AgentResultStorageService {

    public static String storeAgentResult(String tenantId, String ticketId,
                                          String stageName, String filename,
                                          Map<String, Object> result) throws Exception {

        StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);

        String safeFilename = (filename != null && !filename.isEmpty())
                ? filename.replaceAll("[^a-zA-Z0-9.-]", "_")
                : "consolidated";

        String pathPattern = "{tenantId}/HealthClaim/{ticketId}/{stageName}/{filename}.json";
        String storagePath = pathPattern
                .replace("{tenantId}", tenantId)
                .replace("{ticketId}", ticketId)
                .replace("{stageName}", stageName)
                .replace("{filename}", safeFilename);

        JSONObject resultJson = new JSONObject(result);
        byte[] content = resultJson.toString(2).getBytes(StandardCharsets.UTF_8);

        storage.uploadDocument(storagePath, content, "application/json");

        log.info("Stored {} result for {} at: {}", stageName, filename, storagePath);

        return storagePath;
    }

    public static Map<String, Object> retrieveAgentResult(String tenantId, String minioPath) throws Exception {
        StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);

        try {
            byte[] content = storage.downloadDocument(minioPath).readAllBytes();
            String jsonString = new String(content, StandardCharsets.UTF_8);
            JSONObject json = new JSONObject(jsonString);

            Map<String, Object> result = new HashMap<>();

            if (json.has("rawResponse")) {
                result.put("agentId", json.optString("agentId"));
                result.put("statusCode", json.optInt("statusCode"));
                result.put("success", json.optBoolean("success"));
                result.put("apiResponse", json.optString("rawResponse"));
                result.put("timestamp", json.optLong("timestamp"));

                if (json.has("extractedData")) {
                    JSONObject extractedData = json.getJSONObject("extractedData");
                    extractedData.keySet().forEach(key -> result.put(key, extractedData.get(key)));
                }
            } else {
                json.keySet().forEach(key -> result.put(key, json.get(key)));
            }

            log.debug("Retrieved agent result from: {}", minioPath);
            return result;
        } catch (Exception e) {
            log.error("Failed to retrieve agent result from MinIO path: {}", minioPath, e);
            throw new RuntimeException(String.format(
                    "Could not retrieve agent result from MinIO: %s", minioPath), e);
        }
    }

    public static Map<String, Object> retrieveAgentResult(String tenantId, String ticketId,
                                                          String stageName, String filename) throws Exception {

        String safeFilename = (filename != null && !filename.isEmpty())
                ? filename.replaceAll("[^a-zA-Z0-9.-]", "_")
                : "consolidated";

        String path = "{tenantId}/HealthClaim/{ticketId}/{stageName}/{filename}.json"
                .replace("{tenantId}", tenantId)
                .replace("{ticketId}", ticketId)
                .replace("{stageName}", stageName)
                .replace("{filename}", safeFilename);

        return retrieveAgentResult(tenantId, path);
    }

    public static Map<String, Object> buildResultMap(String agentId, int statusCode,
                                                     String rawResponse, Map<String, Object> extractedData) {
        Map<String, Object> result = new HashMap<>();
        result.put("agentId", agentId);
        result.put("statusCode", statusCode);
        result.put("success", statusCode == 200);
        result.put("rawResponse", rawResponse);
        result.put("extractedData", extractedData);
        result.put("timestamp", System.currentTimeMillis());

        return result;
    }
}