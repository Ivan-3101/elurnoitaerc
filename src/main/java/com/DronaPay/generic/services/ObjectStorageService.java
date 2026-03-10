package com.DronaPay.generic.services;

import com.DronaPay.generic.storage.MinIOStorageProvider;
import com.DronaPay.generic.storage.StorageProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class ObjectStorageService {

    private static final Map<String, StorageProvider> providerCache = new HashMap<>();

    public static StorageProvider getStorageProvider(String tenantId) throws Exception {
        if (providerCache.containsKey(tenantId)) {
            return providerCache.get(tenantId);
        }

        Properties props = ConfigurationService.getTenantProperties(tenantId);
        String providerType = props.getProperty("storage.provider", "minio");

        log.info("Initializing storage provider '{}' for tenant {}", providerType, tenantId);

        StorageProvider provider;
        switch (providerType.toLowerCase()) {
            case "minio":
                provider = new MinIOStorageProvider(props);
                break;
            case "s3":
                throw new UnsupportedOperationException("S3 provider not yet implemented");
            case "azure":
                throw new UnsupportedOperationException("Azure provider not yet implemented");
            default:
                throw new IllegalArgumentException("Unknown storage provider: " + providerType);
        }

        providerCache.put(tenantId, provider);
        return provider;
    }

    public static String buildStoragePath(String pathPattern, String tenantId,
                                          String workflowKey, String ticketId, String stageName, String filename) {
        String path = pathPattern
                .replace("{tenantId}", tenantId)
                .replace("{workflowKey}", workflowKey)
                .replace("{ticketId}", String.valueOf(ticketId))
                .replace("{stageName}", stageName);

        return path + filename;
    }

    public static void clearCache() {
        providerCache.clear();
        log.info("Storage provider cache cleared");
    }
}