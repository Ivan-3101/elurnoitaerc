package com.DronaPay.generic.services;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class ConfigurationService {

    private static final Map<String, JSONObject> configCache = new HashMap<>();

    /**
     * Load workflow configuration from database
     * @param workflowKey - e.g., "HealthClaim", "MotorClaim"
     * @param tenantId - tenant identifier
     * @param connection - database connection
     * @return configuration as JSONObject
     */
    public static JSONObject loadWorkflowConfig(String workflowKey, String tenantId, Connection connection) {
        String cacheKey = workflowKey + "_" + tenantId;

        // Check cache first
        if (configCache.containsKey(cacheKey)) {
            log.debug("Loading config from cache for {}", cacheKey);
            return configCache.get(cacheKey);
        }

        try {
            // Query database for configuration
            String sql = "SELECT filterparams FROM ui.workflowmasters WHERE workflowkey = ? AND itenantid = ?";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, workflowKey);
            stmt.setInt(2, Integer.parseInt(tenantId));

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                String configJson = rs.getString("filterparams");
                JSONObject config = new JSONObject(configJson);
                configCache.put(cacheKey, config);
                log.info("Loaded configuration for workflow: {}, tenant: {}", workflowKey, tenantId);
                return config;
            } else {
                log.warn("No configuration found for workflow: {}, tenant: {}", workflowKey, tenantId);
                return new JSONObject(); // Return empty config
            }
        } catch (Exception e) {
            log.error("Error loading workflow configuration", e);
            return new JSONObject();
        }
    }

    /**
     * Get tenant-specific properties
     */
    public static Properties getTenantProperties(String tenantId) throws Exception {
        Properties props = new Properties();
        props.load(ConfigurationService.class.getClassLoader()
                .getResourceAsStream("application.properties_" + tenantId));
        return props;
    }

    /**
     * Clear configuration cache (useful for hot-reload)
     */
    public static void clearCache() {
        configCache.clear();
        log.info("Configuration cache cleared");
    }
}