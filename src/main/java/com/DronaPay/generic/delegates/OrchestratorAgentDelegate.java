package com.DronaPay.generic.delegates;

import com.DronaPay.generic.services.ObjectStorageService;
import com.DronaPay.generic.storage.StorageProvider;
import com.DronaPay.generic.utils.TenantPropertiesUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.cibseven.bpm.engine.delegate.BpmnError;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.JavaDelegate;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * OrchestratorAgentDelegate — Generic Agent execution delegate for the Orchestrator loop.
 *
 * Unlike GenericAgentDelegate, this delegate reads ALL configuration from process
 * variables set by OrchestratorDelegate — not from camunda:field bindings. This means
 * the BPMN Generic Agent Task needs zero inputs configured in Camunda Modeler.
 *
 * Process variables consumed (set by OrchestratorDelegate):
 *   - currentAgentId     : String  — agent ID to call (e.g. "execution-mode-agent")
 *   - currentStepConfig  : String  — full step JSON including outputMapping array
 *
 * Other process variables read:
 *   - TicketID           : used to build MinIO paths
 *
 * Process variables set (read by OrchestratorDelegate on next run):
 *   - lastAgentResult    : String  — full raw agent JSON response (always set)
 *
 * Additional process variables set per outputMapping entries with type="var":
 *   - agentAction, agentStatus, agentQuestions, agentMissingFields, agentConfidence
 *
 * MinIO files written per outputMapping entries with type="minio":
 *   - {tenantId}/RuleCreation/{ticketId}/{Stage Folder}/{key}_run{n}.json
 *
 * Stage folder is derived from stageType in currentStepConfig ("execution-mode" → "Execution Mode").
 * Run counter n starts at 1 and increments each time this delegate executes for the same agent.
 *
 * DIA call payload format:
 *   POST {agent.api.url}/agent
 *   { "agentid": "<currentAgentId>", "data": { "context": <context.json content> } }
 *
 * context.json is fetched fresh from MinIO before every call — it is always the
 * latest state written by OrchestratorDelegate.
 */
@Slf4j
public class OrchestratorAgentDelegate implements JavaDelegate {

    private static final String WORKFLOW_KEY  = "RuleCreation";
    private static final String CONTEXT_FILE  = "context.json";
    private static final String DIA_PROP_KEY  = "agent.api.url";
    private static final String DIA_ROUTE     = "/agent";

    @Override
    public void execute(DelegateExecution execution) throws Exception {

        String tenantId   = execution.getTenantId();
        String ticketId   = String.valueOf(execution.getVariable("TicketID"));
        String agentId    = getString(execution, "currentAgentId");
        String stepCfgStr = getString(execution, "currentStepConfig");

        log.info("=== OrchestratorAgentDelegate | agentId={} | TicketID={} ===",
                agentId, ticketId);

        if (agentId == null || agentId.isEmpty()) {
            throw new BpmnError("AGENT_CONFIG_ERROR",
                    "OrchestratorAgentDelegate: currentAgentId process variable is null or empty.");
        }
        if (stepCfgStr == null || stepCfgStr.isEmpty()) {
            throw new BpmnError("AGENT_CONFIG_ERROR",
                    "OrchestratorAgentDelegate: currentStepConfig process variable is null or empty.");
        }

        JSONObject stepConfig   = new JSONObject(stepCfgStr);
        Properties props        = TenantPropertiesUtil.getTenantProps(tenantId);
        StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);

        // ── 1. Derive stage folder and run counter ────────────────────────────
        // Stage folder: "execution-mode" → "Execution Mode", used as MinIO subfolder.
        // Run counter: tracks how many times this agent has been called in this instance.
        String stageType   = stepConfig.optString("stageType", "unknown");
        String stageFolder = toDisplayName(stageType);
        int    runCount    = incrementRunCount(execution, agentId);
        log.info("Stage folder='{}' | run={}", stageFolder, runCount);

        // ── 2. Fetch latest context.json from MinIO ───────────────────────────
        String contextPath = tenantId + "/" + WORKFLOW_KEY + "/" + ticketId + "/" + CONTEXT_FILE;
        String contextJson;
        try (InputStream is = storage.downloadDocument(contextPath)) {
            contextJson = IOUtils.toString(is, StandardCharsets.UTF_8);
        }
        log.debug("context.json fetched ({} chars)", contextJson.length());

        // ── 3. Build DIA request payload ──────────────────────────────────────
        JSONObject context = new JSONObject(contextJson);
        JSONObject data    = new JSONObject();
        data.put("context", context);

        JSONObject requestBody = new JSONObject();
        requestBody.put("agentid", agentId);
        requestBody.put("data", data);

        log.info("DIA payload built for agentId={} | context keys={}",
                agentId, context.keySet());

        // ── 4. Call DIA endpoint ──────────────────────────────────────────────
        String baseUrl = props.getProperty(DIA_PROP_KEY);
        if (baseUrl == null || baseUrl.trim().isEmpty()) {
            throw new BpmnError("CONFIG_ERROR",
                    "OrchestratorAgentDelegate: Property '" + DIA_PROP_KEY + "' not found in tenant config.");
        }
        if (baseUrl.endsWith("/")) baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        String fullUrl = baseUrl + DIA_ROUTE;

        String username = props.getProperty("agent.api.username", "");
        String password = props.getProperty("agent.api.password", "");

        log.info("Calling DIA: POST {}", fullUrl);

        String responseBody;
        try {
            responseBody = callDia(fullUrl, requestBody.toString(), username, password);
        } catch (Exception e) {
            log.error("DIA call failed for agentId={}: {}", agentId, e.getMessage());
            // Set a synthetic FAIL response so OrchestratorDelegate can handle it
            JSONObject failResult = new JSONObject();
            failResult.put("action",  "FAIL");
            failResult.put("status",  "ERROR");
            failResult.put("message", e.getMessage());
            execution.setVariable("lastAgentResult", failResult.toString());
            return;
        }

        log.info("DIA response received for agentId={} ({} chars)", agentId, responseBody.length());

        // ── 5. Set lastAgentResult (always — OrchestratorDelegate reads this) ─
        execution.setVariable("lastAgentResult", responseBody);

        // ── 6. Process outputMapping ──────────────────────────────────────────
        // outputMapping is a JSON array in currentStepConfig:
        //   [{"type":"var",   "key":"agentAction",                    "jsonPath":"$.action"},
        //    {"type":"minio", "key":"execution-mode-agent_output",    "jsonPath":"$"       }]
        //
        // The type="minio" entry is solely responsible for persisting the agent output
        // file to MinIO. No separate raw-save step is needed.
        String outputMappingStr = stepConfig.optString("outputMapping", "[]");
        JSONArray outputMapping;
        try {
            outputMapping = new JSONArray(outputMappingStr);
        } catch (Exception e) {
            log.warn("outputMapping in currentStepConfig is not a valid JSON array — skipping. Value: {}",
                    outputMappingStr);
            outputMapping = new JSONArray();
        }

        JSONObject agentResponse;
        try {
            agentResponse = new JSONObject(responseBody);
        } catch (Exception e) {
            log.warn("Agent response is not valid JSON — outputMapping skipped. Raw: {}", responseBody);
            return;
        }

        processOutputMapping(outputMapping, agentResponse, execution, storage,
                tenantId, ticketId, stageFolder, runCount);

        log.info("=== OrchestratorAgentDelegate Complete | agentId={} | run={} ===", agentId, runCount);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // OUTPUT MAPPING
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Iterates the outputMapping array from currentStepConfig and processes each entry.
     *
     * type="var"   → extracts value using jsonPath from agent response,
     *                sets as Camunda process variable under "key"
     * type="minio" → extracts value using jsonPath from agent response,
     *                saves as JSON to MinIO at:
     *                {tenantId}/RuleCreation/{ticketId}/{Stage Folder}/{key}_run{n}.json
     */
    private void processOutputMapping(JSONArray outputMapping, JSONObject agentResponse,
                                      DelegateExecution execution, StorageProvider storage,
                                      String tenantId, String ticketId,
                                      String stageFolder, int runCount) {

        for (int i = 0; i < outputMapping.length(); i++) {
            JSONObject entry = outputMapping.optJSONObject(i);
            if (entry == null) continue;

            String type     = entry.optString("type", "var");
            String key      = entry.optString("key", "");
            String jsonPath = entry.optString("jsonPath", "$");

            if (key.isEmpty()) {
                log.warn("outputMapping entry {} has no 'key' — skipping", i);
                continue;
            }

            try {
                Object extracted = extractByJsonPath(agentResponse, jsonPath);

                if ("minio".equalsIgnoreCase(type)) {
                    // Save to MinIO under stage folder with run number
                    String minioPath = tenantId + "/" + WORKFLOW_KEY + "/" + ticketId
                            + "/" + stageFolder + "/" + key + "_run" + runCount + ".json";
                    String content = toJsonString(extracted);
                    storage.uploadDocument(minioPath,
                            content.getBytes(StandardCharsets.UTF_8), "application/json");
                    log.info("outputMapping [minio]: key={} → {}", key, minioPath);

                } else {
                    // type="var" — set as process variable
                    execution.setVariable(key, extracted != null ? extracted.toString() : null);
                    log.info("outputMapping [var]: key={} = {}", key,
                            extracted != null ? extracted.toString() : "null");
                }

            } catch (Exception e) {
                log.warn("outputMapping entry {}: failed to process key='{}' jsonPath='{}' — "
                        + "skipping. Reason: {}", i, key, jsonPath, e.getMessage());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DIA HTTP CALL
    // ═══════════════════════════════════════════════════════════════════════════

    private String callDia(String url, String body, String username, String password)
            throws Exception {

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        try (CloseableHttpClient client = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build()) {

            HttpPost post = new HttpPost(url);
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Accept", "application/json");
            post.setEntity(new StringEntity(body, StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(post)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String responseBody = EntityUtils.toString(response.getEntity(),
                        StandardCharsets.UTF_8);

                log.info("DIA HTTP {}: {} chars", statusCode, responseBody.length());

                if (statusCode < 200 || statusCode >= 300) {
                    throw new RuntimeException(
                            "DIA returned HTTP " + statusCode + ": " + responseBody);
                }
                return responseBody;
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // JSON HELPERS
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Converts a stageType slug into a human-readable folder name by splitting on "-"
     * and capitalising each word.
     * Examples: "execution-mode" → "Execution Mode", "product-class" → "Product Class"
     */
    private String toDisplayName(String stageType) {
        if (stageType == null || stageType.isEmpty()) return "Unknown";
        StringBuilder sb = new StringBuilder();
        for (String part : stageType.split("-")) {
            if (!part.isEmpty()) {
                sb.append(Character.toUpperCase(part.charAt(0)))
                        .append(part.substring(1))
                        .append(" ");
            }
        }
        return sb.toString().trim();
    }

    /**
     * Reads {agentId}_runCount from process variables, increments by 1, saves it back,
     * and returns the new value. Returns 1 on the first call for this agent.
     */
    private int incrementRunCount(DelegateExecution execution, String agentId) {
        String varName  = agentId + "_runCount";
        Object existing = execution.getVariable(varName);
        int newCount    = (existing != null) ? (Integer.parseInt(existing.toString()) + 1) : 1;
        execution.setVariable(varName, newCount);
        return newCount;
    }

    /**
     * Simple jsonPath extraction from a JSONObject.
     * Supports "$" (whole object), "$.fieldName", and "$.nested.field" (one level only).
     * For deeper paths, extend with a full JsonPath library call if needed.
     */
    private Object extractByJsonPath(JSONObject source, String jsonPath) {
        if ("$".equals(jsonPath)) return source;

        String path = jsonPath.startsWith("$.") ? jsonPath.substring(2) : jsonPath;

        // Support one level of nesting: "$.a.b"
        if (path.contains(".")) {
            String[] parts = path.split("\\.", 2);
            Object nested = source.opt(parts[0]);
            if (nested instanceof JSONObject) {
                return ((JSONObject) nested).opt(parts[1]);
            }
            return null;
        }
        return source.opt(path);
    }

    private String toJsonString(Object val) {
        if (val == null) return "null";
        if (val instanceof JSONObject) return ((JSONObject) val).toString(2);
        if (val instanceof JSONArray)  return ((JSONArray)  val).toString(2);
        return val.toString();
    }

    private String getString(DelegateExecution execution, String name) {
        Object val = execution.getVariable(name);
        return val != null ? val.toString() : null;
    }
}