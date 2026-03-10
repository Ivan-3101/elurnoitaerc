package com.DronaPay.generic.delegates;

import com.DronaPay.generic.services.ConfigurationService;
import com.DronaPay.generic.services.DocumentProcessingService;
import com.DronaPay.generic.storage.MinIOStorageProvider;
import com.DronaPay.generic.storage.StorageProvider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.Expression;
import org.cibseven.bpm.engine.delegate.JavaDelegate;
import org.cibseven.bpm.engine.variable.Variables;
import org.cibseven.bpm.engine.variable.value.ObjectValue;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;


@Slf4j
public class GenericTicketIdGenerator implements JavaDelegate {

    // The 'configKey' injected from the Camunda Modeler (e.g., "Generate TicketID and Workflow Name")
    private Expression configKey;

    // Jackson Mapper for handling JSON operations (replacing basic Maps)
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        log.info("=== Generic Ticket Generator Started ===");

        // 1. Resolve the Configuration Key from BPMN
        String configKeyStr = (configKey != null) ? (String) configKey.getValue(execution) : null;
        if (configKeyStr == null || configKeyStr.trim().isEmpty()) {
            throw new RuntimeException("GenericTicketIdGenerator: 'configKey' is missing in BPMN.");
        }

        // 2. Identify Context (Tenant & Workflow)
        String tenantId = execution.getTenantId();
        String workflowKey = execution.getProcessEngineServices().getRepositoryService()
                .getProcessDefinition(execution.getProcessDefinitionId()).getKey();

        log.info("Loading config for Key: '{}', Workflow: '{}', Tenant: '{}'", configKeyStr, workflowKey, tenantId);

        Connection connection = null;
        try {
            // Obtain JDBC connection
            connection = execution.getProcessEngine().getProcessEngineConfiguration().getDataSource().getConnection();

            // 3. Load the Full Workflow Configuration (JSON blob) from Database
            JSONObject fullWorkflowConfig = ConfigurationService.loadWorkflowConfig(workflowKey, tenantId, connection);

            // 4. BOOTSTRAP AGENTS
            // We load the "agents" list into a process variable NOW so downstream
            // multi-instance tasks (Forgery/Classifier) have a list to iterate over.
            loadAgentConfiguration(execution, fullWorkflowConfig);

            // 5. Locate Specific Stage Configuration
            if (!fullWorkflowConfig.has("genericWorkflowDelegateConfigurations") ||
                    !fullWorkflowConfig.getJSONObject("genericWorkflowDelegateConfigurations").has(configKeyStr)) {
                throw new RuntimeException("Configuration not found for key: " + configKeyStr);
            }

            JSONObject stageConfig = fullWorkflowConfig.getJSONObject("genericWorkflowDelegateConfigurations")
                    .getJSONObject(configKeyStr);

            // 6. INITIALIZE ROOT OBJECT (Req 1)
            // Instead of a Map, we create a Jackson ObjectNode. This will hold all our
            // variables (tenantId, TicketID, docs) in a structured JSON format.
            ObjectNode rootObj = mapper.createObjectNode();

            if (stageConfig.has("initialVariablesRootObj")) {
                resolveInitialVariables(execution, stageConfig.getJSONArray("initialVariablesRootObj"), rootObj, tenantId, workflowKey);
            }

            // 7. EXECUTE STEPS
            // Loop through the steps defined in DB and execute them, passing the rootObj.
            if (stageConfig.has("steps")) {
                JSONArray steps = stageConfig.getJSONArray("steps");
                for (int i = 0; i < steps.length(); i++) {
                    executeStep(execution, steps.getJSONObject(i), rootObj, connection, tenantId);
                }
            }

        } finally {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
        log.info("=== Generic Ticket Generator Completed Successfully ===");
    }

    /**
     * Extracts "agents" from the global config and saves it as a JSON Process Variable.
     * Use: Prevents the "Agents Skipped" bug in multi-instance sub-processes.
     */
    private void loadAgentConfiguration(DelegateExecution execution, JSONObject fullWorkflowConfig) {
        if (fullWorkflowConfig.has("agents")) {
            JSONArray agentsArray = fullWorkflowConfig.getJSONArray("agents");
            List<Map<String, Object>> agentList = new ArrayList<>();
            for (int i = 0; i < agentsArray.length(); i++) {
                agentList.add(agentsArray.getJSONObject(i).toMap());
            }
            // Serialize as JSON object so Camunda can handle it in loops
            ObjectValue agentListJson = Variables.objectValue(agentList)
                    .serializationDataFormat(Variables.SerializationDataFormats.JSON)
                    .create();
            execution.setVariable("agentList", agentListJson);
        }
    }

    /**
     * Populates the 'rootObj' with initial data (tenantId, workflowKey, input docs).
     */
    private void resolveInitialVariables(DelegateExecution execution, JSONArray variablesConfig, ObjectNode rootObj, String tenantId, String workflowKey) {
        for (int i = 0; i < variablesConfig.length(); i++) {
            JSONObject varConf = variablesConfig.getJSONObject(i);
            String key = varConf.getString("key");

            if (varConf.has("executionMethod") && varConf.getBoolean("executionMethod")) {
                // Auto-resolve standard metadata directly into the JSON object
                switch (key) {
                    case "tenantId": rootObj.put(key, tenantId); break;
                    case "workflowKey": rootObj.put(key, workflowKey); break;
                    case "processInstanceId": rootObj.put(key, execution.getProcessInstanceId()); break;
                    case "stageName": rootObj.put(key, execution.getCurrentActivityId()); break;
                    default: log.warn("Unknown execution variable: {}", key);
                }
            } else if (varConf.has("processVariable") && varConf.getBoolean("processVariable")) {
                // Fetch variable from process (e.g. docs) and convert to JsonNode
                String sourceVar = varConf.optString("source", key);
                Object val = execution.getVariable(sourceVar);
                rootObj.set(key, mapper.valueToTree(val));
            } else if (varConf.has("staticValue")) {
                rootObj.put(key, String.valueOf(varConf.get("staticValue")));
            }
        }
    }

    /**
     * Router method: Directs execution based on the 'type' field in the step config.
     */
    private void executeStep(DelegateExecution execution, JSONObject step, ObjectNode rootObj, Connection conn, String tenantId) throws Exception {
        String type = step.getString("type");
        log.debug("Executing step: {}", type);

        if ("SqlQueryExecution".equalsIgnoreCase(type)) {
            executeSqlStep(execution, step, rootObj, conn);
        } else if ("UploadToS3".equalsIgnoreCase(type)) {
            executeUploadStep(execution, step, rootObj, tenantId);
        } else if ("SetProcessVariables".equalsIgnoreCase(type)) {
            executeSetVariablesStep(execution, step, rootObj);
        }
    }

    /**
     * Executes SQL queries (INSERT/SELECT) using parameters from rootObj.
     */
    private void executeSqlStep(DelegateExecution execution, JSONObject step, ObjectNode rootObj, Connection conn) throws Exception {
        JSONObject config = step.getJSONObject("config");
        String query = config.getString("query");
        JSONArray params = config.optJSONArray("parameters");

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            // Map parameters: Pull values from rootObj and set in PreparedStatement
            if (params != null) {
                for (int i = 0; i < params.length(); i++) {
                    JSONObject param = params.getJSONObject(i);
                    String key = param.getString("key");

                    JsonNode valNode = rootObj.get(key);
                    Object value = (valNode != null && !valNode.isNull()) ? valNode.asText() : null;

                    stmt.setObject(i + 1, value);
                }
            }

            // Handle SELECT vs UPDATE
            if ("select".equalsIgnoreCase(config.optString("queryType"))) {
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        String returnKey = step.optString("returnValueKey");
                        Object result = rs.getObject(returnKey);

                        // Store result back into rootObj (e.g., the generated TicketID)
                        String contextKey = step.optString("returnValueObjKey");
                        if (!contextKey.isEmpty()) {
                            rootObj.set(contextKey, mapper.valueToTree(result));
                        }

                        // Also set as Process Variable for rest of workflow
                        if (step.has("processVariablesToSetAfterExecution")) {
                            JSONArray varsToSet = step.getJSONArray("processVariablesToSetAfterExecution");
                            for(int j=0; j<varsToSet.length(); j++) {
                                JSONObject v = varsToSet.getJSONObject(j);
                                execution.setVariable(v.getString("key"), result);
                            }
                        }
                    }
                }
            } else {
                stmt.executeUpdate();
            }
        }
    }

    /**
     * Handles file uploads with strict validation and polymorphic input support.
     */
    private void executeUploadStep(DelegateExecution execution, JSONObject step, ObjectNode rootObj, String tenantId) throws Exception {

        // STORAGE TYPE VALIDATION
        // Ensure we only process 'minio' requests for now.
        String storageType = step.optString("storageType");
        if (!"minio".equalsIgnoreCase(storageType)) {
            throw new RuntimeException("GenericTicketIdGenerator: Unsupported or missing 'storageType': " + storageType + ". Only 'minio' is currently supported.");
        }

        // STRICT PATH PATTERN
        String pathPattern = step.optString("pathPattern");
        if (pathPattern == null || pathPattern.trim().isEmpty()) {
            throw new RuntimeException("GenericTicketIdGenerator: 'pathPattern' is missing in JSON config for UploadToS3 step.");
        }

        // CONFIGURABLE FILE SOURCE
        // We look for 'fileObj' key in config (e.g., "docsObj") to find files in rootObj.
        String fileObjKey = step.optString("fileObj");
        if (fileObjKey.isEmpty()) {
            fileObjKey = "docsObj"; // Default fallback
        }

        JsonNode docsNode = rootObj.get(fileObjKey);
        if (docsNode == null || docsNode.isNull()) {
            log.warn("No documents found in rootObj under key: {}", fileObjKey);
            return;
        }

        // PATH RESOLUTION
        // Replace placeholders like {TicketID} with actual values from rootObj
        String resolvedPath = pathPattern;
        Iterator<String> fieldNames = rootObj.fieldNames();
        while(fieldNames.hasNext()) {
            String key = fieldNames.next();
            JsonNode valNode = rootObj.get(key);
            String valStr = (valNode != null && !valNode.isNull()) ? valNode.asText() : "";
            resolvedPath = resolvedPath.replace("{" + key + "}", valStr);
        }

        log.info("Processing upload to path: {}", resolvedPath);

        Properties props = ConfigurationService.getTenantProperties(tenantId);
        StorageProvider storage = new MinIOStorageProvider(props);
        Map<String, String> documentPaths = new HashMap<>();

        try {
            // 5. POLYMORPHIC INPUT HANDLING
            // Handle List (Array) vs Map (Object) inputs automatically
            if (docsNode.isArray()) {
                for (JsonNode doc : docsNode) {
                    processSingleJsonNode(doc, resolvedPath, storage, documentPaths);
                }
            } else if (docsNode.isObject()) {
                if (docsNode.has("filename") && docsNode.has("content")) {
                    processSingleJsonNode(docsNode, resolvedPath, storage, documentPaths);
                } else {
                    // Legacy Map support
                    Iterator<Map.Entry<String, JsonNode>> fields = docsNode.fields();
                    while(fields.hasNext()) {
                        Map.Entry<String, JsonNode> field = fields.next();
                        uploadSingleDoc(field.getKey(), field.getValue().asText(), resolvedPath, storage, documentPaths);
                    }
                }
            }

            // Set outputs for downstream delegates (DocTypeSplitter, Agents)
            execution.setVariable("documentPaths", documentPaths);
            execution.setVariable("attachmentVars", new ArrayList<>(documentPaths.keySet()));
            Map<String, Map<String, Object>> fileProcessMap = DocumentProcessingService.initializeFileProcessMap(documentPaths.keySet());
            execution.setVariable("fileProcessMap", fileProcessMap);

            log.info("Uploaded {} documents.", documentPaths.size());

        } finally {
            if (storage instanceof MinIOStorageProvider) {
                ((MinIOStorageProvider) storage).close();
            }
        }
    }

    private void processSingleJsonNode(JsonNode doc, String basePath, StorageProvider storage, Map<String, String> documentPaths) {
        String name = doc.has("filename") ? doc.get("filename").asText() : null;
        String content = doc.has("content") ? doc.get("content").asText() : null;
        uploadSingleDoc(name, content, basePath, storage, documentPaths);
    }

    private void uploadSingleDoc(String name, String base64Content, String basePath, StorageProvider storage, Map<String, String> documentPaths) {
        try {
            if (name != null && base64Content != null) {
                byte[] content = Base64.getDecoder().decode(base64Content);
                String fullPath = basePath + name;
                storage.uploadDocument(fullPath, content, "application/octet-stream");
                documentPaths.put(name, fullPath);
            }
        } catch (Exception e) {
            log.error("Failed to upload document: {}", name, e);
        }
    }

    private void executeSetVariablesStep(DelegateExecution execution, JSONObject step, ObjectNode rootObj) {
        if (step.has("variables")) {
            JSONArray vars = step.getJSONArray("variables");
            for (int i = 0; i < vars.length(); i++) {
                JSONObject v = vars.getJSONObject(i);
                String varName = v.getString("key");
                Object value;

                if (v.has("staticValue")) {
                    value = v.get("staticValue");
                } else if (v.has("sourcePath")) {
                    String sourceKey = v.getString("sourcePath");
                    JsonNode valNode = rootObj.get(sourceKey);
                    value = (valNode != null && !valNode.isNull()) ? valNode.asText() : null;
                } else {
                    continue;
                }
                execution.setVariable(varName, value);
            }
        }
    }
}