package com.DronaPay.generic.delegates;

import com.DronaPay.generic.services.ObjectStorageService;
import com.DronaPay.generic.storage.StorageProvider;
import com.DronaPay.generic.utils.TenantPropertiesUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.JavaDelegate;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * OrchestratorDelegate — Central state machine for the Rule Creation agent loop.
 *
 * Reads 7 stage configurations from camunda:inputParameter fields injected by the
 * Orchestrator Element Template. Skips any stage where stageNType == "none".
 * Manages context.json in MinIO as the single source of truth for session state.
 *
 * Entry conditions:
 *   - First run    : currentStepIndex is null (not yet set as process variable)
 *   - From Agent   : currentStepIndex is set, userAnswers is null
 *   - From UserTask: currentStepIndex is set, userAnswers is non-null
 *
 * Sets orchestratorAction to one of: CONTINUE | RETRY_STEP | GO_USER_TASK | EXIT
 * GW1 routes on this variable.
 */
@Slf4j
public class OrchestratorDelegate implements JavaDelegate {

    private static final String WORKFLOW_KEY   = "RuleCreation";
    private static final String CONTEXT_FILE   = "context.json";
    private static final int    MAX_STAGE_SLOTS = 7;

    @Override
    public void execute(DelegateExecution execution) throws Exception {

        String tenantId  = execution.getTenantId();
        String ticketId  = String.valueOf(execution.getVariable("TicketID"));
        String userInput = (String) execution.getVariable("userInput");

        log.info("=== OrchestratorDelegate | TicketID: {} | Tenant: {} ===", ticketId, tenantId);

        StorageProvider storage = ObjectStorageService.getStorageProvider(tenantId);
        String contextPath = buildContextPath(tenantId, ticketId);

        Integer currentStepIndex = toInteger(execution.getVariable("currentStepIndex"));

        // ═══════════════════════════════════════════════════════════════════════
        // FIRST RUN — initialise everything
        // ═══════════════════════════════════════════════════════════════════════
        if (currentStepIndex == null) {
            log.info("First run — initialising orchestrator state");

            List<JSONObject> steps = buildSteps(execution);
            if (steps.isEmpty()) {
                throw new RuntimeException(
                        "OrchestratorDelegate: No stages configured — all stageNType values are 'none'.");
            }

            int totalSteps = steps.size();

            // Persist context.json to MinIO
            JSONObject context = new JSONObject();
            context.put("ticketId",          ticketId);
            context.put("workflowKey",       WORKFLOW_KEY);
            context.put("tenantId",          tenantId);
            context.put("userInput",         userInput != null ? userInput : "");
            context.put("currentStepIndex",  0);
            context.put("totalSteps",        totalSteps);
            context.put("workflowStatus",    "RUNNING");
            context.put("extracted_values",  new JSONObject());
            context.put("interaction_history", new JSONArray());

            uploadContext(storage, contextPath, context);

            // Store serialised steps as process variable so subsequent runs
            // don't need to rely on camunda:inputParameter still being in scope
            execution.setVariable("stepsConfig",      stepsToJsonArray(steps).toString());
            execution.setVariable("currentStepIndex", 0);
            execution.setVariable("retryCount",       0);
            execution.setVariable("totalSteps",       totalSteps);
            execution.setVariable("workflowStatus",   "RUNNING");

            setCurrentStepVars(execution, steps.get(0), 0);
            execution.setVariable("orchestratorAction", "CONTINUE");

            log.info("Init complete. totalSteps={} | Stage 0 | agentId={} | action=CONTINUE",
                    totalSteps, steps.get(0).optString("agentId"));
            return;
        }

        // ═══════════════════════════════════════════════════════════════════════
        // SUBSEQUENT RUNS — load state from process variables
        // ═══════════════════════════════════════════════════════════════════════
        List<JSONObject> steps = loadSteps(execution);
        int totalSteps = steps.size();
        int retryCount = toIntVar(execution, "retryCount", 0);

        JSONObject context = downloadContext(storage, contextPath);

        // ── PATH A: Returning from User Task ─────────────────────────────────
        Object userAnswersObj = execution.getVariable("userAnswers");
        if (userAnswersObj != null && !userAnswersObj.toString().trim().isEmpty()) {

            log.info("Returning from User Task | stage={}", currentStepIndex);

            JSONObject userAnswers;
            try {
                userAnswers = new JSONObject(userAnswersObj.toString());
            } catch (Exception e) {
                log.warn("Could not parse userAnswers as JSON, storing as-is: {}", userAnswersObj);
                userAnswers = new JSONObject();
                userAnswers.put("rawAnswer", userAnswersObj.toString());
            }

            // Append user interaction into interaction_history
            JSONArray history = context.optJSONArray("interaction_history");
            if (history == null) history = new JSONArray();

            JSONObject entry = new JSONObject();
            entry.put("stage",          currentStepIndex);
            entry.put("agentId",        execution.getVariable("currentAgentId"));
            entry.put("from",           "USER_TASK");
            entry.put("questions",      toJsonArray(execution.getVariable("agentQuestions")));
            entry.put("missing_fields", toJsonArray(execution.getVariable("agentMissingFields")));
            entry.put("userAnswers",    userAnswers);
            history.put(entry);

            context.put("interaction_history", history);
            uploadContext(storage, contextPath, context);

            // Clear userAnswers so this branch is not re-entered
            execution.setVariable("userAnswers", null);

            execution.setVariable("orchestratorAction", "RETRY_STEP");
            execution.setVariable("workflowStatus",     "RUNNING");

            log.info("User Task return processed | action=RETRY_STEP | stage={}", currentStepIndex);
            return;
        }

        // ── PATH B: Returning from Agent ──────────────────────────────────────
        Object lastAgentResultObj = execution.getVariable("lastAgentResult");

        if (lastAgentResultObj == null) {
            // Should not happen in normal flow — defensive guard
            log.warn("lastAgentResult is null — defaulting to RETRY_STEP");
            execution.setVariable("orchestratorAction", "RETRY_STEP");
            return;
        }

        JSONObject lastAgentResult;
        try {
            lastAgentResult = new JSONObject(lastAgentResultObj.toString());
        } catch (Exception e) {
            log.error("Could not parse lastAgentResult as JSON: {} — forcing EXIT", lastAgentResultObj);
            execution.setVariable("orchestratorAction", "EXIT");
            execution.setVariable("workflowStatus",     "ERROR");
            return;
        }

        String agentAction = lastAgentResult.optString("action", "FAIL").toUpperCase();
        log.info("Returning from Agent | agentAction={} | stage={}", agentAction, currentStepIndex);

        // Always merge updated_context regardless of action taken
        mergeUpdatedContext(context, lastAgentResult);

        // Append agent interaction to history
        JSONArray history = context.optJSONArray("interaction_history");
        if (history == null) history = new JSONArray();
        JSONObject agentEntry = new JSONObject();
        agentEntry.put("stage",    currentStepIndex);
        agentEntry.put("agentId",  execution.getVariable("currentAgentId"));
        agentEntry.put("from",     "AGENT");
        agentEntry.put("action",   agentAction);
        agentEntry.put("status",   lastAgentResult.optString("status", ""));
        agentEntry.put("confidence", lastAgentResult.opt("confidence"));
        history.put(agentEntry);
        context.put("interaction_history", history);

        JSONObject currentStep = steps.get(currentStepIndex);
        String  onFailure  = currentStep.optString("onFailure", "END");
        int     maxRetries = currentStep.optInt("maxRetries", 0);

        switch (agentAction) {

            // ── NEXT_STEP ────────────────────────────────────────────────────
            case "NEXT_STEP": {
                int nextIndex = currentStepIndex + 1;
                context.put("currentStepIndex", nextIndex);
                execution.setVariable("retryCount", 0);

                if (nextIndex >= totalSteps) {
                    context.put("workflowStatus", "DONE");   // ← add this
                    uploadContext(storage, contextPath, context);  // ← move after status set
                    execution.setVariable("orchestratorAction", "EXIT");
                    execution.setVariable("workflowStatus", "DONE");
                } else {
                    context.put("workflowStatus", "RUNNING");  // ← add this
                    uploadContext(storage, contextPath, context);  // ← move after status set
                    execution.setVariable("currentStepIndex", nextIndex);
                    setCurrentStepVars(execution, steps.get(nextIndex), nextIndex);
                    execution.setVariable("orchestratorAction", "CONTINUE");
                    execution.setVariable("workflowStatus", "RUNNING");
                }
                break;
            }

            // ── GO_USER_TASK ─────────────────────────────────────────────────
            case "GO_USER_TASK": {
                uploadContext(storage, contextPath, context);
                execution.setVariable("orchestratorAction", "GO_USER_TASK");
                execution.setVariable("workflowStatus",     "AWAITING_USER");
                log.info("Agent requested user input | action=GO_USER_TASK | stage={}", currentStepIndex);
                break;
            }

            // ── RETRY_STEP (agent-requested) ─────────────────────────────────
            case "RETRY_STEP": {
                int newRetryCount = retryCount + 1;
                execution.setVariable("retryCount", newRetryCount);
                uploadContext(storage, contextPath, context);

                if (newRetryCount > maxRetries) {
                    log.warn("Max retries ({}) exceeded at stage {} | on_failure={}",
                            maxRetries, currentStepIndex, onFailure);
                    applyOnFailure(execution, context, storage, contextPath,
                            onFailure, currentStepIndex, steps);
                } else {
                    execution.setVariable("orchestratorAction", "RETRY_STEP");
                    execution.setVariable("workflowStatus",     "RUNNING");
                    log.info("Agent retry | retryCount={}/{} | action=RETRY_STEP | stage={}",
                            newRetryCount, maxRetries, currentStepIndex);
                }
                break;
            }

            // ── END ──────────────────────────────────────────────────────────
            case "END": {
                uploadContext(storage, contextPath, context);
                execution.setVariable("orchestratorAction", "EXIT");
                execution.setVariable("workflowStatus",     "DONE");
                log.info("Agent signalled END | action=EXIT");
                break;
            }

            // ── FAIL ─────────────────────────────────────────────────────────
            case "FAIL": {
                log.warn("Agent signalled FAIL at stage {} | on_failure={}", currentStepIndex, onFailure);
                uploadContext(storage, contextPath, context);
                applyOnFailure(execution, context, storage, contextPath,
                        onFailure, currentStepIndex, steps);
                break;
            }

            default: {
                log.error("Unknown agentAction '{}' at stage {} — forcing EXIT", agentAction, currentStepIndex);
                uploadContext(storage, contextPath, context);
                execution.setVariable("orchestratorAction", "EXIT");
                execution.setVariable("workflowStatus",     "ERROR");
                break;
            }
        }

        log.info("=== OrchestratorDelegate Complete | action={} ===",
                execution.getVariable("orchestratorAction"));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // PRIVATE HELPERS
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Builds the steps list from camunda:inputParameter values injected into
     * this task's execution scope. Called only on first run.
     */
    private List<JSONObject> buildSteps(DelegateExecution execution) {
        List<JSONObject> steps = new ArrayList<>();
        for (int i = 1; i <= MAX_STAGE_SLOTS; i++) {
            String type = getStringVar(execution, "stage" + i + "Type");
            if (type == null || type.equalsIgnoreCase("none")) continue;

            JSONObject step = new JSONObject();
            step.put("stageType",       type);
            step.put("agentId",         getStringVar(execution, "stage" + i + "AgentId"));
            step.put("allowUserInput",  getStringVar(execution, "stage" + i + "AllowUserInput"));
            step.put("maxRetries",      parseIntSafe(getStringVar(execution, "stage" + i + "MaxRetries")));
            step.put("onFailure",       getStringVar(execution, "stage" + i + "OnFailure"));
            step.put("outputMapping",   getStringVar(execution, "stage" + i + "OutputMapping"));
            steps.add(step);

            log.debug("Stage slot {} loaded: type={} agentId={}", i, type, step.optString("agentId"));
        }
        log.info("Built {} active stage(s) from template config", steps.size());
        return steps;
    }

    /**
     * Loads steps from the stepsConfig process variable (set on first run).
     * Used on all subsequent runs to avoid relying on camunda:inputParameter scope.
     */
    private List<JSONObject> loadSteps(DelegateExecution execution) {
        List<JSONObject> steps = new ArrayList<>();
        String stepsConfigStr = getStringVar(execution, "stepsConfig");
        if (stepsConfigStr == null || stepsConfigStr.isEmpty()) {
            log.warn("stepsConfig process variable not found — rebuilding from input params");
            return buildSteps(execution);
        }
        JSONArray arr = new JSONArray(stepsConfigStr);
        for (int i = 0; i < arr.length(); i++) {
            steps.add(arr.getJSONObject(i));
        }
        return steps;
    }

    /**
     * Sets the three process variables that the Generic Agent Task reads:
     *   currentAgentId     — the agentId string for this stage
     *   currentStepConfig  — full step JSON (includes outputMapping for GenericAgentDelegate)
     *   currentStepIndex   — current position in the steps array
     */
    private void setCurrentStepVars(DelegateExecution execution, JSONObject step, int index) {
        execution.setVariable("currentAgentId",    step.optString("agentId", "unknown-agent"));
        execution.setVariable("currentStepConfig", step.toString());
        execution.setVariable("currentStepIndex",  index);
    }

    /**
     * Applies the on_failure strategy when max retries are exceeded or agent signals FAIL.
     * RETRY  → keeps looping (RETRY_STEP)
     * END    → exits workflow (EXIT)
     */
    private void applyOnFailure(DelegateExecution execution, JSONObject context,
                                StorageProvider storage, String contextPath,
                                String onFailure, int currentStepIndex,
                                List<JSONObject> steps) throws Exception {
        if ("RETRY".equalsIgnoreCase(onFailure)) {
            execution.setVariable("orchestratorAction", "RETRY_STEP");
            execution.setVariable("workflowStatus",     "RUNNING");
            log.info("on_failure=RETRY — RETRY_STEP at stage {}", currentStepIndex);
        } else {
            // Default: END
            execution.setVariable("orchestratorAction", "EXIT");
            execution.setVariable("workflowStatus",     "DONE");
            log.info("on_failure=END — EXIT at stage {}", currentStepIndex);
        }
    }

    /**
     * Merges the agent's updated_context into context.json extracted_values.
     * Called on every agent return regardless of agentAction.
     */
    private void mergeUpdatedContext(JSONObject context, JSONObject agentResult) {
        try {
            JSONObject updatedContext = agentResult.optJSONObject("updated_context");
            if (updatedContext == null || updatedContext.isEmpty()) return;

            JSONObject extracted = context.optJSONObject("extracted_values");
            if (extracted == null) extracted = new JSONObject();

            for (String key : updatedContext.keySet()) {
                extracted.put(key, updatedContext.get(key));
            }
            context.put("extracted_values", extracted);
            log.debug("Merged {} updated_context key(s): {}", updatedContext.length(), updatedContext.keySet());
        } catch (Exception e) {
            log.warn("Failed to merge updated_context — skipping. Reason: {}", e.getMessage());
        }
    }

    private JSONObject downloadContext(StorageProvider storage, String path) throws Exception {
        try (InputStream is = storage.downloadDocument(path)) {
            String json = IOUtils.toString(is, StandardCharsets.UTF_8);
            return new JSONObject(json);
        }
    }

    private void uploadContext(StorageProvider storage, String path, JSONObject context) throws Exception {
        byte[] bytes = context.toString(2).getBytes(StandardCharsets.UTF_8);
        storage.uploadDocument(path, bytes, "application/json");
        log.debug("context.json uploaded to {}", path);
    }

    private String buildContextPath(String tenantId, String ticketId) {
        return tenantId + "/" + WORKFLOW_KEY + "/" + ticketId + "/" + CONTEXT_FILE;
    }

    private JSONArray stepsToJsonArray(List<JSONObject> steps) {
        JSONArray arr = new JSONArray();
        steps.forEach(arr::put);
        return arr;
    }

    private String getStringVar(DelegateExecution execution, String name) {
        Object val = execution.getVariable(name);
        return val != null ? val.toString() : null;
    }

    private Integer toInteger(Object val) {
        if (val == null) return null;
        try {
            return Integer.parseInt(val.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private int toIntVar(DelegateExecution execution, String name, int defaultValue) {
        Object val = execution.getVariable(name);
        if (val == null) return defaultValue;
        try {
            return Integer.parseInt(val.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private int parseIntSafe(String value) {
        if (value == null || value.equalsIgnoreCase("NA") || value.trim().isEmpty()) return 0;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Safely converts a process variable (String, JSONArray, List, or null) to JSONArray.
     * Used when reading agentQuestions / agentMissingFields from the User Task branch.
     */
    private JSONArray toJsonArray(Object val) {
        if (val == null) return new JSONArray();
        try {
            return new JSONArray(val.toString());
        } catch (Exception e) {
            return new JSONArray();
        }
    }
}