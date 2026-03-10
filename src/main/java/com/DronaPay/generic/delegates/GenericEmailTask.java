package com.DronaPay.generic.delegates;

import com.DronaPay.generic.utils.TenantPropertiesUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.cibseven.bpm.engine.RuntimeService;
import org.cibseven.bpm.engine.delegate.DelegateExecution;
import org.cibseven.bpm.engine.delegate.Expression;
import org.cibseven.bpm.engine.delegate.JavaDelegate;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class GenericEmailTask implements JavaDelegate {

    private Expression templateId;
    private Expression recipientEmail;
    private Expression ccEmails;
    private Expression bccEmails;
    private Expression bodyParams;
    private Expression enableAutoResponse;
    private Expression autoResponseMessage;

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        log.info("=== Generic Email Task Started ===");

        String tenantId = execution.getTenantId();
        String ticketId = String.valueOf(execution.getVariable("TicketID"));
        log.info("TicketID: {}, TenantID: {}", ticketId, tenantId);

        // Load tenant properties
        Properties props = TenantPropertiesUtil.getTenantProps(tenantId);

        // Extract field values
        int templateIdValue = Integer.parseInt(getFieldValue(templateId, execution));
        String recipient = resolveValue(getFieldValue(recipientEmail, execution), execution);
        String cc = getFieldValue(ccEmails, execution);
        String bcc = getFieldValue(bccEmails, execution);
        String bodyParamsJson = getFieldValue(bodyParams, execution);
        boolean autoResponse = Boolean.parseBoolean(getFieldValue(enableAutoResponse, execution));
        String messageCorrelation = getFieldValue(autoResponseMessage, execution);

        log.info("Template ID: {}, Recipient: {}, Auto-Response: {}", templateIdValue, recipient, autoResponse);

        // Build email request
        JSONObject emailRequest = buildEmailRequest(templateIdValue, recipient, cc, bcc, bodyParamsJson, tenantId, execution);

        // Send email
        sendEmail(emailRequest, tenantId, props);

        log.info("Email sent successfully");

        // Handle auto-response
        if (autoResponse && isValidValue(messageCorrelation)) {
            handleAutoResponse(execution, messageCorrelation, tenantId);
        }

        log.info("=== Generic Email Task Completed ===");
    }

    private String getFieldValue(Expression field, DelegateExecution execution) {
        if (field == null) {
            return "";
        }
        Object value = field.getValue(execution);
        return value != null ? value.toString() : "";
    }

    private boolean isValidValue(String value) {
        return value != null
                && !value.trim().isEmpty()
                && !value.equalsIgnoreCase("NA")
                && !value.equals("{}")
                && !value.equals("[]");
    }

    private String resolveValue(String value, DelegateExecution execution) {
        if (!isValidValue(value)) {
            return "";
        }

        // If it's a variable reference like ${varName}, extract the variable
        if (value.startsWith("${") && value.endsWith("}")) {
            String varName = value.substring(2, value.length() - 1);
            Object varValue = execution.getVariable(varName);
            return varValue != null ? varValue.toString() : "";
        }

        return value;
    }

    private JSONObject buildEmailRequest(int templateId, String recipient, String cc, String bcc,
                                         String bodyParamsJson, String tenantId, DelegateExecution execution) {
        JSONObject request = new JSONObject();

        request.put("itenantId", Integer.parseInt(tenantId));
        request.put("templateid", templateId);

        // Recipients
        JSONArray toEmails = new JSONArray();
        if (!recipient.isEmpty()) {
            toEmails.put(recipient);
        }
        request.put("toEmail", toEmails);

        // CC
        JSONArray ccEmails = new JSONArray();
        if (isValidValue(cc)) {
            String resolvedCc = resolveValue(cc, execution);
            if (!resolvedCc.isEmpty()) {
                for (String email : resolvedCc.split(",")) {
                    String trimmed = email.trim();
                    if (!trimmed.isEmpty()) {
                        ccEmails.put(trimmed);
                    }
                }
            }
        }
        request.put("ccEmail", ccEmails);

        // BCC
        JSONArray bccEmails = new JSONArray();
        if (isValidValue(bcc)) {
            String resolvedBcc = resolveValue(bcc, execution);
            if (!resolvedBcc.isEmpty()) {
                for (String email : resolvedBcc.split(",")) {
                    String trimmed = email.trim();
                    if (!trimmed.isEmpty()) {
                        bccEmails.put(trimmed);
                    }
                }
            }
        }
        request.put("bccEmail", bccEmails);

        // Body parameters
        JSONObject bodyParamsObj = parseBodyParams(bodyParamsJson, execution);
        request.put("bodyParams", bodyParamsObj);

        log.debug("Email request: {}", request.toString());

        return request;
    }

    private JSONObject parseBodyParams(String bodyParamsJson, DelegateExecution execution) {
        JSONObject params = new JSONObject();

        if (!isValidValue(bodyParamsJson) || bodyParamsJson.equals("{}")) {
            return params;
        }

        try {
            JSONObject rawParams = new JSONObject(bodyParamsJson);

            for (String key : rawParams.keySet()) {
                String value = rawParams.getString(key);

                // Check if it's a variable reference like ${varName}
                if (value.startsWith("${") && value.endsWith("}")) {
                    String varName = value.substring(2, value.length() - 1);
                    Object varValue = execution.getVariable(varName);

                    if (varValue instanceof List) {
                        // Special handling: List variable → formatted HTML bullet list
                        log.info("Variable '{}' is a List — formatting as HTML list", varName);
                        params.put(key, formatListVariable((List<?>) varValue));
                    } else {
                        // Normal variable — just toString()
                        String resolvedValue = varValue != null ? varValue.toString() : "";
                        params.put(key, resolvedValue);
                    }
                } else {
                    // Plain string value, no variable resolution needed
                    params.put(key, value);
                }
            }
        } catch (Exception e) {
            log.error("Error parsing body params: {}", bodyParamsJson, e);
        }

        return params;
    }

    /**
     * Converts a List of missing document objects into an HTML <ul> list string.
     *
     * Handles two formats:
     *   1. List<Map> — e.g. [{missing_document: "medical_bill"}, ...] (from application/json deserialization)
     *   2. List<Object> — plain toString() fallback for any other format
     *
     * Output example:
     *   <ul><li>Medical Bill</li><li>Claim Form</li></ul>
     */
    private String formatListVariable(List<?> list) {
        if (list == null || list.isEmpty()) {
            log.warn("formatListVariable called with null or empty list");
            return "<ul><li>No documents listed</li></ul>";
        }

        StringBuilder html = new StringBuilder("<ul>");
        for (Object item : list) {
            String docName;
            if (item instanceof Map) {
                // application/json deserialization gives LinkedHashMap
                Object val = ((Map<?, ?>) item).get("missing_document");
                docName = val != null ? val.toString() : item.toString();
            } else {
                docName = item.toString();
            }
            // Convert snake_case/underscore to Title Case for readability
            String displayName = toTitleCase(docName.replace("_", " "));
            html.append("<li>").append(displayName).append("</li>");
        }
        html.append("</ul>");

        log.info("Formatted list variable to HTML: {}", html);
        return html.toString();
    }

    /**
     * Converts a space-separated string to Title Case.
     * e.g. "medical bill" → "Medical Bill"
     */
    private String toTitleCase(String input) {
        if (input == null || input.isEmpty()) return input;
        String[] words = input.split(" ");
        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            if (!word.isEmpty()) {
                sb.append(Character.toUpperCase(word.charAt(0)))
                        .append(word.substring(1).toLowerCase())
                        .append(" ");
            }
        }
        return sb.toString().trim();
    }

    private void sendEmail(JSONObject emailRequest, String tenantId, Properties props) throws Exception {
        boolean emailEnabled = Boolean.parseBoolean(props.getProperty("email.enable", "false"));

        if (!emailEnabled) {
            log.info("Email sending disabled - skipping actual send");
            return;
        }

        String emailProvider = props.getProperty("email.provider", "api");

        // Add SMTP config if provider is smtp
        if ("smtp".equals(emailProvider)) {
            JSONObject smtpProps = new JSONObject();
            smtpProps.put("mail.smtp.host", props.getProperty("mail.smtp.host"));
            smtpProps.put("mail.smtp.port", props.getProperty("mail.smtp.port"));
            smtpProps.put("mail.username", props.getProperty("mail.username"));
            smtpProps.put("mail.password", props.getProperty("mail.password"));
            smtpProps.put("mail.sender", props.getProperty("mail.sender"));
            smtpProps.put("mail.smtp.auth", props.getProperty("mail.smtp.auth"));
            smtpProps.put("mail.smtp.connectiontimeout", props.getProperty("mail.smtp.connectiontimeout"));
            smtpProps.put("mail.smtp.timeout", props.getProperty("mail.smtp.timeout"));
            smtpProps.put("mail.smtp.writetimeout", props.getProperty("mail.smtp.writetimeout"));
            smtpProps.put("mail.smtp.starttls.enable", props.getProperty("mail.smtp.starttls.enable"));

            emailRequest.put("emailProvider", "smtp");
            emailRequest.put("providerProperties", smtpProps);
        }

        String uiserverUrl = props.getProperty("uiserver.url");
        String apiKey = props.getProperty("uiserver.apikey");
        String authType = props.getProperty("uiserver.auth.type", "apikey");

        String endpoint = uiserverUrl + "/api/v1/testing/email-service/send-email/tenant-id/" + tenantId;

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(endpoint);
            post.setHeader("Content-Type", "application/json");

            if ("apikey".equalsIgnoreCase(authType)) {
                post.setHeader("X-API-Key", apiKey);
            }

            post.setEntity(new StringEntity(emailRequest.toString(), StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(post)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String responseBody = response.getEntity() != null ?
                        EntityUtils.toString(response.getEntity()) : "";

                log.info("Email API response - Status: {}, Body: {}", statusCode, responseBody);

                if (statusCode != 200) {
                    log.error("Email sending failed with status: {}", statusCode);
                }
            }
        }
    }

    private void handleAutoResponse(DelegateExecution execution, String messageName, String tenantId) {
        Connection conn = null;

        try {
            conn = execution.getProcessEngine()
                    .getProcessEngineConfiguration()
                    .getDataSource()
                    .getConnection();

            RuntimeService runtimeService = execution.getProcessEngineServices().getRuntimeService();
            String businessKey = execution.getBusinessKey();
            String workflowKey = (String) execution.getVariable("workflowKey");
            if (workflowKey == null) {
                workflowKey = "HealthClaim";
            }

            Connection finalConn = conn;
            String finalWorkflowKey = workflowKey;

            new Thread(() -> {
                try {
                    Thread.sleep(60 * 1000);

                    PreparedStatement stmt = finalConn.prepareStatement(
                            "SELECT activeflag FROM ui.templateresponse WHERE templatename = ?");
                    stmt.setString(1, finalWorkflowKey);

                    ResultSet rs = stmt.executeQuery();
                    String enabled = "N";
                    if (rs.next()) {
                        enabled = rs.getString("activeflag");
                    }

                    rs.close();
                    stmt.close();
                    finalConn.close();

                    if ("Y".equals(enabled)) {
                        log.info("Auto-response triggered - correlating message: {}", messageName);
                        runtimeService.createMessageCorrelation(messageName)
                                .processInstanceBusinessKey(businessKey)
                                .correlate();
                        log.info("Message correlated successfully");
                    } else {
                        log.info("Auto-response disabled in database");
                    }
                } catch (Exception e) {
                    log.error("Error in auto-response handler", e);
                }
            }).start();

            log.info("Auto-response handler thread spawned for message: {}", messageName);

        } catch (Exception e) {
            log.error("Error setting up auto-response", e);
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ex) {
                    log.error("Error closing connection", ex);
                }
            }
        }
    }
}