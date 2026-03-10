-- =====================================================
-- Agent Configuration for HealthClaim Workflow
-- Date: 2025-12-01
-- Description: Adds agent configurations to filterparams
-- =====================================================

-- Update Tenant 1 HealthClaim configuration with agent configs
UPDATE ui.workflowmasters
SET filterparams = filterparams || '{
  "agents": [
    {
      "agentId": "forgeryagent",
      "displayName": "Forgery Detection",
      "order": 1,
      "enabled": true,
      "critical": false,
      "config": {
        "endpoint": "${agentApiUrl}/agent",
        "timeout": 30000,
        "inputMapping": {
          "source": "documentVariable",
          "transformation": "toBase64"
        },
        "outputMapping": {
          "variablesToSet": {
            "isForged": {
              "jsonPath": "/answer",
              "transformation": "mapSuspiciousToBoolean",
              "dataType": "boolean"
            }
          }
        },
        "errorHandling": {
          "onFailure": "logAndContinue",
          "continueOnError": true
        }
      }
    },
    {
      "agentId": "openaiVision",
      "displayName": "OCR Extraction",
      "order": 2,
      "enabled": true,
      "critical": true,
      "config": {
        "endpoint": "${agentApiUrl}/agent",
        "timeout": 45000,
        "inputMapping": {
          "source": "documentVariable",
          "transformation": "toBase64"
        },
        "outputMapping": {
          "variablesToSet": {
            "ocr_text": {
              "jsonPath": "/answer",
              "dataType": "string"
            }
          }
        },
        "errorHandling": {
          "onFailure": "throwError",
          "continueOnError": false,
          "errorCode": "failedOcr"
        }
      }
    },
    {
      "agentId": "ocrToFhir",
      "displayName": "FHIR Conversion",
      "order": 3,
      "enabled": true,
      "critical": true,
      "config": {
        "endpoint": "${agentApiUrl}/agent",
        "timeout": 60000,
        "inputMapping": {
          "source": "processVariable",
          "variableName": "ocr_text"
        },
        "outputMapping": {
          "variablesToSet": {
            "fhir_json": {
              "jsonPath": "/answer",
              "dataType": "json"
            }
          }
        },
        "errorHandling": {
          "onFailure": "throwError",
          "continueOnError": false
        }
      }
    },
    {
      "agentId": "policy_comp",
      "displayName": "Policy Comparator",
      "order": 4,
      "enabled": true,
      "critical": false,
      "config": {
        "endpoint": "${agentApiUrl}/agent",
        "timeout": 30000,
        "inputMapping": {
          "source": "processVariable",
          "variableName": "fhir_json",
          "additionalInputs": {
            "policy_data": "policyDetails",
            "coverage_limits": "coverageAmount"
          }
        },
        "outputMapping": {
          "variablesToSet": {
            "policyMissingInfo": {
              "jsonPath": "/answer/List 2",
              "dataType": "string",
              "defaultValue": "None"
            },
            "policyPotentialIssues": {
              "jsonPath": "/answer/List 2",
              "dataType": "string",
              "defaultValue": "None"
            }
          }
        },
        "errorHandling": {
          "onFailure": "logAndContinue",
          "continueOnError": true
        }
      }
    }
  ]
}'::jsonb
WHERE workflowkey = 'HealthClaim'
  AND itenantid = 1;

-- Verify the update
SELECT
    workflowkey,
    itenantid,
    jsonb_array_length(filterparams->'agents') as agent_count,
    jsonb_pretty(filterparams->'agents') as agents_config
FROM ui.workflowmasters
WHERE workflowkey = 'HealthClaim'
  AND itenantid = 1;