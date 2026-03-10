-- Migration: Add new agents and consolidation configuration
-- Date: 2025-12-08
-- Description: Updates workflow configuration for FHIR consolidation and new agent flow

-- Forward migration
BEGIN;

-- Update HealthClaim workflow configuration with new agents
UPDATE ui.workflowmasters
SET filterparams = jsonb_set(
    filterparams,
    '{agents}',
    '[
        {
            "agentId": "forgeryagent",
            "displayName": "Forgery Detection",
            "order": 1,
            "enabled": true,
            "critical": false,
            "perDocument": true,
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
            "agentId": "Document_Classifier",
            "displayName": "Document Classification",
            "order": 2,
            "enabled": true,
            "critical": false,
            "perDocument": true,
            "config": {
                "endpoint": "${agentApiUrl}/agent",
                "timeout": 30000,
                "inputMapping": {
                    "source": "documentVariable",
                    "transformation": "toBase64"
                },
                "outputMapping": {
                    "variablesToSet": {}
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
            "order": 3,
            "enabled": true,
            "critical": true,
            "perDocument": true,
            "config": {
                "endpoint": "${agentApiUrl}/agent",
                "timeout": 45000,
                "inputMapping": {
                    "source": "documentVariable",
                    "transformation": "toBase64"
                },
                "outputMapping": {
                    "variablesToSet": {}
                },
                "errorHandling": {
                    "onFailure": "throwError",
                    "errorCode": "ocrFailed"
                }
            }
        },
        {
            "agentId": "ocrToFhir",
            "displayName": "OCR to FHIR Conversion",
            "order": 4,
            "enabled": true,
            "critical": true,
            "perDocument": true,
            "config": {
                "endpoint": "${agentApiUrl}/agent",
                "timeout": 60000,
                "inputMapping": {
                    "source": "chainedOutput",
                    "chainFrom": "openaiVision",
                    "transformation": "wrapInOcrText"
                },
                "outputMapping": {
                    "variablesToSet": {}
                },
                "errorHandling": {
                    "onFailure": "throwError",
                    "errorCode": "fhirConversionFailed"
                }
            }
        },
        {
            "agentId": "fhirAnalyser",
            "displayName": "FHIR Analyser",
            "order": 5,
            "enabled": true,
            "critical": false,
            "perDocument": false,
            "requiresConsolidation": true,
            "config": {
                "endpoint": "${agentApiUrl}/agent",
                "timeout": 45000,
                "inputMapping": {
                    "source": "processVariable",
                    "variableName": "consolidatedFhir",
                    "transformation": "none"
                },
                "outputMapping": {
                    "variablesToSet": {
                        "fhirClaimStatus": {
                            "jsonPath": "/claim_status",
                            "dataType": "string"
                        },
                        "fhirClaimSummary": {
                            "jsonPath": "/claim_summary",
                            "dataType": "string"
                        },
                        "fhirRecommendation": {
                            "jsonPath": "/final_recommendation",
                            "dataType": "string"
                        }
                    }
                },
                "errorHandling": {
                    "onFailure": "logAndContinue",
                    "continueOnError": true
                }
            }
        }
    ]'::jsonb
)
WHERE workflowkey = 'HealthClaim';

-- Add consolidation configuration if not exists
UPDATE ui.workflowmasters
SET filterparams = jsonb_set(
    filterparams,
    '{consolidation}',
    '{
        "enabled": true,
        "sourceAgent": "ocrToFhir",
        "outputVariable": "consolidatedFhir",
        "mergeStrategy": "preferNonNull",
        "storageEnabled": true
    }'::jsonb,
    true
)
WHERE workflowkey = 'HealthClaim';

-- Add UI configuration if not exists
UPDATE ui.workflowmasters
SET filterparams = jsonb_set(
    filterparams,
    '{uiConfiguration}',
    '{
        "enabled": true,
        "inputVariable": "consolidatedFhir",
        "outputVariable": "uiFields",
        "fieldMappings": {
            "patientName": "/Claim Form/Insured_Patient_Details/Patient_Name",
            "policyNumber": "/Identity Card/Policy_Number",
            "hospitalName": "/Claim Form/Declaration/Hospital_Name",
            "totalCost": "/Claim Form/Admission_Hospitalization_Details/Sum_total_expected_cost_of_hospitalization"
        }
    }'::jsonb,
    true
)
WHERE workflowkey = 'HealthClaim';

COMMIT;

-- Rollback migration
-- BEGIN;
--
-- -- Restore previous agent configuration (needs actual previous state)
-- UPDATE ui.workflowmasters
-- SET filterparams = jsonb_set(
--     filterparams,
--     '{agents}',
--     '[...]'::jsonb  -- Replace with previous configuration
-- )
-- WHERE workflowkey = 'HealthClaim';
--
-- -- Remove consolidation configuration
-- UPDATE ui.workflowmasters
-- SET filterparams = filterparams - 'consolidation'
-- WHERE workflowkey = 'HealthClaim';
--
-- -- Remove UI configuration
-- UPDATE ui.workflowmasters
-- SET filterparams = filterparams - 'uiConfiguration'
-- WHERE workflowkey = 'HealthClaim';
--
-- COMMIT;