-- =====================================================================
-- Add External API Configuration to HealthClaim Workflow
-- Date: 2025-11-28
-- Description: Adds externalAPIs section to filterparams for tenant 1
-- =====================================================================

-- Update Tenant 1 HealthClaim configuration
UPDATE ui.workflowmasters
SET filterparams = filterparams || '{
  "externalAPIs": {
    "springAPI": {
      "baseUrl": "https://main.dev.dronapay.net/springapi",
      "apiKey": "add98899-1729-4e82-beae-25ec43e4128a",
      "endpoints": {
        "score": "/score",
        "accounts": "/accounts/{identifier}"
      }
    },
    "agentAPI": {
      "baseUrl": "https://main.dev.dronapay.net/dia",
      "username": "batchuser",
      "password": "100!batch",
      "endpoints": {
        "agent": "/agent"
      }
    }
  }
}'::jsonb
WHERE workflowkey = 'HealthClaim'
  AND itenantid = 1;

-- Verify the update
SELECT
    workflowkey,
    itenantid,
    filterparams->'externalAPIs' as external_apis
FROM ui.workflowmasters
WHERE workflowkey = 'HealthClaim'
  AND itenantid = 1;