-- =====================================================
-- Scoring Configuration for HealthClaim Workflow
-- Tenant 0 and Tenant 1
-- =====================================================

-- Step 1: Get the next available workflowid
DO $$
DECLARE
    next_workflow_id INTEGER;
BEGIN
    -- Get max workflowid and add 1, or start at 1 if table is empty
    SELECT COALESCE(MAX(workflowid), 0) + 1 INTO next_workflow_id
    FROM ui.workflowmasters;

    -- Insert Tenant 0 Configuration
    INSERT INTO ui.workflowmasters (
        workflowid,
        workflowkey,
        itenantid,
        filterparams
    )
    VALUES (
        next_workflow_id,
        'HealthClaim',
        0,
        '{
            "scoring": {
                "fwaDecisioning": {
                    "enabled": true,
                    "requestTemplate": {
                        "staticFields": {
                            "org": "EPIFI",
                            "class": "health_claim"
                        },
                        "variableMappings": [
                            {"apiField": "txn.attribs.patient_name", "processVariable": "patientName"},
                            {"apiField": "txn.attribs.gender", "processVariable": "gender"},
                            {"apiField": "txn.attribs.doctor_id", "processVariable": "doctorIdentifier"},
                            {"apiField": "txn.attribs.doctor_name", "processVariable": "doctorName"},
                            {"apiField": "txn.attribs.disease_name", "processVariable": "diseaseName"},
                            {"apiField": "txn.attribs.procedure", "processVariable": "procedure"},
                            {"apiField": "txn.attribs.start_date", "processVariable": "startDate"},
                            {"apiField": "txn.attribs.medication_name", "processVariable": "medicationName"},
                            {"apiField": "txn.attribs.instruction_for_usage", "processVariable": "dosageInstruction"},
                            {"apiField": "txn.attribs.patient_contact", "processVariable": "contact"},
                            {"apiField": "txn.attribs.insurance_company", "processVariable": "insuranceCompany"},
                            {"apiField": "txn.attribs.time_period", "processVariable": "period"},
                            {"apiField": "txn.attribs.hospital_name", "processVariable": "hospitalName"},
                            {"apiField": "txn.attribs.claim_for", "processVariable": "claimFor"},
                            {"apiField": "txn.attribs.claim_amount", "processVariable": "claimAmount"}
                        ]
                    },
                    "responseMapping": {
                        "RiskScore": {
                            "jsonPath": "/score/score",
                            "dataType": "long",
                            "defaultValue": 100
                        }
                    },
                    "errorHandling": {
                        "errorCode": "failedFWA"
                    }
                },
                "claimCostComputation": {
                    "enabled": true,
                    "requestTemplate": {
                        "staticFields": {
                            "org": "EPIFI",
                            "class": "health_claim_cost"
                        },
                        "variableMappings": [
                            {"apiField": "txn.attribs.patient_name", "processVariable": "patientName"},
                            {"apiField": "txn.attribs.gender", "processVariable": "gender"},
                            {"apiField": "txn.attribs.doctor_id", "processVariable": "doctorIdentifier"},
                            {"apiField": "txn.attribs.doctor_name", "processVariable": "doctorName"},
                            {"apiField": "txn.attribs.disease_name", "processVariable": "diseaseName"},
                            {"apiField": "txn.attribs.procedure", "processVariable": "procedure"},
                            {"apiField": "txn.attribs.start_date", "processVariable": "startDate"},
                            {"apiField": "txn.attribs.medication_name", "processVariable": "medicationName"},
                            {"apiField": "txn.attribs.instruction_for_usage", "processVariable": "dosageInstruction"},
                            {"apiField": "txn.attribs.patient_contact", "processVariable": "contact"},
                            {"apiField": "txn.attribs.insurance_company", "processVariable": "insuranceCompany"},
                            {"apiField": "txn.attribs.time_period", "processVariable": "period"},
                            {"apiField": "txn.attribs.hospital_name", "processVariable": "hospitalName"},
                            {"apiField": "txn.attribs.claim_for", "processVariable": "claimFor"},
                            {"apiField": "txn.attribs.claim_amount", "processVariable": "claimAmount"}
                        ]
                    },
                    "responseMapping": {
                        "approvedAmount": {
                            "jsonPath": "/score/decisiondetails/0/approved_amount",
                            "dataType": "long",
                            "defaultValue": 0
                        },
                        "bonusAmount": {
                            "jsonPath": "/score/decisiondetails/0/bonus",
                            "dataType": "long",
                            "defaultValue": 0
                        },
                        "finalAmount": {
                            "jsonPath": "/score/decisiondetails/0/final_amount",
                            "dataType": "long",
                            "defaultValue": 0
                        }
                    },
                    "errorHandling": {
                        "errorCode": "failedClaimCost"
                    }
                }
            }
        }'::jsonb
    );

    -- Insert Tenant 1 Configuration
    INSERT INTO ui.workflowmasters (
        workflowid,
        workflowkey,
        itenantid,
        filterparams
    )
    VALUES (
        next_workflow_id + 1,
        'HealthClaim',
        1,
        '{
            "scoring": {
                "fwaDecisioning": {
                    "enabled": true,
                    "requestTemplate": {
                        "staticFields": {
                            "org": "EPIFI",
                            "class": "health_claim"
                        },
                        "variableMappings": [
                            {"apiField": "txn.attribs.patient_name", "processVariable": "patientName"},
                            {"apiField": "txn.attribs.gender", "processVariable": "gender"},
                            {"apiField": "txn.attribs.doctor_id", "processVariable": "doctorIdentifier"},
                            {"apiField": "txn.attribs.doctor_name", "processVariable": "doctorName"},
                            {"apiField": "txn.attribs.disease_name", "processVariable": "diseaseName"},
                            {"apiField": "txn.attribs.procedure", "processVariable": "procedure"},
                            {"apiField": "txn.attribs.start_date", "processVariable": "startDate"},
                            {"apiField": "txn.attribs.medication_name", "processVariable": "medicationName"},
                            {"apiField": "txn.attribs.instruction_for_usage", "processVariable": "dosageInstruction"},
                            {"apiField": "txn.attribs.patient_contact", "processVariable": "contact"},
                            {"apiField": "txn.attribs.insurance_company", "processVariable": "insuranceCompany"},
                            {"apiField": "txn.attribs.time_period", "processVariable": "period"},
                            {"apiField": "txn.attribs.hospital_name", "processVariable": "hospitalName"},
                            {"apiField": "txn.attribs.claim_for", "processVariable": "claimFor"},
                            {"apiField": "txn.attribs.claim_amount", "processVariable": "claimAmount"}
                        ]
                    },
                    "responseMapping": {
                        "RiskScore": {
                            "jsonPath": "/score/score",
                            "dataType": "long",
                            "defaultValue": 100
                        }
                    },
                    "errorHandling": {
                        "errorCode": "failedFWA"
                    }
                },
                "claimCostComputation": {
                    "enabled": true,
                    "requestTemplate": {
                        "staticFields": {
                            "org": "EPIFI",
                            "class": "health_claim_cost"
                        },
                        "variableMappings": [
                            {"apiField": "txn.attribs.patient_name", "processVariable": "patientName"},
                            {"apiField": "txn.attribs.gender", "processVariable": "gender"},
                            {"apiField": "txn.attribs.doctor_id", "processVariable": "doctorIdentifier"},
                            {"apiField": "txn.attribs.doctor_name", "processVariable": "doctorName"},
                            {"apiField": "txn.attribs.disease_name", "processVariable": "diseaseName"},
                            {"apiField": "txn.attribs.procedure", "processVariable": "procedure"},
                            {"apiField": "txn.attribs.start_date", "processVariable": "startDate"},
                            {"apiField": "txn.attribs.medication_name", "processVariable": "medicationName"},
                            {"apiField": "txn.attribs.instruction_for_usage", "processVariable": "dosageInstruction"},
                            {"apiField": "txn.attribs.patient_contact", "processVariable": "contact"},
                            {"apiField": "txn.attribs.insurance_company", "processVariable": "insuranceCompany"},
                            {"apiField": "txn.attribs.time_period", "processVariable": "period"},
                            {"apiField": "txn.attribs.hospital_name", "processVariable": "hospitalName"},
                            {"apiField": "txn.attribs.claim_for", "processVariable": "claimFor"},
                            {"apiField": "txn.attribs.claim_amount", "processVariable": "claimAmount"}
                        ]
                    },
                    "responseMapping": {
                        "approvedAmount": {
                            "jsonPath": "/score/decisiondetails/0/approved_amount",
                            "dataType": "long",
                            "defaultValue": 0
                        },
                        "bonusAmount": {
                            "jsonPath": "/score/decisiondetails/0/bonus",
                            "dataType": "long",
                            "defaultValue": 0
                        },
                        "finalAmount": {
                            "jsonPath": "/score/decisiondetails/0/final_amount",
                            "dataType": "long",
                            "defaultValue": 0
                        }
                    },
                    "errorHandling": {
                        "errorCode": "failedClaimCost"
                    }
                }
            }
        }'::jsonb
    );

    RAISE NOTICE 'Inserted HealthClaim configurations with workflowid % and %', next_workflow_id, next_workflow_id + 1;
END $$;

-- Verify insertion
SELECT workflowid, workflowkey, itenantid
FROM ui.workflowmasters
WHERE workflowkey = 'HealthClaim'
ORDER BY workflowid;