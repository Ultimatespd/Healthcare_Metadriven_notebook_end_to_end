-- 1. Source: stream of landed ingestion rows (append-only)
CREATE OR REFRESH STREAMING TABLE src_mx_claim AS
SELECT `claim_id`, `provider_id`, `patient_id`, `date_of_service`, `billed_amount`, `procedure_code`, `diagnosis_code`, `allowed_amount`, `paid_amount`, `insurance_type`, `claim_status`, `reason_code`, `follow-up_required`, `ar_status`, `outcome`, `data_source_id`, `data_source`, `dataset`,
       row_hash AS hashkey,
       current_timestamp() AS last_modified_date
FROM STREAM(devproduct.dev.ingestion_mx_claim);

-- 2. SCD target + AUTO CDC flow (new version only when the hash changes)
CREATE OR REFRESH STREAMING TABLE scd_mx_claim;

CREATE FLOW scd_mx_claim_flow AS AUTO CDC INTO scd_mx_claim
FROM STREAM(src_mx_claim)
KEYS (`claim_id`)
SEQUENCE BY last_modified_date
COLUMNS * EXCEPT (last_modified_date)
STORED AS SCD TYPE 2
TRACK HISTORY ON hashkey;

-- 3. Serving view with the uniform audit columns
CREATE OR REFRESH MATERIALIZED VIEW mx_claim AS
SELECT `claim_id`, `provider_id`, `patient_id`, `date_of_service`, `billed_amount`, `procedure_code`, `diagnosis_code`, `allowed_amount`, `paid_amount`, `insurance_type`, `claim_status`, `reason_code`, `follow-up_required`, `ar_status`, `outcome`, `data_source_id`, `data_source`, `dataset`,
       hashkey,
       __START_AT                                          AS add_date,
       COALESCE(__END_AT, TIMESTAMP'2099-12-31 00:00:00')  AS delete_date,
       __START_AT                                          AS last_modified_date,
       CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END        AS is_active,
       CASE WHEN ROW_NUMBER() OVER (PARTITION BY `claim_id` ORDER BY __START_AT DESC) = 1
            THEN 1 ELSE 0 END                              AS is_incremental
FROM scd_mx_claim;
