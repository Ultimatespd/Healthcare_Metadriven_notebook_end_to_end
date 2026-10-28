-- 1. Source: stream of landed ingestion rows (append-only)
CREATE OR REFRESH STREAMING TABLE src_{target} AS
SELECT {cols},
       row_hash AS hashkey,
       current_timestamp() AS {seq_col}
FROM STREAM({src});

-- 2. SCD target + AUTO CDC flow (new version only when the hash changes)
CREATE OR REFRESH STREAMING TABLE scd_{target};

CREATE FLOW scd_{target}_flow AS AUTO CDC INTO scd_{target}
FROM STREAM(src_{target})
KEYS ({keys})
SEQUENCE BY {seq_col}
COLUMNS * EXCEPT ({seq_col})
STORED AS SCD TYPE {scd_type}
{track};

-- 3. Serving view with the uniform audit columns
CREATE OR REFRESH MATERIALIZED VIEW {target} AS
SELECT {cols},
       hashkey,
       __START_AT                                          AS add_date,
       COALESCE(__END_AT, TIMESTAMP'2099-12-31 00:00:00')  AS delete_date,
       __START_AT                                          AS last_modified_date,
       CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END        AS is_active,
       CASE WHEN ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY __START_AT DESC) = 1
            THEN 1 ELSE 0 END                              AS is_incremental
FROM scd_{target};
