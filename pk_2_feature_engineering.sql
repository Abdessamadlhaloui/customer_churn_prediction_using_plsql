--- create SPECIFICATION 
CREATE OR REPLACE PACKAGE pkg_feature_engineering 
IS 
	PROCEDURE build_features (p_as_of_date DATE DEFAULT SYSDATE) ; 
END pkg_feature_engineering ;


--- create BODY 
CREATE OR REPLACE PACKAGE BODY pkg_feature_engineering 
IS 
	PROCEDURE build_features(p_as_of_date IN DATE DEFAULT SYSDATE) IS
        v_cnt NUMBER;
    BEGIN
        -- Clear previous features for this date to allow re-runs
        DELETE FROM feature_store WHERE as_of_date = TRUNC(p_as_of_date);

        -- Single-pass INSERT using analytic functions + UNION ALL unpivot
        INSERT INTO feature_store (entity_id, feature_name, feature_value, as_of_date)
        WITH stats AS (
            -- Pre-compute population statistics for normalization
            SELECT
                AVG(tenure_months)             AS avg_ten,
                STDDEV(tenure_months)          AS std_ten,
                MIN(monthly_charges)           AS min_mc,
                MAX(monthly_charges)           AS max_mc
            FROM staging_churn
        ),
        base AS (
            SELECT
                s.customer_id,
                -- Z-score normalization: (x - μ) / σ
                (s.tenure_months - st.avg_ten)
                    / NULLIF(st.std_ten, 0)                   AS tenure_zscore,
                -- Min-max normalization: (x - min) / (max - min)
                (s.monthly_charges - st.min_mc)
                    / NULLIF(st.max_mc - st.min_mc, 0)        AS charges_minmax,
                -- One-hot encoding of contract type
                CASE WHEN UPPER(s.contract_type) LIKE '%MONTH%'
                     THEN 1 ELSE 0 END                        AS contract_mtm,
                CASE WHEN UPPER(s.contract_type) LIKE '%ONE%'
                     THEN 1 ELSE 0 END                        AS contract_1yr,
                CASE WHEN UPPER(s.contract_type) LIKE '%TWO%'
                     THEN 1 ELSE 0 END                        AS contract_2yr,
                -- ARPU = Average Revenue Per User
                s.total_charges
                    / NULLIF(s.tenure_months, 0)              AS arpu,
                -- Binary service flags
                CASE WHEN UPPER(s.tech_support)    = 'YES'
                     THEN 1 ELSE 0 END                        AS has_tech,
                CASE WHEN UPPER(s.online_security) = 'YES'
                     THEN 1 ELSE 0 END                        AS has_sec,
                -- As-is numeric
                NVL(s.senior_citizen, 0)                      AS senior_citizen,
                -- Target label: churn → 1/0
                CASE WHEN UPPER(s.churn) = 'YES'
                     THEN 1 ELSE 0 END                        AS churn_flag
            FROM staging_churn s
            CROSS JOIN stats st
        )
        -- Unpivot wide → EAV using UNION ALL
        SELECT customer_id, 'TENURE_ZSCORE',     tenure_zscore,     TRUNC(p_as_of_date) FROM base UNION ALL
        SELECT customer_id, 'CHARGES_MINMAX',    charges_minmax,    TRUNC(p_as_of_date) FROM base UNION ALL
        SELECT customer_id, 'CONTRACT_MTM',      contract_mtm,      TRUNC(p_as_of_date) FROM base UNION ALL
        SELECT customer_id, 'CONTRACT_1YR',      contract_1yr,      TRUNC(p_as_of_date) FROM base UNION ALL
        SELECT customer_id, 'CONTRACT_2YR',      contract_2yr,      TRUNC(p_as_of_date) FROM base UNION ALL
        SELECT customer_id, 'ARPU',              arpu,              TRUNC(p_as_of_date) FROM base UNION ALL
        SELECT customer_id, 'HAS_TECH_SUPPORT',  has_tech,          TRUNC(p_as_of_date) FROM base UNION ALL
        SELECT customer_id, 'HAS_SECURITY',      has_sec,           TRUNC(p_as_of_date) FROM base UNION ALL
        SELECT customer_id, 'SENIOR_CITIZEN',    senior_citizen,    TRUNC(p_as_of_date) FROM base UNION ALL
        SELECT customer_id, 'CHURN_FLAG',        churn_flag,        TRUNC(p_as_of_date) FROM base;

        v_cnt := SQL%ROWCOUNT;
        COMMIT;
        pkg_data_ingestion.log_audit('BUILD_FEATURES', 'SUCCESS', v_cnt);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            pkg_data_ingestion.log_audit('BUILD_FEATURES', 'ERROR', 0, SQLERRM);
            RAISE;
    END build_features;

END pkg_feature_engineering;