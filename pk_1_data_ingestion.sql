CREATE OR REPLACE PACKAGE pk_data_ingestion
AUTHID CURRENT_USER
IS

    PROCEDURE log_audit (
        p_step   IN VARCHAR2,
        p_status IN VARCHAR2,
        p_rows   IN NUMBER,
        p_err    IN CLOB DEFAULT NULL
    );

    PROCEDURE load_raw_data (
        p_source IN VARCHAR2 DEFAULT 'EXT_CHURN'
    );

    PROCEDURE validate_and_clean;

END pk_data_ingestion;
/

CREATE OR REPLACE PACKAGE BODY pk_data_ingestion IS

    ------------------------------------------------------------------
    -- AUDIT LOGGER
    ------------------------------------------------------------------
    PROCEDURE log_audit(
        p_step   IN VARCHAR2,
        p_status IN VARCHAR2,
        p_rows   IN NUMBER,
        p_err    IN CLOB DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO pipeline_audit
            (step_name, status, rows_processed, error_msg, run_time)
        VALUES
            (p_step, p_status, p_rows, p_err, SYSTIMESTAMP);

        COMMIT;
    END log_audit;


    ------------------------------------------------------------------
    -- LOAD RAW DATA
    ------------------------------------------------------------------
    PROCEDURE load_raw_data(
        p_source IN VARCHAR2 DEFAULT 'EXT_CHURN'
    ) IS

        TYPE t_churn_tab IS TABLE OF staging_churn%ROWTYPE;
        l_rows   t_churn_tab;

        l_total  NUMBER := 0;
        l_cur    SYS_REFCURSOR;

    BEGIN

        OPEN l_cur FOR
            'SELECT customer_id,
                    tenure_months,
                    monthly_charges,
                    total_charges,
                    contract_type,
                    payment_method,
                    internet_service,
                    tech_support,
                    online_security,
                    senior_citizen,
                    dependents,
                    churn,
                    TRUNC(SYSDATE) load_date
             FROM ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_source);

        LOOP
            FETCH l_cur BULK COLLECT INTO l_rows LIMIT 1000;
            EXIT WHEN l_rows.COUNT = 0;

            FOR i IN 1 .. l_rows.COUNT LOOP

                -- Data normalization
                l_rows(i).tenure_months :=
                    NVL(l_rows(i).tenure_months, 0);

                l_rows(i).monthly_charges :=
                    NVL(l_rows(i).monthly_charges, 0);

                l_rows(i).total_charges :=
                    NVL(l_rows(i).total_charges,
                        l_rows(i).tenure_months *
                        l_rows(i).monthly_charges);

                l_rows(i).load_date := TRUNC(SYSDATE);

                -- MERGE into staging
                MERGE INTO staging_churn t
                USING (SELECT l_rows(i).customer_id      AS cid,
                              l_rows(i).tenure_months    AS ten,
                              l_rows(i).monthly_charges  AS mc,
                              l_rows(i).total_charges    AS tc,
                              l_rows(i).contract_type    AS ct,
                              l_rows(i).payment_method   AS pm,
                              l_rows(i).internet_service AS isp,
                              l_rows(i).tech_support     AS ts,
                              l_rows(i).online_security  AS os,
                              l_rows(i).senior_citizen   AS sc,
                              l_rows(i).dependents       AS dep,
                              l_rows(i).churn            AS churn,
                              l_rows(i).load_date        AS ld
                       FROM dual) s
                ON (t.customer_id = s.cid
                    AND t.load_date = s.ld)

                WHEN MATCHED THEN
                    UPDATE SET
                        t.tenure_months    = s.ten,
                        t.monthly_charges  = s.mc,
                        t.total_charges    = s.tc,
                        t.contract_type    = s.ct,
                        t.payment_method   = s.pm,
                        t.internet_service = s.isp,
                        t.tech_support     = s.ts,
                        t.online_security  = s.os,
                        t.senior_citizen   = s.sc,
                        t.dependents       = s.dep,
                        t.churn            = s.churn

                WHEN NOT MATCHED THEN
                    INSERT (
                        customer_id,
                        tenure_months,
                        monthly_charges,
                        total_charges,
                        contract_type,
                        payment_method,
                        internet_service,
                        tech_support,
                        online_security,
                        senior_citizen,
                        dependents,
                        churn,
                        load_date
                    )
                    VALUES (
                        s.cid, s.ten, s.mc, s.tc,
                        s.ct, s.pm, s.isp,
                        s.ts, s.os, s.sc,
                        s.dep, s.churn, s.ld
                    );

            END LOOP;

            l_total := l_total + l_rows.COUNT;

        END LOOP;

        CLOSE l_cur;
        COMMIT;

        log_audit('LOAD_RAW_DATA', 'SUCCESS', l_total);

    EXCEPTION
        WHEN OTHERS THEN
            IF l_cur%ISOPEN THEN
                CLOSE l_cur;
            END IF;

            ROLLBACK;
            log_audit('LOAD_RAW_DATA', 'ERROR', 0, SQLERRM);
            RAISE;
    END load_raw_data;


    ------------------------------------------------------------------
    -- VALIDATE AND CLEAN
    ------------------------------------------------------------------
    PROCEDURE validate_and_clean IS

        l_rejected NUMBER := 0;
        l_cleaned  NUMBER := 0;

    BEGIN

        -- Reject null primary keys
        DELETE FROM staging_churn
        WHERE customer_id IS NULL;

        l_rejected := SQL%ROWCOUNT;

        -- Clean data
        UPDATE staging_churn
        SET tenure_months   = NVL(tenure_months, 0),
            monthly_charges = NVL(monthly_charges, 0),
            total_charges   = NVL(total_charges,
                                   NVL(tenure_months,0) *
                                   NVL(monthly_charges,0)),
            senior_citizen  = NVL(senior_citizen, 0),
            contract_type   = NVL(UPPER(TRIM(contract_type)), 'MONTH-TO-MONTH'),
            churn           = NVL(UPPER(TRIM(churn)), 'NO'),
            tech_support    = NVL(UPPER(TRIM(tech_support)), 'NO'),
            online_security = NVL(UPPER(TRIM(online_security)), 'NO'),
            dependents      = NVL(UPPER(TRIM(dependents)), 'NO');

        l_cleaned := SQL%ROWCOUNT;

        -- Reject invalid churn values
        DELETE FROM staging_churn
        WHERE UPPER(TRIM(churn)) NOT IN ('YES','NO');

        l_rejected := l_rejected + SQL%ROWCOUNT;

        COMMIT;

        log_audit(
            'VALIDATE_CLEAN',
            'SUCCESS',
            l_cleaned,
            'Rejected=' || l_rejected
        );

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_audit('VALIDATE_CLEAN', 'ERROR', 0, SQLERRM);
            RAISE;
    END validate_and_clean;

END pk_data_ingestion;
/