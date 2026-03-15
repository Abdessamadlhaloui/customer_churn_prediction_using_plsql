CREATE OR REPLACE PROCEDURE run_full_pipeline IS
    l_model_id NUMBER;
    l_f1       NUMBER;
    l_baseline CONSTANT NUMBER := 0.75;
BEGIN
    -- Step 1: Ingest raw data
    pkg_data_ingestion.load_raw_data('EXT_CHURN_DAILY');
    pkg_data_ingestion.validate_and_clean;

    -- Step 2: Feature engineering
    pkg_feature_engineering.build_features(TRUNC(SYSDATE));

    -- Step 3: Train logistic regression model
    pkg_model_train.train_logistic_regression(
        p_learning_rate => 0.01,
        p_iterations    => 300,
        p_as_of_date    => TRUNC(SYSDATE),
        p_model_id      => l_model_id
    );

    -- Step 4: Score all customers
    pkg_model_score.score_all_customers(l_model_id, TRUNC(SYSDATE));

    -- Step 5: Evaluate model
    pkg_evaluation.evaluate_model(l_model_id, TRUNC(SYSDATE));

    -- Step 6: Check F1 threshold and promote or alert
    SELECT NVL(f1_score, 0) INTO l_f1
    FROM training_log
    WHERE model_id = l_model_id
    ORDER BY run_id DESC
    FETCH FIRST 1 ROWS ONLY;

    IF l_f1 >= l_baseline THEN
        -- F1 meets threshold: promote as new champion
        pkg_model_registry.promote_model(l_model_id);
        DBMS_ALERT.SIGNAL('PIPELINE_SUCCESS',
            'Model ' || l_model_id || ' promoted. F1=' || ROUND(l_f1, 4));
        DBMS_OUTPUT.PUT_LINE('[PIPELINE] Model promoted successfully. F1=' ||
                             ROUND(l_f1, 4));
    ELSE
        -- F1 below threshold: send drift warning
        UTL_MAIL.SEND(
            sender     => 'oracle@db.internal',
            recipients => 'ml-team@company.com',
            subject    => '[ALERT] Churn Model Drift Detected',
            message    => 'Model ' || l_model_id ||
                          ' F1=' || ROUND(l_f1, 4) ||
                          ' is below threshold ' || l_baseline ||
                          '. Manual review required.'
        );
        DBMS_OUTPUT.PUT_LINE('[PIPELINE] DRIFT WARNING — F1=' ||
                             ROUND(l_f1, 4) || ' < ' || l_baseline);
    END IF;

    COMMIT;
    pkg_data_ingestion.log_audit('FULL_PIPELINE', 'SUCCESS', 1);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        pkg_data_ingestion.log_audit('FULL_PIPELINE', 'FAILED', 0, SQLERRM);
        RAISE;
END run_full_pipeline;
/

-- ============================================================
-- SECTION 9: DBMS_SCHEDULER Job — Daily at 02:00 AM
-- ============================================================

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'CHURN_PIPELINE_DAILY',
        job_type        => 'STORED_PROCEDURE',
        job_action      => 'RUN_FULL_PIPELINE',
        start_date      => TRUNC(SYSDATE + 1) + INTERVAL '2' HOUR,
        repeat_interval => 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0',
        enabled         => TRUE,
        comments        => 'Daily churn prediction ML pipeline - retrain, score, evaluate'
    );
    DBMS_OUTPUT.PUT_LINE('[SCHEDULER] Job CHURN_PIPELINE_DAILY created.');
EXCEPTION
    WHEN OTHERS THEN
        -- Job may already exist; attempt to drop and recreate
        BEGIN
            DBMS_SCHEDULER.DROP_JOB('CHURN_PIPELINE_DAILY', force => TRUE);
            DBMS_SCHEDULER.CREATE_JOB(
                job_name        => 'CHURN_PIPELINE_DAILY',
                job_type        => 'STORED_PROCEDURE',
                job_action      => 'RUN_FULL_PIPELINE',
                start_date      => TRUNC(SYSDATE + 1) + INTERVAL '2' HOUR,
                repeat_interval => 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0',
                enabled         => TRUE,
                comments        => 'Daily churn prediction ML pipeline'
            );
        EXCEPTION WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[SCHEDULER] Warning: ' || SQLERRM);
        END;
END;
/

-- ============================================================
-- SECTION 10: Monitoring Dashboard View
-- ============================================================

CREATE OR REPLACE VIEW v_pipeline_dashboard AS
SELECT
    mr.model_id,
    mr.name,
    mr.algorithm,
    mr.version,
    mr.status,
    ROUND(NVL(tl.accuracy, 0)    * 100, 2) AS accuracy_pct,
    ROUND(NVL(tl.precision_v, 0) * 100, 2) AS precision_pct,
    ROUND(NVL(tl.recall_v, 0)    * 100, 2) AS recall_pct,
    ROUND(NVL(tl.f1_score, 0)    * 100, 2) AS f1_pct,
    ROUND(NVL(tl.loss, 0), 6)               AS final_loss,
    tl.iterations,
    ROUND(EXTRACT(SECOND FROM (tl.end_time - tl.start_time))
        + EXTRACT(MINUTE FROM (tl.end_time - tl.start_time)) * 60, 1) AS training_sec,
    (SELECT COUNT(*) FROM prediction_output po
     WHERE po.model_id = mr.model_id AND po.predicted_label = 'Yes') AS high_risk_count,
    (SELECT COUNT(*) FROM prediction_output po
     WHERE po.model_id = mr.model_id)                                AS total_scored,
    mr.created_date
FROM model_registry mr
LEFT JOIN training_log tl
    ON tl.model_id = mr.model_id
   AND tl.run_id = (SELECT MAX(run_id) FROM training_log
                     WHERE model_id = mr.model_id)
ORDER BY mr.created_date DESC;
/