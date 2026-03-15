CREATE OR REPLACE PACKAGE pkg_evaluation AUTHID CURRENT_USER IS
    PROCEDURE evaluate_model(
        p_model_id   IN NUMBER,
        p_as_of_date IN DATE DEFAULT SYSDATE);
END pkg_evaluation;
/

CREATE OR REPLACE PACKAGE BODY pkg_evaluation IS

    -- Evaluate model using confusion matrix metrics
    -- Computes: TP, TN, FP, FN, Accuracy, Precision, Recall, F1, RMSE
    PROCEDURE evaluate_model(
        p_model_id   IN NUMBER,
        p_as_of_date IN DATE DEFAULT SYSDATE
    ) IS
        l_tp   NUMBER := 0;
        l_tn   NUMBER := 0;
        l_fp   NUMBER := 0;
        l_fn   NUMBER := 0;
        l_acc  NUMBER;
        l_prec NUMBER;
        l_rec  NUMBER;
        l_f1   NUMBER;
        l_rmse NUMBER;
        l_total NUMBER;
    BEGIN
        -- Build confusion matrix via SUM(CASE WHEN)
        SELECT
            NVL(SUM(CASE WHEN po.predicted_label = 'Yes' AND UPPER(sc.churn) = 'YES'
                         THEN 1 ELSE 0 END), 0),
            NVL(SUM(CASE WHEN po.predicted_label = 'No'  AND UPPER(sc.churn) = 'NO'
                         THEN 1 ELSE 0 END), 0),
            NVL(SUM(CASE WHEN po.predicted_label = 'Yes' AND UPPER(sc.churn) = 'NO'
                         THEN 1 ELSE 0 END), 0),
            NVL(SUM(CASE WHEN po.predicted_label = 'No'  AND UPPER(sc.churn) = 'YES'
                         THEN 1 ELSE 0 END), 0)
        INTO l_tp, l_tn, l_fp, l_fn
        FROM prediction_output po
        JOIN staging_churn sc ON po.entity_id = sc.customer_id
        WHERE po.model_id = p_model_id;

        l_total := l_tp + l_tn + l_fp + l_fn;

        -- Compute metrics using NULLIF to avoid divide-by-zero
        l_acc  := (l_tp + l_tn)     / NULLIF(l_total, 0);
        l_prec := l_tp              / NULLIF(l_tp + l_fp, 0);
        l_rec  := l_tp              / NULLIF(l_tp + l_fn, 0);
        l_f1   := 2 * l_prec * l_rec / NULLIF(l_prec + l_rec, 0);

        -- RMSE: score calibration error
        -- RMSE = SQRT(AVG(POWER(actual - predicted_score, 2)))
        SELECT SQRT(AVG(POWER(
            CASE WHEN UPPER(sc.churn) = 'YES' THEN 1 ELSE 0 END - po.score, 2)))
        INTO l_rmse
        FROM prediction_output po
        JOIN staging_churn sc ON po.entity_id = sc.customer_id
        WHERE po.model_id = p_model_id;

        -- UPDATE TRAINING_LOG with computed metrics
        UPDATE training_log
        SET accuracy    = l_acc,
            precision_v = l_prec,
            recall_v    = l_rec,
            f1_score    = l_f1
        WHERE model_id = p_model_id
          AND run_id = (SELECT MAX(run_id) FROM training_log
                        WHERE model_id = p_model_id);
        COMMIT;

        -- Print full evaluation report via DBMS_OUTPUT
        DBMS_OUTPUT.PUT_LINE('================================================');
        DBMS_OUTPUT.PUT_LINE('  MODEL EVALUATION REPORT — Model #' || p_model_id);
        DBMS_OUTPUT.PUT_LINE('================================================');
        DBMS_OUTPUT.PUT_LINE('Confusion Matrix:');
        DBMS_OUTPUT.PUT_LINE('  TP=' || l_tp || '  TN=' || l_tn ||
                             '  FP=' || l_fp || '  FN=' || l_fn);
        DBMS_OUTPUT.PUT_LINE('  Total=' || l_total);
        DBMS_OUTPUT.PUT_LINE('------------------------------------------------');
        DBMS_OUTPUT.PUT_LINE('  Accuracy  : ' || ROUND(NVL(l_acc,0)  * 100, 2) || '%');
        DBMS_OUTPUT.PUT_LINE('  Precision : ' || ROUND(NVL(l_prec,0) * 100, 2) || '%');
        DBMS_OUTPUT.PUT_LINE('  Recall    : ' || ROUND(NVL(l_rec,0)  * 100, 2) || '%');
        DBMS_OUTPUT.PUT_LINE('  F1 Score  : ' || ROUND(NVL(l_f1,0)   * 100, 2) || '%');
        DBMS_OUTPUT.PUT_LINE('  RMSE      : ' || ROUND(NVL(l_rmse,0), 4));
        DBMS_OUTPUT.PUT_LINE('================================================');

        pkg_data_ingestion.log_audit('EVALUATE_MODEL', 'SUCCESS', l_total);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            pkg_data_ingestion.log_audit('EVALUATE_MODEL', 'ERROR', 0, SQLERRM);
            RAISE;
    END evaluate_model;

END pkg_evaluation;
/