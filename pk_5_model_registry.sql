CREATE OR REPLACE PACKAGE pkg_model_registry AUTHID CURRENT_USER IS
    PROCEDURE promote_model(p_model_id IN NUMBER);
    PROCEDURE compare_models(p_model_a IN NUMBER, p_model_b IN NUMBER);
    PROCEDURE rollback_model(p_model_id IN NUMBER);
END pkg_model_registry;
/

CREATE OR REPLACE PACKAGE BODY pkg_model_registry IS

    -- Archive current ACTIVE model and promote the new one
    PROCEDURE promote_model(p_model_id IN NUMBER) IS
    BEGIN
        UPDATE model_registry SET status = 'ARCHIVED'
        WHERE status = 'ACTIVE' AND model_id != p_model_id;

        UPDATE model_registry SET status = 'ACTIVE'
        WHERE model_id = p_model_id;

        COMMIT;
        pkg_data_ingestion.log_audit('PROMOTE_MODEL', 'SUCCESS', 1);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            pkg_data_ingestion.log_audit('PROMOTE_MODEL', 'ERROR', 0, SQLERRM);
            RAISE;
    END promote_model;

    -- Side-by-side comparison of two models
    PROCEDURE compare_models(p_model_a IN NUMBER, p_model_b IN NUMBER) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('============================================');
        DBMS_OUTPUT.PUT_LINE('     MODEL COMPARISON REPORT');
        DBMS_OUTPUT.PUT_LINE('============================================');
        FOR r IN (
            SELECT mr.model_id, mr.algorithm, mr.version, mr.status,
                   tl.accuracy, tl.precision_v, tl.recall_v,
                   tl.f1_score, tl.loss, tl.iterations
            FROM model_registry mr
            JOIN training_log tl ON tl.model_id = mr.model_id
                AND tl.run_id = (SELECT MAX(run_id) FROM training_log
                                 WHERE model_id = mr.model_id)
            WHERE mr.model_id IN (p_model_a, p_model_b)
            ORDER BY tl.f1_score DESC NULLS LAST
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(
                'Model #' || r.model_id || ' [' || r.algorithm || ' v' || r.version || '] ' ||
                'Status=' || r.status);
            DBMS_OUTPUT.PUT_LINE(
                '  Accuracy=' || ROUND(NVL(r.accuracy,0)*100,2) || '%' ||
                '  Precision=' || ROUND(NVL(r.precision_v,0)*100,2) || '%' ||
                '  Recall=' || ROUND(NVL(r.recall_v,0)*100,2) || '%');
            DBMS_OUTPUT.PUT_LINE(
                '  F1=' || ROUND(NVL(r.f1_score,0)*100,2) || '%' ||
                '  Loss=' || ROUND(NVL(r.loss,0),6) ||
                '  Iterations=' || r.iterations);
            DBMS_OUTPUT.PUT_LINE('--------------------------------------------');
        END LOOP;
    END compare_models;

    -- Rollback: reactivate previous ARCHIVED version of same algorithm
    PROCEDURE rollback_model(p_model_id IN NUMBER) IS
        l_prev NUMBER;
    BEGIN
        SELECT MAX(model_id) INTO l_prev
        FROM model_registry
        WHERE status = 'ARCHIVED'
          AND algorithm = (SELECT algorithm FROM model_registry
                           WHERE model_id = p_model_id)
          AND model_id != p_model_id;

        IF l_prev IS NOT NULL THEN
            promote_model(l_prev);
            DBMS_OUTPUT.PUT_LINE('Rolled back to model_id=' || l_prev);
        ELSE
            DBMS_OUTPUT.PUT_LINE('No previous version found for rollback.');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            pkg_data_ingestion.log_audit('ROLLBACK_MODEL', 'ERROR', 0, SQLERRM);
            RAISE;
    END rollback_model;

END pkg_model_registry;
/