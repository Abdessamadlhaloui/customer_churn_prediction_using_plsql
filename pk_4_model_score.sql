--- create SPECIFICATION 
CREATE OR REPLACE PACKAGE pkg_model_score 
AUTHID CURRENT_USER 
IS 
	PROCEDURE score_all_customers(
		model_id IN NUMBER , 
		p_as_of_date IN DATE DEFAULT SYSDATE 
	) ;

END pkg_model_score ; 



--- create BODY 
CREATE OR REPLACE PACKAGE BODY pkg_model_score IS

    -- Sigmoid with clamping for scoring
    FUNCTION sigmoid(p_z NUMBER) RETURN NUMBER IS
    BEGIN
        RETURN 1.0 / (1.0 + EXP(-LEAST(GREATEST(p_z, -500), 500)));
    END sigmoid;

    -- Score all customers by computing dot product of features * weights
    -- Uses BULK COLLECT with LIMIT 5000 for batch insert
    PROCEDURE score_all_customers(
        p_model_id   IN NUMBER,
        p_as_of_date IN DATE DEFAULT SYSDATE
    ) IS
        TYPE t_entity IS TABLE OF VARCHAR2(20);
        TYPE t_score  IS TABLE OF NUMBER;
        TYPE t_label  IS TABLE OF VARCHAR2(5);
        l_entities t_entity;
        l_scores   t_score;
        l_labels   t_label;
        l_total    NUMBER := 0;

        -- JOIN feature_store with model_weights in SQL
        -- Compute: SUM(feature_value * weight_value) + MAX(bias) = z
        CURSOR c_scored IS
            SELECT
                fs.entity_id,
                -- Dot product + bias in SQL, then apply sigmoid
                1.0 / (1.0 + EXP(-LEAST(GREATEST(
                    SUM(fs.feature_value * mw.weight_value) + MAX(mw.bias),
                    -500), 500))) AS churn_prob
            FROM feature_store fs
            JOIN model_weights mw
              ON fs.feature_name = mw.feature_name
             AND mw.model_id = p_model_id
            WHERE fs.as_of_date = TRUNC(p_as_of_date)
              AND fs.feature_name != 'CHURN_FLAG'
            GROUP BY fs.entity_id;
    BEGIN
        -- Clear previous predictions for this model
        DELETE FROM prediction_output WHERE model_id = p_model_id;

        OPEN c_scored;
        LOOP
            -- BULK COLLECT with LIMIT 5000 to control memory
            FETCH c_scored BULK COLLECT INTO l_entities, l_scores LIMIT 5000;
            EXIT WHEN l_entities.COUNT = 0;

            -- Apply threshold: >= 0.5 = Yes (churn), else No
            l_labels := t_label();
            l_labels.EXTEND(l_entities.COUNT);
            FOR i IN 1 .. l_entities.COUNT LOOP
                l_labels(i) := CASE WHEN l_scores(i) >= 0.5 THEN 'Yes' ELSE 'No' END;
            END LOOP;

            -- FORALL batch insert into PREDICTION_OUTPUT
            FORALL i IN 1 .. l_entities.COUNT
                INSERT INTO prediction_output
                    (entity_id, model_id, score, predicted_label, scored_at)
                VALUES
                    (l_entities(i), p_model_id,
                     ROUND(l_scores(i), 6), l_labels(i), SYSDATE);

            l_total := l_total + l_entities.COUNT;
            COMMIT;
        END LOOP;
        CLOSE c_scored;

        pkg_data_ingestion.log_audit('SCORE_BATCH', 'SUCCESS', l_total);

    EXCEPTION
        WHEN OTHERS THEN
            IF c_scored%ISOPEN THEN CLOSE c_scored; END IF;
            ROLLBACK;
            pkg_data_ingestion.log_audit('SCORE_BATCH', 'ERROR', 0, SQLERRM);
            RAISE;
    END score_all_customers;

END pkg_model_score;
/