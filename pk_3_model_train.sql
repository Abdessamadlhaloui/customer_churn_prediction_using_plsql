--- create SPECIFICATION 
CREATE OR REPLACE PACKAGE pkg_model_train 
AUTHID CURRENT_USER 
IS 

	PROCEDURE train_logistic_regression (
				learning_rate IN NUMBER DEFAULT 0.01 , 
				number_iterations IN NUMBER DEFAULT 500 , 
				p_as_of_date IN DATE DEFAULT SYSDATE , 
				p_model_id OUT NUMBER 
				) ; 
	
	PROCEDURE train_naive_bayes (
				p_as_of_date IN DATE DEFAULT SYSDATE , 
				p_model_id OUT NUMBER 
				) ; 
	
	PROCEDURE train_kmeans(
				p_k IN NUMBER DEFAULT 3 , 
				p_max_iteration IN NUMBER DEFAULT 500 , 
				p_as_of_date IN DATE DEFAULT SYSDATE , 
				p_model_id OUT NUMBER
				) ; 

END pkg_model_train ;



--- create BODY 
CREATE OR REPLACE PACKAGE BODY pkg_model_train IS

    -- Sigmoid with clamping to prevent EXP overflow: sigma(z)=1/(1+e^-z)
    FUNCTION sigmoid(p_z IN NUMBER) RETURN NUMBER IS
    BEGIN
        RETURN 1.0 / (1.0 + EXP(-LEAST(GREATEST(p_z, -500), 500)));
    END sigmoid;

    -- (a) LOGISTIC REGRESSION via full batch gradient descent
    -- Forward: z=w*x+b, p=sigma(z)
    -- Loss: L=-1/n SUM[y*ln(p)+(1-y)*ln(1-p)]
    -- Gradient: dw_j=1/n SUM(p-y)*x_j, db=1/n SUM(p-y)
    -- Update: w -= lr*dw, b -= lr*db
    PROCEDURE train_logistic_regression(
        p_learning_rate IN NUMBER DEFAULT 0.01, p_iterations IN NUMBER DEFAULT 500,
        p_as_of_date IN DATE DEFAULT SYSDATE, p_model_id OUT NUMBER
    ) IS
        TYPE t_feat_list IS TABLE OF VARCHAR2(50);
        l_feat t_feat_list := t_feat_list(
            'TENURE_ZSCORE','CHARGES_MINMAX','CONTRACT_MTM','CONTRACT_1YR',
            'CONTRACT_2YR','ARPU','HAS_TECH_SUPPORT','HAS_SECURITY','SENIOR_CITIZEN');
        c_nf CONSTANT PLS_INTEGER := 9;
        TYPE t_wt IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
        TYPE t_nt IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
        TYPE t_mx IS TABLE OF t_nt INDEX BY PLS_INTEGER;
        l_w t_wt; l_g t_wt; l_bias NUMBER := 0; l_gb NUMBER;
        l_X t_mx; l_y t_nt; l_n PLS_INTEGER; l_z NUMBER; l_p NUMBER; l_e NUMBER;
        l_loss NUMBER; l_start TIMESTAMP := SYSTIMESTAMP;
        CURSOR c_d IS
            SELECT entity_id,
                MAX(CASE WHEN feature_name='TENURE_ZSCORE' THEN feature_value END) f1,
                MAX(CASE WHEN feature_name='CHARGES_MINMAX' THEN feature_value END) f2,
                MAX(CASE WHEN feature_name='CONTRACT_MTM' THEN feature_value END) f3,
                MAX(CASE WHEN feature_name='CONTRACT_1YR' THEN feature_value END) f4,
                MAX(CASE WHEN feature_name='CONTRACT_2YR' THEN feature_value END) f5,
                MAX(CASE WHEN feature_name='ARPU' THEN feature_value END) f6,
                MAX(CASE WHEN feature_name='HAS_TECH_SUPPORT' THEN feature_value END) f7,
                MAX(CASE WHEN feature_name='HAS_SECURITY' THEN feature_value END) f8,
                MAX(CASE WHEN feature_name='SENIOR_CITIZEN' THEN feature_value END) f9,
                MAX(CASE WHEN feature_name='CHURN_FLAG' THEN feature_value END) yv
            FROM feature_store WHERE as_of_date=TRUNC(p_as_of_date) GROUP BY entity_id;
        lr c_d%ROWTYPE; l_i PLS_INTEGER;
    BEGIN
        INSERT INTO model_registry(name,version,algorithm,parameters)
        VALUES('CHURN_LOGREG','1.0','LOGISTIC_REGRESSION',
               '{"lr":'||p_learning_rate||',"iter":'||p_iterations||'}')
        RETURNING model_id INTO p_model_id;
        FOR j IN 1..c_nf LOOP l_w(j):=0; l_g(j):=0; END LOOP;
        l_i:=0;
        OPEN c_d;
        LOOP FETCH c_d INTO lr; EXIT WHEN c_d%NOTFOUND; l_i:=l_i+1;
            l_X(l_i)(1):=NVL(lr.f1,0); l_X(l_i)(2):=NVL(lr.f2,0);
            l_X(l_i)(3):=NVL(lr.f3,0); l_X(l_i)(4):=NVL(lr.f4,0);
            l_X(l_i)(5):=NVL(lr.f5,0); l_X(l_i)(6):=NVL(lr.f6,0);
            l_X(l_i)(7):=NVL(lr.f7,0); l_X(l_i)(8):=NVL(lr.f8,0);
            l_X(l_i)(9):=NVL(lr.f9,0); l_y(l_i):=NVL(lr.yv,0);
        END LOOP; CLOSE c_d; l_n:=l_i;
        -- Gradient descent loop
        FOR epoch IN 1..p_iterations LOOP
            l_loss:=0; l_gb:=0;
            FOR j IN 1..c_nf LOOP l_g(j):=0; END LOOP;
            FOR i IN 1..l_n LOOP
                l_z:=l_bias;
                FOR j IN 1..c_nf LOOP l_z:=l_z+l_w(j)*l_X(i)(j); END LOOP;
                l_p:=sigmoid(l_z); l_e:=l_p-l_y(i);
                l_gb:=l_gb+l_e;
                FOR j IN 1..c_nf LOOP l_g(j):=l_g(j)+l_e*l_X(i)(j); END LOOP;
                l_loss:=l_loss-(l_y(i)*LN(GREATEST(l_p,1E-10))
                    +(1-l_y(i))*LN(GREATEST(1-l_p,1E-10)));
            END LOOP;
            l_bias:=l_bias-p_learning_rate*(l_gb/NULLIF(l_n,0));
            FOR j IN 1..c_nf LOOP
                l_w(j):=l_w(j)-p_learning_rate*(l_g(j)/NULLIF(l_n,0));
            END LOOP;
        END LOOP;
        FOR j IN 1..c_nf LOOP
            INSERT INTO model_weights(model_id,feature_name,weight_value,bias)
            VALUES(p_model_id,l_feat(j),l_w(j),l_bias);
        END LOOP;
        INSERT INTO training_log(model_id,start_time,end_time,loss,iterations)
        VALUES(p_model_id,l_start,SYSTIMESTAMP,l_loss/NULLIF(l_n,0),p_iterations);
        UPDATE model_registry SET status='TRAINED' WHERE model_id=p_model_id;
        COMMIT;
        pkg_data_ingestion.log_audit('TRAIN_LOGREG','SUCCESS',l_n);
    EXCEPTION WHEN OTHERS THEN
        ROLLBACK; pkg_data_ingestion.log_audit('TRAIN_LOGREG','ERROR',0,SQLERRM); RAISE;
    END train_logistic_regression;

    -- (b) GAUSSIAN NAIVE BAYES
    -- P(C|x) proportional to P(C)*PRODUCT(P(x_j|C))
    -- P(x_j|C=c) = Gaussian(mean_jc, std_jc)
    -- Stored: feature_name suffix _C0/_C1, bias=mean, weight_value=stddev
    PROCEDURE train_naive_bayes(
        p_as_of_date IN DATE DEFAULT SYSDATE, p_model_id OUT NUMBER
    ) IS
        l_start TIMESTAMP := SYSTIMESTAMP; l_cnt NUMBER;
    BEGIN
        INSERT INTO model_registry(name,version,algorithm,parameters)
        VALUES('CHURN_NB','1.0','NAIVE_BAYES','{"type":"gaussian"}')
        RETURNING model_id INTO p_model_id;
        INSERT INTO model_weights(model_id,feature_name,weight_value,bias)
        WITH cs AS (
            SELECT fs.feature_name,
                CASE WHEN UPPER(s.churn)='YES' THEN 1 ELSE 0 END AS cl,
                AVG(fs.feature_value) AS fm,
                NVL(NULLIF(STDDEV(fs.feature_value),0),0.0001) AS fs2
            FROM feature_store fs JOIN staging_churn s ON fs.entity_id=s.customer_id
            WHERE fs.as_of_date=TRUNC(p_as_of_date) AND fs.feature_name!='CHURN_FLAG'
            GROUP BY fs.feature_name, CASE WHEN UPPER(s.churn)='YES' THEN 1 ELSE 0 END
        )
        SELECT p_model_id, feature_name||'_C'||cl, fs2, fm FROM cs;
        l_cnt := SQL%ROWCOUNT;
        INSERT INTO model_weights(model_id,feature_name,weight_value,bias)
        SELECT p_model_id,'PRIOR_C'||CASE WHEN UPPER(churn)='YES' THEN 1 ELSE 0 END,
            COUNT(*)/NULLIF((SELECT COUNT(*) FROM staging_churn),0), 0
        FROM staging_churn GROUP BY CASE WHEN UPPER(churn)='YES' THEN 1 ELSE 0 END;
        INSERT INTO training_log(model_id,start_time,end_time,iterations)
        VALUES(p_model_id,l_start,SYSTIMESTAMP,1);
        UPDATE model_registry SET status='TRAINED' WHERE model_id=p_model_id;
        COMMIT;
        pkg_data_ingestion.log_audit('TRAIN_NB','SUCCESS',l_cnt);
    EXCEPTION WHEN OTHERS THEN
        ROLLBACK; pkg_data_ingestion.log_audit('TRAIN_NB','ERROR',0,SQLERRM); RAISE;
    END train_naive_bayes;

    -- (c) K-MEANS with K-Means++ seeding
    -- 1. Random first centroid, then D(x)^2 proportional selection
    -- 2. Iterative assign + centroid recalc
    -- Centroids stored: feature_name=feat, weight_value=component, bias=cluster_id
    PROCEDURE train_kmeans(
        p_k IN NUMBER DEFAULT 3, p_max_iter IN NUMBER DEFAULT 100,
        p_as_of_date IN DATE DEFAULT SYSDATE, p_model_id OUT NUMBER
    ) IS
        c_nf CONSTANT PLS_INTEGER := 9;
        TYPE t_fl IS TABLE OF VARCHAR2(50);
        l_feat t_fl := t_fl('TENURE_ZSCORE','CHARGES_MINMAX','CONTRACT_MTM',
            'CONTRACT_1YR','CONTRACT_2YR','ARPU','HAS_TECH_SUPPORT',
            'HAS_SECURITY','SENIOR_CITIZEN');
        TYPE t_nt IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
        TYPE t_mx IS TABLE OF t_nt INDEX BY PLS_INTEGER;
        l_X t_mx; l_n PLS_INTEGER; l_C t_mx; l_Cn t_mx; l_cc t_nt;
        l_asgn t_nt; l_dist NUMBER; l_md NUMBER; l_bk PLS_INTEGER;
        l_chg BOOLEAN; l_start TIMESTAMP := SYSTIMESTAMP;
        l_si PLS_INTEGER; l_td NUMBER; l_rv NUMBER; l_cm NUMBER; l_d2 t_nt;
        CURSOR c_d IS
            SELECT entity_id,
                MAX(CASE WHEN feature_name='TENURE_ZSCORE' THEN feature_value END) f1,
                MAX(CASE WHEN feature_name='CHARGES_MINMAX' THEN feature_value END) f2,
                MAX(CASE WHEN feature_name='CONTRACT_MTM' THEN feature_value END) f3,
                MAX(CASE WHEN feature_name='CONTRACT_1YR' THEN feature_value END) f4,
                MAX(CASE WHEN feature_name='CONTRACT_2YR' THEN feature_value END) f5,
                MAX(CASE WHEN feature_name='ARPU' THEN feature_value END) f6,
                MAX(CASE WHEN feature_name='HAS_TECH_SUPPORT' THEN feature_value END) f7,
                MAX(CASE WHEN feature_name='HAS_SECURITY' THEN feature_value END) f8,
                MAX(CASE WHEN feature_name='SENIOR_CITIZEN' THEN feature_value END) f9
            FROM feature_store WHERE as_of_date=TRUNC(p_as_of_date)
                AND feature_name!='CHURN_FLAG' GROUP BY entity_id;
        lr c_d%ROWTYPE; l_i PLS_INTEGER;
    BEGIN
        INSERT INTO model_registry(name,version,algorithm,parameters)
        VALUES('CHURN_KMEANS','1.0','KMEANS',
               '{"k":'||p_k||',"max_iter":'||p_max_iter||'}')
        RETURNING model_id INTO p_model_id;
        l_i:=0; OPEN c_d;
        LOOP FETCH c_d INTO lr; EXIT WHEN c_d%NOTFOUND; l_i:=l_i+1;
            l_X(l_i)(1):=NVL(lr.f1,0); l_X(l_i)(2):=NVL(lr.f2,0);
            l_X(l_i)(3):=NVL(lr.f3,0); l_X(l_i)(4):=NVL(lr.f4,0);
            l_X(l_i)(5):=NVL(lr.f5,0); l_X(l_i)(6):=NVL(lr.f6,0);
            l_X(l_i)(7):=NVL(lr.f7,0); l_X(l_i)(8):=NVL(lr.f8,0);
            l_X(l_i)(9):=NVL(lr.f9,0); l_asgn(l_i):=0;
        END LOOP; CLOSE c_d; l_n:=l_i;
        -- K-Means++ seed: first centroid random
        l_si:=MOD(ABS(DBMS_RANDOM.RANDOM),l_n)+1;
        FOR j IN 1..c_nf LOOP l_C(1)(j):=l_X(l_si)(j); END LOOP;
        -- Remaining centroids proportional to D(x)^2
        FOR c IN 2..p_k LOOP
            l_td:=0;
            FOR i IN 1..l_n LOOP
                l_md:=1E30;
                FOR cc IN 1..c-1 LOOP
                    l_dist:=0;
                    FOR j IN 1..c_nf LOOP
                        l_dist:=l_dist+POWER(l_X(i)(j)-l_C(cc)(j),2);
                    END LOOP;
                    IF l_dist<l_md THEN l_md:=l_dist; END IF;
                END LOOP;
                l_d2(i):=l_md; l_td:=l_td+l_md;
            END LOOP;
            l_rv:=DBMS_RANDOM.VALUE(0,l_td); l_cm:=0; l_si:=l_n;
            FOR i IN 1..l_n LOOP
                l_cm:=l_cm+l_d2(i);
                IF l_cm>=l_rv THEN l_si:=i; EXIT; END IF;
            END LOOP;
            FOR j IN 1..c_nf LOOP l_C(c)(j):=l_X(l_si)(j); END LOOP;
        END LOOP;
        -- Iterative assignment + centroid recalculation
        FOR iter IN 1..p_max_iter LOOP
            l_chg:=FALSE;
            FOR i IN 1..l_n LOOP
                l_md:=1E30; l_bk:=1;
                FOR c IN 1..p_k LOOP
                    l_dist:=0;
                    FOR j IN 1..c_nf LOOP
                        l_dist:=l_dist+POWER(l_X(i)(j)-l_C(c)(j),2);
                    END LOOP;
                    IF l_dist<l_md THEN l_md:=l_dist; l_bk:=c; END IF;
                END LOOP;
                IF l_asgn(i)!=l_bk THEN l_asgn(i):=l_bk; l_chg:=TRUE; END IF;
            END LOOP;
            EXIT WHEN NOT l_chg;
            FOR c IN 1..p_k LOOP l_cc(c):=0;
                FOR j IN 1..c_nf LOOP l_Cn(c)(j):=0; END LOOP;
            END LOOP;
            FOR i IN 1..l_n LOOP
                l_cc(l_asgn(i)):=l_cc(l_asgn(i))+1;
                FOR j IN 1..c_nf LOOP
                    l_Cn(l_asgn(i))(j):=l_Cn(l_asgn(i))(j)+l_X(i)(j);
                END LOOP;
            END LOOP;
            FOR c IN 1..p_k LOOP
                IF l_cc(c)>0 THEN
                    FOR j IN 1..c_nf LOOP l_C(c)(j):=l_Cn(c)(j)/l_cc(c); END LOOP;
                END IF;
            END LOOP;
        END LOOP;
        -- Persist centroids
        FOR c IN 1..p_k LOOP
            FOR j IN 1..c_nf LOOP
                INSERT INTO model_weights(model_id,feature_name,weight_value,bias)
                VALUES(p_model_id,l_feat(j),l_C(c)(j),c-1);
            END LOOP;
        END LOOP;
        INSERT INTO training_log(model_id,start_time,end_time,iterations)
        VALUES(p_model_id,l_start,SYSTIMESTAMP,p_max_iter);
        UPDATE model_registry SET status='TRAINED' WHERE model_id=p_model_id;
        COMMIT;
        pkg_data_ingestion.log_audit('TRAIN_KMEANS','SUCCESS',l_n);
    EXCEPTION WHEN OTHERS THEN
        ROLLBACK; pkg_data_ingestion.log_audit('TRAIN_KMEANS','ERROR',0,SQLERRM); RAISE;
    END train_kmeans;

END pkg_model_train;
/

