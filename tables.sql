-- 1.1 Raw Staging Table (Range-Partitioned by LOAD_DATE)
CREATE TABLE staging_churn (
    customer_id       VARCHAR2(20)   NOT NULL,
    tenure_months     NUMBER(5),
    monthly_charges   NUMBER(10,2),
    total_charges     NUMBER(14,2),
    contract_type     VARCHAR2(30),
    payment_method    VARCHAR2(40),
    internet_service  VARCHAR2(20),
    tech_support      VARCHAR2(5),
    online_security   VARCHAR2(5),
    senior_citizen    NUMBER(1),
    dependents        VARCHAR2(5),
    churn             VARCHAR2(5),
    load_date         DATE DEFAULT SYSDATE,
    CONSTRAINT pk_staging_churn PRIMARY KEY (customer_id, load_date)
)
PARTITION BY RANGE (load_date) (
    PARTITION p_2024   VALUES LESS THAN (DATE '2025-01-01'),
    PARTITION p_2025   VALUES LESS THAN (DATE '2026-01-01'),
    PARTITION p_2026   VALUES LESS THAN (DATE '2027-01-01'),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);
/

-- 1.2 Feature Store (EAV Model)
CREATE TABLE feature_store (
    entity_id      VARCHAR2(20)  NOT NULL,
    feature_name   VARCHAR2(50)  NOT NULL,
    feature_value  NUMBER,
    as_of_date     DATE          NOT NULL,
    CONSTRAINT pk_feature_store PRIMARY KEY (entity_id, feature_name, as_of_date)
);

CREATE INDEX idx_fs_date_feat ON feature_store (as_of_date, feature_name);
CREATE INDEX idx_fs_entity    ON feature_store (entity_id, as_of_date);
/

-- 1.3 Model Registry
CREATE TABLE model_registry (
    model_id     NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         VARCHAR2(100),
    version      VARCHAR2(20),
    algorithm    VARCHAR2(50),
    parameters   CLOB,
    created_date DATE DEFAULT SYSDATE,
    status       VARCHAR2(20) DEFAULT 'TRAINED'
        CONSTRAINT chk_model_status CHECK (status IN ('TRAINED','ACTIVE','ARCHIVED'))
);
/

-- 1.4 Model Weights
CREATE TABLE model_weights (
    weight_id    NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_id     NUMBER NOT NULL,
    feature_name VARCHAR2(50),
    weight_value NUMBER,
    bias         NUMBER DEFAULT 0,
    CONSTRAINT fk_mw_model FOREIGN KEY (model_id) REFERENCES model_registry(model_id)
);

CREATE INDEX idx_mw_model_feat ON model_weights (model_id, feature_name);
/

-- 1.5 Prediction Output
CREATE TABLE prediction_output (
    pred_id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entity_id       VARCHAR2(20),
    model_id        NUMBER NOT NULL,
    score           NUMBER,
    predicted_label VARCHAR2(5),
    scored_at       DATE DEFAULT SYSDATE,
    CONSTRAINT fk_po_model FOREIGN KEY (model_id) REFERENCES model_registry(model_id)
);

CREATE INDEX idx_pred_model_date   ON prediction_output (model_id, scored_at);
CREATE INDEX idx_pred_entity_model ON prediction_output (entity_id, model_id);
/

-- 1.6 Training Log
CREATE TABLE training_log (
    run_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_id    NUMBER NOT NULL,
    start_time  TIMESTAMP,
    end_time    TIMESTAMP,
    accuracy    NUMBER,
    precision_v NUMBER,
    recall_v    NUMBER,
    f1_score    NUMBER,
    loss        NUMBER,
    iterations  NUMBER,
    CONSTRAINT fk_tl_model FOREIGN KEY (model_id) REFERENCES model_registry(model_id)
);
/

-- 1.7 Pipeline Audit
CREATE TABLE pipeline_audit (
    audit_id       NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    step_name      VARCHAR2(100),
    status         VARCHAR2(20),
    rows_processed NUMBER,
    error_msg      CLOB,
    run_time       TIMESTAMP DEFAULT SYSTIMESTAMP
);
/