-- =============================================================================
-- Warehouse Setup
-- =============================================================================

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS ZENDESK_WH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60        -- seconds of inactivity before suspend
    AUTO_RESUME         = TRUE
    MIN_CLUSTER_COUNT   = 1
    MAX_CLUSTER_COUNT   = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Warehouse for Zendesk ingestion and transformation workloads';

-- Grant usage to pipeline roles
GRANT USAGE ON WAREHOUSE ZENDESK_WH TO ROLE FIVETRAN_ROLE;
GRANT USAGE ON WAREHOUSE ZENDESK_WH TO ROLE DBT_ROLE;
GRANT USAGE ON WAREHOUSE ZENDESK_WH TO ROLE ANALYST_ROLE;
