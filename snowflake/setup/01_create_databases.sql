-- =============================================================================
-- Snowflake Database Setup for Zendesk Pipeline
-- =============================================================================
-- Fivetran lands raw data into ZENDESK_RAW.
-- dbt transforms data through ZENDESK_ANALYTICS (staging -> intermediate -> marts).
-- =============================================================================

USE ROLE SYSADMIN;

-- Raw database: Fivetran connector lands Zendesk data here
CREATE DATABASE IF NOT EXISTS ZENDESK_RAW
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw Zendesk data ingested by Fivetran';

-- Analytics database: dbt transformations live here
CREATE DATABASE IF NOT EXISTS ZENDESK_ANALYTICS
    DATA_RETENTION_TIME_IN_DAYS = 14
    COMMENT = 'Transformed Zendesk data produced by dbt';
