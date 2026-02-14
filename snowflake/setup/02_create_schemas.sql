-- =============================================================================
-- Schema Setup
-- =============================================================================

USE ROLE SYSADMIN;

-- ---- ZENDESK_RAW schemas ----
-- Fivetran will auto-create the ZENDESK schema, but we define it explicitly
-- so permissions can be granted upfront.
USE DATABASE ZENDESK_RAW;

CREATE SCHEMA IF NOT EXISTS ZENDESK
    COMMENT = 'Raw Zendesk tables managed by Fivetran connector';

-- ---- ZENDESK_ANALYTICS schemas (dbt layers) ----
USE DATABASE ZENDESK_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Staging models - cleaned, renamed, typed raw Zendesk data';

CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    COMMENT = 'Intermediate models - business logic joins and enrichments';

CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Mart models - final dimension and fact tables for BI consumption';
