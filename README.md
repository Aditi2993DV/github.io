# Zendesk to Snowflake Data Pipeline

An end-to-end ELT pipeline that extracts Zendesk support data via **Fivetran**, transforms it with **dbt** (using Fivetran's official dbt packages), loads it into **Snowflake**, and is orchestrated by **Apache Airflow** through a scalable YAML-driven DAG factory.

```
Zendesk (API)
     |
     v
Fivetran Connector ──sync──> Snowflake  ZENDESK_RAW.ZENDESK
                                              |
                                        dbt transforms
                                              |
                                     ZENDESK_ANALYTICS
                                     ├── STAGING        (views)
                                     ├── INTERMEDIATE   (ephemeral)
                                     └── MARTS          (tables)
                                              |
                                         BI / Analytics
```

---

## Table of Contents

- [Project Structure](#project-structure)
- [Architecture Overview](#architecture-overview)
- [Snowflake Setup](#snowflake-setup)
- [dbt Project](#dbt-project)
  - [Packages and Configuration](#packages-and-configuration)
  - [Staging Layer](#staging-layer)
  - [Intermediate Layer](#intermediate-layer)
  - [Marts Layer](#marts-layer)
  - [Macros, Seeds, and Tests](#macros-seeds-and-tests)
- [Airflow Orchestration](#airflow-orchestration)
  - [YAML-Driven DAG Factory](#yaml-driven-dag-factory)
  - [Pipeline Configuration](#pipeline-configuration)
  - [Adding a New Pipeline](#adding-a-new-pipeline)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Environment Variables](#environment-variables)
  - [Step-by-Step Setup](#step-by-step-setup)
- [Data Model Reference](#data-model-reference)

---

## Project Structure

```
.
├── snowflake/
│   └── setup/
│       ├── 01_create_databases.sql           # ZENDESK_RAW + ZENDESK_ANALYTICS databases
│       ├── 02_create_schemas.sql             # STAGING / INTERMEDIATE / MARTS schemas
│       ├── 03_create_roles_and_permissions.sql # FIVETRAN_ROLE, DBT_ROLE, ANALYST_ROLE
│       └── 04_create_warehouse.sql           # ZENDESK_WH (X-SMALL, auto-suspend)
│
├── dbt_project/
│   ├── dbt_project.yml                       # Project config and materialization rules
│   ├── packages.yml                          # fivetran/zendesk, fivetran/zendesk_source, dbt_utils
│   ├── profiles.yml                          # Snowflake connection (dev + prod targets)
│   │
│   ├── models/
│   │   ├── staging/                          # Layer 1: clean and rename raw data
│   │   │   ├── sources.yml                   #   Source definitions + freshness checks
│   │   │   ├── schema.yml                    #   Model docs + column tests
│   │   │   ├── stg_zendesk__tickets.sql
│   │   │   ├── stg_zendesk__users.sql
│   │   │   ├── stg_zendesk__organizations.sql
│   │   │   ├── stg_zendesk__ticket_comments.sql
│   │   │   ├── stg_zendesk__groups.sql
│   │   │   ├── stg_zendesk__brands.sql
│   │   │   ├── stg_zendesk__ticket_metrics.sql
│   │   │   └── stg_zendesk__ticket_tags.sql
│   │   │
│   │   ├── intermediate/                     # Layer 2: business logic (ephemeral)
│   │   │   ├── schema.yml
│   │   │   ├── int_zendesk__ticket_enriched.sql
│   │   │   ├── int_zendesk__agent_performance.sql
│   │   │   └── int_zendesk__sla_compliance.sql
│   │   │
│   │   └── marts/                            # Layer 3: BI-ready dimensions + facts
│   │       ├── schema.yml
│   │       ├── dim_tickets.sql
│   │       ├── dim_users.sql
│   │       ├── dim_organizations.sql
│   │       ├── fct_ticket_metrics.sql
│   │       ├── fct_agent_performance.sql
│   │       └── fct_daily_ticket_summary.sql
│   │
│   ├── macros/
│   │   └── zendesk_utils.sql                 # Utility macros (time conversion, bucketing)
│   ├── seeds/
│   │   └── zendesk_ticket_priority_weights.csv # SLA targets per priority level
│   └── tests/
│       └── assert_ticket_resolution_positive.sql
│
├── airflow/
│   ├── dags/
│   │   ├── zendesk_snowflake_pipeline.py     # Entry point — calls the DAG factory
│   │   ├── requirements.txt                  # Python dependencies
│   │   ├── factory/
│   │   │   └── dag_factory.py                # Generic YAML-to-DAG generator
│   │   └── pipelines/
│   │       ├── zendesk.yml                   # Zendesk pipeline config
│   │       └── salesforce_example.yml        # Example: adding a second pipeline
│   └── plugins/
│       └── zendesk_callbacks.py              # Failure/success alert handlers
│
├── .env.example                              # Credential template
└── .gitignore
```

---

## Architecture Overview

This pipeline follows the **ELT** (Extract, Load, Transform) pattern:

| Stage | Tool | What Happens |
|-------|------|--------------|
| **Extract + Load** | Fivetran | Connects to the Zendesk API and syncs raw tables (tickets, users, organizations, comments, etc.) into Snowflake's `ZENDESK_RAW.ZENDESK` schema. |
| **Transform** | dbt | Reads raw data and applies three layers of transformation — staging, intermediate, and marts — writing results to `ZENDESK_ANALYTICS`. |
| **Orchestrate** | Airflow | Triggers the Fivetran sync, waits for completion, then runs dbt seed, source freshness checks, model builds, and tests in sequence. |
| **Serve** | Snowflake | Final mart tables in `ZENDESK_ANALYTICS.MARTS` are available to BI tools (Looker, Tableau, etc.) via the `ANALYST_ROLE`. |

### Why this pattern?

- **Fivetran** handles the complexity of the Zendesk API (pagination, rate limits, schema changes) so you don't write custom extraction code.
- **dbt** provides version-controlled, testable SQL transformations with a dependency graph.
- **Snowflake** separates storage from compute, so Fivetran writes and dbt transforms don't compete for resources.
- **Airflow** ties it together with scheduling, retries, alerting, and dependency management.

---

## Snowflake Setup

Run the SQL scripts in `snowflake/setup/` in numbered order against your Snowflake account. They create:

### Databases

| Database | Purpose |
|----------|---------|
| `ZENDESK_RAW` | Landing zone for Fivetran. Raw Zendesk tables land here untouched. 7-day time travel. |
| `ZENDESK_ANALYTICS` | dbt writes transformed models here. 14-day time travel. |

### Schemas

| Schema | Database | Purpose |
|--------|----------|---------|
| `ZENDESK` | ZENDESK_RAW | Fivetran target schema (raw tables) |
| `STAGING` | ZENDESK_ANALYTICS | Cleaned/renamed views of raw data |
| `INTERMEDIATE` | ZENDESK_ANALYTICS | (Ephemeral — not materialized as physical tables) |
| `MARTS` | ZENDESK_ANALYTICS | Final dimension and fact tables for BI |

### Roles (Least-Privilege Access)

```
SYSADMIN
├── FIVETRAN_ROLE   → Can write to ZENDESK_RAW.ZENDESK only
├── DBT_ROLE        → Can read ZENDESK_RAW, write all of ZENDESK_ANALYTICS
└── ANALYST_ROLE    → Can read ZENDESK_ANALYTICS.MARTS only
```

### Warehouse

`ZENDESK_WH` — X-SMALL size, auto-suspends after 60 seconds of inactivity, auto-resumes on query. Shared by all three roles.

---

## dbt Project

### Packages and Configuration

**`packages.yml`** pulls in three packages:

| Package | Purpose |
|---------|---------|
| `fivetran/zendesk` | Pre-built transformation models for Fivetran's Zendesk connector. Provides tested, maintained SQL for common Zendesk analytics. |
| `fivetran/zendesk_source` | Staging models that the zendesk package depends on. Maps raw Fivetran column names to clean names. |
| `dbt-labs/dbt_utils` | Utility macros (e.g. `accepted_range` test used in mart schemas). |

**`dbt_project.yml`** defines materialization rules:

| Layer | Materialization | Rationale |
|-------|----------------|-----------|
| Staging | `view` | Lightweight; always reads fresh data from raw. No storage cost. |
| Intermediate | `ephemeral` | Inlined as CTEs into downstream models. No physical table created. |
| Marts | `table` | Full table rebuild for fast BI queries. `fct_daily_ticket_summary` uses `incremental` for efficiency. |

**`profiles.yml`** defines two Snowflake targets:

- **dev**: 4 threads, for local development
- **prod**: 8 threads, used by Airflow in production

Both use environment variables (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`) so credentials never live in code.

### Staging Layer

**Location**: `models/staging/`
**Materialization**: views
**Purpose**: One model per raw Zendesk table. Each model does three things:

1. **Filters** deleted rows (`where not coalesce(_fivetran_deleted, false)`)
2. **Renames** columns to a consistent convention (`id` → `ticket_id`, `via__channel` → `channel`)
3. **Casts** timestamps to `timestamp_ntz`

| Model | Source Table | Key Columns |
|-------|-------------|-------------|
| `stg_zendesk__tickets` | `ticket` | ticket_id, requester_id, assignee_id, ticket_status, ticket_priority, channel |
| `stg_zendesk__users` | `user` | user_id, user_name, email, user_role (end-user/agent/admin), is_active |
| `stg_zendesk__organizations` | `organization` | organization_id, organization_name, has_shared_tickets |
| `stg_zendesk__ticket_comments` | `ticket_comment` | comment_id, ticket_id, author_id, body, is_public |
| `stg_zendesk__groups` | `group` | group_id, group_name |
| `stg_zendesk__brands` | `brand` | brand_id, brand_name, subdomain, is_active |
| `stg_zendesk__ticket_metrics` | `ticket_metric` | ticket_id, first_reply_time_*_minutes, full_resolution_time_*_minutes, reopens, replies |
| `stg_zendesk__ticket_tags` | `ticket_tag` | ticket_id, tag_name |

**Source freshness** is configured in `sources.yml`: warns after 12 hours, errors after 24 hours of no Fivetran sync.

**Tests** in `schema.yml` include:
- `unique` + `not_null` on all primary keys
- `accepted_values` on ticket_status, ticket_priority, and user_role

### Intermediate Layer

**Location**: `models/intermediate/`
**Materialization**: ephemeral (compiled as CTEs, no physical tables)
**Purpose**: Reusable business logic that multiple marts depend on.

#### `int_zendesk__ticket_enriched`

Builds a wide record per ticket by joining:

```
tickets
├── LEFT JOIN users (as requesters)  → requester_name, requester_email
├── LEFT JOIN users (as assignees)   → assignee_name, assignee_email
├── LEFT JOIN organizations          → organization_name
├── LEFT JOIN groups                 → group_name
├── LEFT JOIN brands                 → brand_name
└── LEFT JOIN ticket_metrics         → reply times, resolution times, reopens
```

Also adds derived columns:
- `is_resolved` — true when status is `solved` or `closed`
- `hours_open` — hours between creation and resolution (or now if still open)

#### `int_zendesk__agent_performance`

Aggregates enriched tickets per assignee (agent):
- Ticket counts: total assigned, resolved, open
- Response time: avg and median first reply time (business minutes)
- Resolution time: avg and median full resolution time (business minutes)
- Quality signals: avg reopens per ticket, avg replies per ticket
- Satisfaction: good/bad/rated counts, satisfaction percentage

#### `int_zendesk__sla_compliance`

Joins each ticket to the SLA target seed data (`zendesk_ticket_priority_weights.csv`) and evaluates:
- `first_reply_sla_met` — Was the first reply within the target for this priority?
- `resolution_sla_met` — Was the ticket resolved within the target?

### Marts Layer

**Location**: `models/marts/`
**Materialization**: table (except `fct_daily_ticket_summary` which is incremental)
**Purpose**: Final dimension and fact tables optimized for BI tool consumption.

#### Dimensions

| Model | Grain | Key Content |
|-------|-------|-------------|
| `dim_tickets` | 1 row per ticket | Full context: subject, status, priority, channel, requester/assignee info, org/group/brand, comma-separated tags, timestamps, hours_open |
| `dim_users` | 1 row per user | Name, email, role, active/suspended flags, organization name, login timestamps |
| `dim_organizations` | 1 row per organization | Org details + aggregated ticket counts (total, resolved, open), first/latest ticket dates |

#### Facts

| Model | Grain | Key Content |
|-------|-------|-------------|
| `fct_ticket_metrics` | 1 row per ticket | All timing metrics (reply, resolution, wait, hold), SLA compliance flags with targets, satisfaction score, volume metrics (reopens, replies) |
| `fct_agent_performance` | 1 row per agent per group | Avg/median response and resolution times, ticket volume, satisfaction percentage |
| `fct_daily_ticket_summary` | 1 row per day | Daily counts (created, resolved, urgent, high), channel breakdown, avg response/resolution times, SLA compliance percentages, satisfaction |

`fct_daily_ticket_summary` uses **incremental materialization** with a merge strategy — on each run it only processes tickets created since the last summary date, making it efficient for large volumes.

### Macros, Seeds, and Tests

#### Macros (`macros/zendesk_utils.sql`)

| Macro | Usage |
|-------|-------|
| `minutes_to_hours(column)` | Converts a minutes column to hours (`column / 60.0`) |
| `classify_response_time(minutes_column)` | Buckets response time into categories: under_1h, 1h_to_4h, 4h_to_8h, 8h_to_24h, over_24h |
| `business_hours_between(start, end)` | Approximates business hours between two timestamps (5/7 weekday ratio, 8/24 hour ratio) |

#### Seeds (`seeds/zendesk_ticket_priority_weights.csv`)

Static reference data loaded by `dbt seed`:

| priority | first_reply_target_minutes | resolution_target_minutes | priority_weight |
|----------|---------------------------|--------------------------|----------------|
| urgent   | 30                        | 240  (4 hours)           | 4              |
| high     | 60                        | 480  (8 hours)           | 3              |
| normal   | 120                       | 1440 (24 hours)          | 2              |
| low      | 480                       | 2880 (48 hours)          | 1              |

These targets drive the SLA compliance evaluation in `int_zendesk__sla_compliance`.

#### Custom Tests (`tests/`)

`assert_ticket_resolution_positive.sql` — Fails if any resolved ticket has a negative resolution time (data quality gate).

---

## Airflow Orchestration

### YAML-Driven DAG Factory

Instead of writing a Python DAG file for each pipeline, this project uses a **factory pattern**:

```
airflow/dags/
├── zendesk_snowflake_pipeline.py   ← 4-line entry point
├── factory/
│   └── dag_factory.py              ← Generic DAG generator
└── pipelines/
    ├── zendesk.yml                 ← Pipeline config (YAML)
    └── salesforce_example.yml      ← Another pipeline (YAML)
```

**How it works:**

1. `zendesk_snowflake_pipeline.py` calls `generate_dags()` at module load time.
2. `generate_dags()` scans `pipelines/*.yml` and reads each YAML file.
3. For each YAML file, it parses the config into Python dataclasses and builds a full Airflow DAG.
4. The generated DAGs are injected into `globals()` so Airflow's DagBag discovers them.

### Pipeline Configuration

Each YAML file defines a complete pipeline. Here's the Zendesk config (`pipelines/zendesk.yml`):

```yaml
dag:
  dag_id: zendesk_snowflake_pipeline
  schedule_interval: "0 */2 * * *"    # every 2 hours
  tags: [zendesk, fivetran, dbt, snowflake]

fivetran:
  connection_id: fivetran_default
  connectors:
    - id: "{{ var.value.zendesk_fivetran_connector_id }}"
      name: zendesk_support
      poke_interval_seconds: 60
      timeout_seconds: 3600

dbt:
  project_dir: "{{ var.value.dbt_project_dir }}"
  profiles_dir: "{{ var.value.dbt_profiles_dir }}"
  target: prod
  steps:
    - command: seed
    - command: source
      args: "freshness"
    - command: run
      selectors:
        - tag:staging
        - tag:marts
    - command: test

callbacks:
  on_failure:
    module: zendesk_callbacks
    function: on_failure_callback
```

The factory translates this into the following Airflow task graph:

```
[fivetran_zendesk_support]
  ├── trigger_sync
  └── wait_for_sync
         |
         v
    [dbt_seed_0]
         |
         v
    [dbt_source_1]  (source freshness)
         |
         v
    [dbt_run_2]
      ├── dbt_run_tag_staging
      └── dbt_run_tag_marts   (sequential)
         |
         v
    [dbt_test_3]
         |
         v
    [pipeline_complete]
```

Key features of the factory:
- **Multiple Fivetran connectors** run in parallel (separate TaskGroups, all must finish before dbt starts)
- **dbt selectors** within a single step run sequentially in a TaskGroup (staging before marts)
- **Callbacks** are dynamically imported from the module/function specified in YAML
- **Retries** and **alert settings** come from `default_args` in the YAML

### Adding a New Pipeline

To onboard a new data source (e.g., Salesforce, Jira, HubSpot):

1. Set up the Fivetran connector in the Fivetran dashboard.
2. Create the dbt models (staging + marts) with appropriate tags.
3. Drop a new YAML file in `airflow/dags/pipelines/`:

```yaml
# pipelines/hubspot.yml
dag:
  dag_id: hubspot_snowflake_pipeline
  schedule_interval: "0 */6 * * *"
  tags: [hubspot, fivetran, dbt, snowflake]

fivetran:
  connection_id: fivetran_default
  connectors:
    - id: "{{ var.value.hubspot_fivetran_connector_id }}"
      name: hubspot_crm

dbt:
  project_dir: "{{ var.value.dbt_project_dir }}"
  profiles_dir: "{{ var.value.dbt_profiles_dir }}"
  target: prod
  steps:
    - command: run
      selectors: [tag:hubspot]
    - command: test
      args: "--select tag:hubspot"
```

That's it. No Python code changes. Airflow picks up the new DAG on its next DagBag refresh.

---

## Getting Started

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Snowflake account | — | Data warehouse |
| Fivetran account | — | Zendesk connector |
| Python | >= 3.9 | dbt and Airflow runtime |
| dbt-snowflake | >= 1.7 | dbt adapter for Snowflake |
| Apache Airflow | >= 2.7 | Orchestration |

### Environment Variables

Copy `.env.example` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Your Snowflake account identifier (e.g., `abc123.us-east-1.aws`) |
| `SNOWFLAKE_USER` | Service account username (e.g., `DBT_USER`) |
| `SNOWFLAKE_PASSWORD` | Service account password |
| `FIVETRAN_API_KEY` | Fivetran REST API key |
| `FIVETRAN_API_SECRET` | Fivetran REST API secret |

### Step-by-Step Setup

**1. Provision Snowflake infrastructure**

Run the setup scripts in order in a Snowflake worksheet (logged in as `SYSADMIN` / `SECURITYADMIN`):

```sql
-- Run each file in sequence:
-- snowflake/setup/01_create_databases.sql
-- snowflake/setup/02_create_schemas.sql
-- snowflake/setup/03_create_roles_and_permissions.sql
-- snowflake/setup/04_create_warehouse.sql
```

**2. Configure the Fivetran Zendesk connector**

In the Fivetran dashboard:
- Create a new connector for Zendesk.
- Set the destination to `ZENDESK_RAW.ZENDESK` in Snowflake.
- Use the `FIVETRAN_USER` / `FIVETRAN_ROLE` credentials.
- Note the connector ID for the Airflow variable.

**3. Install dbt packages**

```bash
cd dbt_project
dbt deps          # Downloads fivetran/zendesk, fivetran/zendesk_source, dbt_utils
dbt seed          # Loads zendesk_ticket_priority_weights.csv
dbt run           # Builds all models
dbt test          # Runs all tests
```

**4. Set up Airflow**

```bash
pip install -r airflow/dags/requirements.txt

# Set Airflow variables
airflow variables set zendesk_fivetran_connector_id "<your-connector-id>"
airflow variables set dbt_project_dir "/path/to/dbt_project"
airflow variables set dbt_profiles_dir "/path/to/dbt_project"

# Set Airflow connections
airflow connections add fivetran_default \
    --conn-type http \
    --login "$FIVETRAN_API_KEY" \
    --password "$FIVETRAN_API_SECRET"
```

The DAG `zendesk_snowflake_pipeline` will appear in the Airflow UI and run every 2 hours.

---

## Data Model Reference

### Entity-Relationship Diagram

```
                    ┌──────────────────────┐
                    │   dim_organizations  │
                    │──────────────────────│
                    │ organization_id (PK) │
                    │ organization_name    │
                    │ total_tickets        │
                    │ resolved_tickets     │
                    │ open_tickets         │
                    └──────────┬───────────┘
                               │ 1:N
                               │
┌──────────────┐    ┌──────────┴───────────┐    ┌─────────────────────┐
│  dim_users   │    │     dim_tickets      │    │  fct_ticket_metrics │
│──────────────│    │──────────────────────│    │─────────────────────│
│ user_id (PK) ├─N:1┤ ticket_id (PK)      ├─1:1┤ ticket_id (PK)     │
│ user_name    │    │ subject              │    │ first_reply_time_*  │
│ email        │    │ ticket_status        │    │ resolution_time_*   │
│ user_role    │    │ ticket_priority      │    │ first_reply_sla_met │
│ org_name     │    │ requester_name       │    │ resolution_sla_met  │
│ is_active    │    │ assignee_name        │    │ satisfaction_score   │
└──────────────┘    │ organization_name    │    │ reopens, replies    │
                    │ group_name           │    └─────────────────────┘
                    │ brand_name           │
                    │ tags                 │
                    │ hours_open           │
                    └──────────────────────┘

┌─────────────────────────┐    ┌───────────────────────────────┐
│  fct_agent_performance  │    │   fct_daily_ticket_summary    │
│─────────────────────────│    │───────────────────────────────│
│ agent_id (PK)           │    │ summary_date (PK)             │
│ agent_name              │    │ tickets_created               │
│ total_tickets_assigned  │    │ tickets_resolved              │
│ avg_first_reply_time    │    │ email/web/chat/api_tickets    │
│ avg_resolution_time     │    │ avg_first_reply_time_minutes  │
│ satisfaction_pct        │    │ first_reply_sla_pct           │
└─────────────────────────┘    │ resolution_sla_pct            │
                               └───────────────────────────────┘
```

### Common BI Queries

**Ticket backlog by priority:**
```sql
SELECT ticket_priority, count(*) AS open_tickets
FROM zendesk_analytics.marts.dim_tickets
WHERE NOT is_resolved
GROUP BY 1
ORDER BY
  CASE ticket_priority
    WHEN 'urgent' THEN 1 WHEN 'high' THEN 2
    WHEN 'normal' THEN 3 WHEN 'low'  THEN 4
  END;
```

**Weekly SLA compliance trend:**
```sql
SELECT
  date_trunc('week', summary_date)  AS week,
  round(avg(first_reply_sla_pct), 1) AS avg_first_reply_sla,
  round(avg(resolution_sla_pct), 1)  AS avg_resolution_sla
FROM zendesk_analytics.marts.fct_daily_ticket_summary
GROUP BY 1
ORDER BY 1;
```

**Top 10 agents by satisfaction:**
```sql
SELECT agent_name, total_tickets_assigned, satisfaction_pct
FROM zendesk_analytics.marts.fct_agent_performance
WHERE rated_ticket_count >= 10
ORDER BY satisfaction_pct DESC
LIMIT 10;
```
