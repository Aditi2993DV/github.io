/*
    Ensures that resolved tickets have a non-negative resolution time.
    Returns rows that violate this expectation (test fails if any rows returned).
*/

select
    ticket_id,
    full_resolution_time_calendar_minutes
from {{ ref('fct_ticket_metrics') }}
where is_resolved
  and full_resolution_time_calendar_minutes < 0
