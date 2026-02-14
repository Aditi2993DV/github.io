{% macro minutes_to_hours(column_name) %}
    round({{ column_name }} / 60.0, 2)
{% endmacro %}


{% macro classify_response_time(minutes_column) %}
    case
        when {{ minutes_column }} is null then 'unknown'
        when {{ minutes_column }} <= 60 then 'under_1h'
        when {{ minutes_column }} <= 240 then '1h_to_4h'
        when {{ minutes_column }} <= 480 then '4h_to_8h'
        when {{ minutes_column }} <= 1440 then '8h_to_24h'
        else 'over_24h'
    end
{% endmacro %}


{% macro business_hours_between(start_ts, end_ts) %}
    /*
        Approximation: calculates elapsed business hours assuming
        8-hour business days and 5-day weeks.  For precise SLA
        calculations, use Zendesk's own business-hour metrics.
    */
    greatest(
        datediff('hour', {{ start_ts }}, {{ end_ts }})
        * (5.0 / 7.0)   -- weekday ratio
        * (8.0 / 24.0),  -- business-hour ratio
        0
    )
{% endmacro %}
