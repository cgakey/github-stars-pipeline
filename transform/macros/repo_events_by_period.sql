{% macro repo_event_by_period(event, period) -%} 
    SELECT
        repo_id, 
        repo_name, 
        DATE_TRUNC('{{ period }}', event_date) AS date_{{ period }}, 
        COUNT(*) AS {{ event }}_count,
        SUM({{ event }}_count) OVER(PARTITION BY repo_id ORDER BY date_{{ period }}) AS cumul_{{ event }}_count 
    FROM {{ ref("stg_gharchive") }} 
    WHERE event_type = '{{ event }}'
    GROUP BY 1, 2, 3 
{%- endmacro %}