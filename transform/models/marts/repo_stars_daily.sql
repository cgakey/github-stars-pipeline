SELECT 
  repo_id, 
  repo_name, 
  DATE_TRUNC('day', event_date) AS date_day, 
  COUNT(*) AS star_count, 
  SUM(star_count) OVER(PARTITION BY repo_id ORDER BY date_day) AS cumul_star_count
FROM {{ ref("stg_gharchive") }} 
WHERE event_type = 'Watch' 
GROUP BY 1, 2, 3 