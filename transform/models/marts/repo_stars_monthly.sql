SELECT 
  repo_id, 
  repo_name, 
  DATE_TRUNC('month', event_date) AS date_month, 
  COUNT(*) AS star_count,
  SUM(star_count) OVER(PARTITION BY repo_id ORDER BY date_month) AS cumul_star_count 
FROM {{ ref("stg_gharchive") }} 
WHERE event_type = 'Watch' 
GROUP BY 1, 2, 3 