select 
  LOWER(REPLACE(type, 'Event', '')) as event_type,
  actor.login as user, 
  repo.id as repo_id, 
  repo.name as repo_name, 
  created_at as event_date 
from {{ source("gharchive", "gharchive") }} 
