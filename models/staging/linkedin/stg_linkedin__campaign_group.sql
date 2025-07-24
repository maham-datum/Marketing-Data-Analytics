select
  id as campaign_group_id,
  name as campaign_group_name
from {{source('linkedin','campaign_group_history')}}