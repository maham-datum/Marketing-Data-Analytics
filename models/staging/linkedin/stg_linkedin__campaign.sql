select
  id as campaign_id,
  name as campaign_name,
  campaign_group_id
from {{source('linkedin','campaign_history')}}