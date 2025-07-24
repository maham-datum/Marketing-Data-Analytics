select
    id as campaign_id,
    name as campaign_name
from {{source('google','campaign_history')}}