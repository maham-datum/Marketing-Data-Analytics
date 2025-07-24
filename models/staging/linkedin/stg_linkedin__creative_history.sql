select
    last_modified_at as date,
    campaign_id,
    name as creative_name
from {{source('linkedin','creative_history')}}