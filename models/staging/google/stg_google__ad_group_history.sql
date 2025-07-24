select
    id as ad_group_id,
    name as ad_group_name
from {{source('google','ad_group_history')}}