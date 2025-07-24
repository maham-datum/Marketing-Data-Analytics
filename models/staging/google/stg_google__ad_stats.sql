select
    date,
    ad_id,
    ad_group_id,
    campaign_id,
    clicks,
    impressions,
    cost_micros as spent
from {{source('google','ad_stats')}}
