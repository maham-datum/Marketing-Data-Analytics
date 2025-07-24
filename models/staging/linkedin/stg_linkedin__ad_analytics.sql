select
campaign_id,
day,
clicks,
impressions,
cost_in_local_currency as spent
from {{source('linkedin','ad_analytics_by_campaign')}}