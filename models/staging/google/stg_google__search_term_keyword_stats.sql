select
    date,
    search_term as keyword,
    ad_group_id,
    campaign_id,
    clicks,
    impressions,
    cost_micros as spent
from {{source('google','search_term_keyword_stats')}}

