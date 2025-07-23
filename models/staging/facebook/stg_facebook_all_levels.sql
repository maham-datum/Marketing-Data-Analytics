select 
    date, 
    'facebook'as platform,
    campaign_name,
    adset_name,
    ad_name,
    impressions,
    inline_link_clicks as clicks,
    ctr,
    cpc,
    spend
from {{source('facebook','basic_all_levels')}}