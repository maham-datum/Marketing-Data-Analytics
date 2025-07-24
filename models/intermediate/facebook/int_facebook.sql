with cleaned as (
    select
        date,
        platform,
        campaign_name,
        adset_name,
        ad_name,
        coalesce(impressions, 0) as impressions,
        coalesce(clicks, 0) as clicks,
        coalesce(spend, 0) as spend
    from {{ ref('stg_facebook_all_levels') }}
),

aggregated as (
    select
        date,
        platform,
        campaign_name,
        adset_name,
        ad_name,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        case when sum(impressions) > 0 then round(sum(clicks)::decimal / sum(impressions), 2) else 0 end as ctr,
        case when sum(clicks) > 0 then round(sum(spend)::decimal / sum(clicks), 2) else 0 end as cpc,
        sum(spend) as total_spent
    from cleaned
    group by
        date,
        platform,
        campaign_name,
        adset_name,
        ad_name
)

select *
from aggregated
order by date
