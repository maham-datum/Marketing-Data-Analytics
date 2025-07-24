with ad_data as (
    select *
    from {{ ref('stg_linkedin__ad_analytics') }}
),

campaigns as (
    select *
    from {{ ref('stg_linkedin__campaign') }}
),

campaign_groups as (
    select *
    from {{ ref('stg_linkedin__campaign_group') }}
),

creative as (
    select
        distinct on (campaign_id)
        date,
        campaign_id,
        creative_name
    from {{ ref('stg_linkedin__creative_history') }}
    order by campaign_id, date desc
),

joined as (
    select
        cast(ad.day as date) as date,
        ad.campaign_id,
        c.campaign_name,
        g.campaign_group_name,
        coalesce(ad.impressions, 0) as impressions,
        coalesce(ad.clicks, 0) as clicks,
        coalesce(ad.spent, 0) as spent,
        cr.creative_name
    from ad_data ad
    left join campaigns c on ad.campaign_id = c.campaign_id
    left join campaign_groups g on c.campaign_group_id = g.campaign_group_id
    left join creative cr on ad.campaign_id = cr.campaign_id
),

 aggregated as (
    select
        date,
        'linkedin' as platform,
        campaign_name,
        campaign_group_name,
        creative_name,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        case when sum(impressions) > 0 then round((sum(clicks)::decimal / sum(impressions)) * 100, 2) else 0 end as ctr,
        case when sum(clicks) > 0 then round(sum(spent) / sum(clicks), 2) else 0 end as cpc,
        sum(spent) as total_spent
    from joined
    group by date, campaign_name, campaign_group_name, creative_name
)

select *
from aggregated
order by date