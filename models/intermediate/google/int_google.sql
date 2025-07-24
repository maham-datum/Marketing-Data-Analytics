with ad_groups as (
    select *
    from {{ ref('stg_google__ad_group_history') }}
),

campaigns as (
    select *
    from {{ ref('stg_google__campaign_history') }}
),

ad_stats_cleaned as (
    select
        date,
        ad_id,
        ad_group_id,
        campaign_id,
        coalesce(clicks, 0) as clicks,
        coalesce(impressions, 0) as impressions,
        coalesce(spent, 0) / 1000000.0 as spent
    from {{ ref('stg_google__ad_stats') }}
),

joined as (
    select
        a.date,
        a.ad_id,
        ag.ad_group_name,
        c.campaign_name,
        a.clicks,
        a.impressions,
        a.spent
    from ad_stats_cleaned a
    left join ad_groups ag on a.ad_group_id = ag.ad_group_id
    left join campaigns c on a.campaign_id = c.campaign_id
),

aggregated as (
    select
        date,
        'google' as platform,
        ad_id,
        ad_group_name,
        campaign_name,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        case when sum(impressions) > 0 then round(sum(clicks)::decimal / sum(impressions), 2) else 0 end as ctr,
        case when sum(clicks) > 0 then round(sum(spent)::decimal / sum(clicks), 2) else 0 end as cpc,
        round(sum(spent),2) as total_spent,
    from joined
    group by date, ad_id, ad_group_name, campaign_name
)

select *
from aggregated
order by date
