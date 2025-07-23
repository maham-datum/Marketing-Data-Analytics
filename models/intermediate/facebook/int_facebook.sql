with aggregated as (
    select
        date,                     
        platform,                
        campaign_name,
        adset_name,
        ad_name,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(spend) as total_spend,
        case when sum(impressions) > 0 then sum(clicks)::float / sum(impressions) else null end as ctr,
        case when sum(clicks) > 0 then sum(spend)::float / sum(clicks) else null end as cpc
    from {{ ref('stg_facebook_all_levels') }}
    group by
        date,
        platform,
        campaign_name,
        adset_name,
        ad_name
)
select * from aggregated
