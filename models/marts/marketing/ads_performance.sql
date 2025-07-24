with facebook as (
    select
        date,
        platform,
        campaign_name,
        adset_name as ad_group_name,
        ad_name as "ad_name/keyword",
        clicks,
        impressions,
        ctr,
        cpc,
        total_spent
    from {{ ref('int_facebook') }}
),

google as (
    select
        date,
        platform,
        campaign_name,
        ad_group_name,
        keyword as "ad_name/keyword", 
        clicks,
        impressions,
        ctr,
        cpc,
        total_spent
    from {{ ref('int_google') }}
),

linkedin as (
    select
        date,
        platform,
        campaign_name,
        campaign_group_name as "ad_group_name",
        creative_name as "ad_name/keyword",
        clicks,
        impressions,
        ctr,
        cpc,
        total_spent
    from {{ ref('int_linkedin') }}
)

select * from facebook
union all
select * from google
union all
select * from linkedin
