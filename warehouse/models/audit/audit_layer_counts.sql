-- Keep row counts queryable so reconciliation is evidence, not a log message.
with counts as (
    select 'raw' as layer, count(*)::bigint as row_count
    from {{ source('raw', 'raw_ons_observations') }}
    -- UNION ALL retains every layer even when two layers have the same count.
    union all
    select 'staging' as layer, count(*)::bigint as row_count
    from {{ ref('stg_ons_observations') }}
    union all
    select 'fact' as layer, count(*)::bigint as row_count
    from {{ ref('fact_population_observation') }}
)
select * from counts
