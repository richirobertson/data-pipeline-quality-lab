-- Publish one population total per source version and geography.
select
    geography.geography_code,
    geography.geography_label,
    fact.dataset_id,
    fact.edition,
    fact.version,
    sum(fact.observation) as population
from {{ ref('fact_population_observation') }} as fact
inner join {{ ref('dim_geography') }} as geography
    -- An inner join is safe because relationship tests prohibit orphaned facts.
    on fact.geography_key = geography.geography_key
group by
    geography.geography_code,
    geography.geography_label,
    fact.dataset_id,
    fact.edition,
    fact.version
