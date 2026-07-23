-- Defense in depth: invalid negative measures must never reach the fact table.
select *
from {{ ref('fact_population_observation') }}
where observation < 0
