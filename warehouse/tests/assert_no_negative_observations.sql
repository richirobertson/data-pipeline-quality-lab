select *
from {{ ref('fact_population_observation') }}
where observation < 0

