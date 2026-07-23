select *
from {{ ref('mart_population_by_geography') }}
where
    (geography_code = 'E06000057' and population != 273000)
    or (geography_code = 'E08000025' and population != 962000)
    or geography_code not in ('E06000057', 'E08000025')

