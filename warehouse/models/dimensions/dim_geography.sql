select
    md5(geography_code) as geography_key,
    geography_code,
    max(geography_label) as geography_label
from {{ ref('stg_ons_observations') }}
group by geography_code

