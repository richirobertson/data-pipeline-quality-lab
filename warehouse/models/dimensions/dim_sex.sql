select
    md5(sex_code) as sex_key,
    sex_code,
    max(sex_label) as sex_label
from {{ ref('stg_ons_observations') }}
group by sex_code

