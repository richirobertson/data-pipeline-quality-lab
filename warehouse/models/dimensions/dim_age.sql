select
    md5(age_code) as age_key,
    age_code,
    max(age_label) as age_label
from {{ ref('stg_ons_observations') }}
group by age_code

