-- One row per ONS age code provides the lookup target used by the fact table.
select
    -- A deterministic hash is reproducible across full rebuilds.
    md5(age_code) as age_key,
    age_code,
    -- Labels should agree for a code; MAX makes selection deterministic.
    max(age_label) as age_label
from {{ ref('stg_ons_observations') }}
group by age_code
