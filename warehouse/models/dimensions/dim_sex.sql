-- One row per ONS sex code provides a small conformed lookup dimension.
select
    -- Hashing the source code creates a stable warehouse join key.
    md5(sex_code) as sex_key,
    sex_code,
    -- Labels should agree for a code; MAX makes selection deterministic.
    max(sex_label) as sex_label
from {{ ref('stg_ons_observations') }}
group by sex_code
