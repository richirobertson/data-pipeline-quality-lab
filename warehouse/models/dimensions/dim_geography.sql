-- One row per ONS geography code prevents descriptive labels repeating in facts.
select
    -- A deterministic surrogate key keeps joins stable across rebuilds.
    md5(geography_code) as geography_key,
    geography_code,
    -- Labels should agree for a code; MAX makes selection deterministic.
    max(geography_label) as geography_label
from {{ ref('stg_ons_observations') }}
group by geography_code
