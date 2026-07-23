-- dbt checks this SELECT against the names and types declared in schema.yml.
{{ config(contract={"enforced": true}) }}

-- Staging changes representation only; it must not filter or aggregate rows.
select
    cast(dataset_id as {{ dbt.type_string() }}) as dataset_id,
    cast(edition as {{ dbt.type_string() }}) as edition,
    cast(version as integer) as version,
    cast(geography_code as {{ dbt.type_string() }}) as geography_code,
    cast(geography_label as {{ dbt.type_string() }}) as geography_label,
    cast(age_code as {{ dbt.type_string() }}) as age_code,
    cast(age_label as {{ dbt.type_string() }}) as age_label,
    cast(sex_code as {{ dbt.type_string() }}) as sex_code,
    cast(sex_label as {{ dbt.type_string() }}) as sex_label,
    cast(observation as bigint) as observation,
    cast(source_checksum as {{ dbt.type_string() }}) as source_checksum,
    cast(pipeline_run_id as {{ dbt.type_string() }}) as pipeline_run_id,
    cast(source_record_hash as {{ dbt.type_string() }}) as source_record_hash,
    cast(loaded_at as timestamp) as loaded_at
from {{ source('raw', 'raw_ons_observations') }}
