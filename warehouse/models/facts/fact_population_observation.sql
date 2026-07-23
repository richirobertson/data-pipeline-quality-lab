-- Preserve the staging grain: dataset version + geography + age + sex.
select
    -- Separators prevent different value combinations producing the same text.
    md5(
        dataset_id || '|' || edition || '|' || version::text || '|' ||
        geography_code || '|' || age_code || '|' || sex_code
    ) as observation_key,
    -- Dimension keys use the same formulas as their target dimension models.
    md5(geography_code) as geography_key,
    md5(age_code) as age_key,
    md5(sex_code) as sex_key,
    dataset_id,
    edition,
    version,
    observation,
    source_checksum,
    pipeline_run_id,
    source_record_hash,
    loaded_at
from {{ ref('stg_ons_observations') }}
