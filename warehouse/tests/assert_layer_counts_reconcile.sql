-- Equal minimum and maximum counts prove no rows disappeared or multiplied.
with counts as (
    select min(row_count) as minimum_count, max(row_count) as maximum_count
    from {{ ref('audit_layer_counts') }}
)
select *
from counts
where minimum_count != maximum_count
