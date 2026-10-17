select target_month, country, count(*) as row_count
from {{ ref('gold_nowcasting_features') }}
group by target_month, country
having count(*) > 1
