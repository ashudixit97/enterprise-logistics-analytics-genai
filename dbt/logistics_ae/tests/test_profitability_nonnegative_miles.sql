select *
from {{ ref('fct_route_profitability_daily') }}
where miles < 0
