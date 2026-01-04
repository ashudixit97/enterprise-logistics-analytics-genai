select *
from {{ ref('fct_route_profitability_daily') }}
where fuel_cost_per_mile < 0
   or profit_per_mile is null
