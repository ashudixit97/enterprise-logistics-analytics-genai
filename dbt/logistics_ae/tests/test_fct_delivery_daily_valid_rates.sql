-- on_time_rate must be between 0 and 1
select *
from {{ ref('fct_delivery_daily') }}
where on_time_rate < 0
   or on_time_rate > 1