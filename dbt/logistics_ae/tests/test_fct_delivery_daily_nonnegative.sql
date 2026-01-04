-- metrics should not be negative
select *
from {{ ref('fct_delivery_daily') }}
where deliveries < 0
   or on_time_deliveries < 0
   or avg_detention_minutes < 0
