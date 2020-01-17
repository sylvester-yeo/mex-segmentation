select 
    passenger_id
    ,city_name
    ,mex.business_name
    ,mex.primary_cuisine_names
    ,date_trunc('week', date(date_local)) as week_of
    ,count(1) as no_of_orders
    ,sum(gross_merchandise_value/fx_one_usd) as gmv_usd
    ,sum(promo_expense/fx_one_usd) as promo_expense_usd
    ,min(date_local) as min_date_local
    ,max(date_local) as max_date_local
    ,date_trunc('week', date(date_local)) as partition_date
from datamart_grabfood.base_bookings bb 
left join datamart.dim_merchants mex on bb.merchant_id = mex.merchant_id
where date(date_local) >= date_trunc('week', date([[inc_start_date]]))
    and date(date_local) <= date_add('week', 1, date_trunc('week', date([[inc_end_date]])))
    and bb.city_id = 10
    and bb.booking_state_simple = 'COMPLETED'
group by 1,2,3,4,5