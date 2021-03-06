with first_gf_order as (
    select 
        bb.passenger_id
        ,mex.business_name
        ,date_trunc('week', date(date_local)) as week_of
        ,1 as first_gf_indicator
    from datamart_grabfood.base_bookings bb 
    inner join transforms.passenger_first_last_booking_v2 pax on bb.passenger_id = pax.passenger_id and bb.booking_code = pax.grabfood_ride_code
    left join datamart.dim_merchants mex on bb.merchant_id = mex.merchant_id
    where date_trunc('week',date(date_local)) = date([[inc_start_date]])
)
,current_week as (
    select 
        bb.*
        ,case when first_gf_order.first_gf_indicator = 1 then 'First GF Order' else NULL end as first_gf_order
    from slide.gf_pax_agg bb
    left join first_gf_order 
        on bb.passenger_id = first_gf_order.passenger_id 
            and bb.business_name = first_gf_order.business_name
            and bb.week_of = first_gf_order.week_of
    where date_trunc('week',date(partition_date)) = date([[inc_start_date]])
)
,past_eight_weeks as (
    select 
        passenger_id
        ,business_name
        ,min(min_date_local) as min_date_local
        ,cast(max(max_date_local) as date) as max_date_local
        ,sum(no_of_orders) as no_of_orders
        ,sum(gmv_usd) as gmv_usd
        ,sum(promo_expense_usd) as promo_expense_usd
    from slide.gf_pax_agg
    where date_trunc('week',date(partition_date)) >= date_add('week', -8, date_trunc('week',date([[inc_start_date]])))
        and date_trunc('week',date(partition_date)) < date([[inc_start_date]])
    group by 1,2
)
, int as (
    select 
        current_week.passenger_id
        ,current_week.business_name
        ,current_week.week_of
        ,case 
            when first_gf_order.passenger_id is not null then 'First GF order'
            when date_diff('day',past_eight_weeks.max_date_local, current_week.week_of) <= 28 then 'Active Pax'
            when date_diff('day',past_eight_weeks.max_date_local, current_week.week_of) > 28 then 'Resurrected Pax'
            when past_eight_weeks.passenger_id is null then 'Not Found in past 8 weeks'
            else 'Error' end as pax_label
        ,sum(current_week.no_of_orders) as current_no_of_orders
        ,sum(current_week.gmv_usd) as current_gmv_usd
        ,sum(current_week.promo_expense_usd) as current_promo_expense_usd
        
        ,sum(past_eight_weeks.no_of_orders) as past_no_of_orders
        ,sum(past_eight_weeks.gmv_usd) as past_gmv_usd
        ,sum(past_eight_weeks.promo_expense_usd) as past_promo_expense_usd
    from current_week 
    left join past_eight_weeks on current_week.passenger_id = past_eight_weeks.passenger_id and current_week.business_name = past_eight_weeks.business_name
    left join first_gf_order on current_week.passenger_id = first_gf_order.passenger_id and current_week.business_name = first_gf_order.business_name
    group by 1,2,3,4
)
select 
    business_name
    ,week_of
    ,count(distinct passenger_id) as distinct_pax_id
    ,sum(case when pax_label = 'First GF order' then 1 else 0 end) as first_gf_order_pax
    ,sum(case when pax_label = 'Active Pax' then 1 else 0 end) as active_pax
    ,sum(case when pax_label = 'Resurrected Pax' then 1 else 0 end) as ressurrected_pax
    ,sum(case when pax_label = 'Not Found in past 8 weeks' then 1 else 0 end) as unknown_pax

    ,sum(case when pax_label = 'First GF order' then current_no_of_orders else 0 end) as first_gf_order_current_no_of_orders
    ,sum(case when pax_label = 'Active Pax' then current_no_of_orders else 0 end) as active_current_no_of_orders
    ,sum(case when pax_label = 'Resurrected Pax' then current_no_of_orders else 0 end) as ressurrected_current_no_of_orders
    ,sum(case when pax_label = 'Not Found in past 8 weeks' then current_no_of_orders else 0 end) as unknown_current_no_of_orders

    ,sum(case when pax_label = 'First GF order' then current_gmv_usd else 0 end) as first_gf_order_current_gmv_usd
    ,sum(case when pax_label = 'Active Pax' then current_gmv_usd else 0 end) as active_current_gmv_usd
    ,sum(case when pax_label = 'Resurrected Pax' then current_gmv_usd else 0 end) as ressurrected_current_gmv_usd
    ,sum(case when pax_label = 'Not Found in past 8 weeks' then current_gmv_usd else 0 end) as unknown_current_gmv_usd

    ,sum(case when pax_label = 'First GF order' then current_promo_expense_usd else 0 end) as first_gf_order_current_promo_expense_usd
    ,sum(case when pax_label = 'Active Pax' then current_promo_expense_usd else 0 end) as active_current_promo_expense_usd
    ,sum(case when pax_label = 'Resurrected Pax' then current_promo_expense_usd else 0 end) as ressurrected_current_promo_expense_usd
    ,sum(case when pax_label = 'Not Found in past 8 weeks' then current_promo_expense_usd else 0 end) as unknown_current_promo_expense_usd
, week_of as partition_date
from  int
group by 1,2