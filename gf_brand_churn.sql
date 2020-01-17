with current_week as (
    select 
        bb.*
    from slide.gf_pax_agg bb
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
        past_eight_weeks.passenger_id
        ,past_eight_weeks.business_name
        ,case 
            when date_diff('day',past_eight_weeks.max_date_local, date_trunc('week',date([[inc_start_date]]))) <= 28 then 'Yet to order within 28 days'
            when date_diff('day',past_eight_weeks.max_date_local, date_trunc('week',date([[inc_start_date]]))) > 28 then 'Churn Pax'
            else 'Error' end as pax_label
        ,sum(past_eight_weeks.no_of_orders) as past_no_of_orders
        ,sum(past_eight_weeks.gmv_usd) as past_gmv_usd
        ,sum(past_eight_weeks.promo_expense_usd) as past_promo_expense_usd
    from past_eight_weeks
    left join current_week on current_week.passenger_id = past_eight_weeks.passenger_id and current_week.business_name = past_eight_weeks.business_name
    where current_week.passenger_id is null 
    group by 1,2,3
)
select 
    *
    ,date_trunc('week',date([[inc_start_date]])) AS partition_date
from (
    select 
        business_name
        ,date_trunc('week',date([[inc_start_date]])) AS time_period
        ,count(distinct passenger_id) as distinct_pax_id

        ,sum(case when pax_label = 'Yet to order within 28 days' then 1 else 0 end) as yet_to_order_within_28
        ,sum(case when pax_label = 'Churn Pax' then 1 else 0 end) as churn_pax

        ,sum(case when pax_label = 'Yet to order within 28 days' then past_no_of_orders else 0 end) as yet_to_order_past_no_of_orders
        ,sum(case when pax_label = 'Churn Pax' then past_no_of_orders else 0 end) as churn_pax_past_no_of_orders

        ,sum(case when pax_label = 'Yet to order within 28 days' then past_gmv_usd else 0 end) as yet_to_order_past_gmv_usd
        ,sum(case when pax_label = 'Churn Pax' then past_gmv_usd else 0 end) as churn_pax_past_gmv_usd

        ,sum(case when pax_label = 'Yet to order within 28 days' then past_promo_expense_usd else 0 end) as yet_to_order_past_promo_expense_usd
        ,sum(case when pax_label = 'Churn Pax' then past_promo_expense_usd else 0 end) as churn_pax_past_promo_expense_usd

    from int
    group by 1,2
)