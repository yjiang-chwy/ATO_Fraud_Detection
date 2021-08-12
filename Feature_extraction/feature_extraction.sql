/* Create temp table fo the 14 basic features */
SELECT 
       CASE WHEN T1.DATE IS NOT NULL THEN T1.DATE ELSE T2.DATE END AS DATE, 
       CASE WHEN T1.CUSTOMER_ID IS NOT NULL THEN T1.CUSTOMER_ID ELSE T2.CUSTOMER_ID END AS CUSTOMER_ID, 
       T1.TOTAL_NUM_ORDERS, 
       T1.NUM_ONETIME_ORDERS, 
       T1.DISTINCT_PRODUCT_ORDERED, 
       T1.DISTINCT_PRODUCT_ONETIME_ORDERED,
       T1.ONETIME_ORDER_QUANTITY, 
       T1.TOTAL_ORDER_QUANTITY, 
       T1.TOTAL_ORDER_PRICE,
       T1.ONETIME_ORDER_PRICE,
       T2.DISTINCT_PRODUCT_HIT, 
       T2.TOTAL_PRODUCT_HIT,
       T2.DISTINCT_PRODUCT_CLICKED, 
       T2.TOTAL_PRODUCT_CLICKED, 
       T2.TOTAL_PAGES_VIEWED, 
       T2.DISTINCT_PAGES_VIEWED  
FROM
  (SELECT CUSTOMER_ID, to_char(ORDER_PLACED_DTTM,'YYYY-MM-DD') AS DATE, 
          COUNT(DISTINCT ORDER_ID) AS TOTAL_NUM_ORDERS,
          COUNT(DISTINCT PRODUCT_ID) AS DISTINCT_PRODUCT_ORDERED, 
          SUM(CASE WHEN ORDER_STATUS='D' THEN ORDER_LINE_QUANTITY ELSE 0 END) AS TOTAL_ORDER_QUANTITY, 
          SUM(CASE WHEN ORDER_STATUS='D' THEN ORDER_LINE_TOTAL_PRICE ELSE 0 END) AS TOTAL_ORDER_PRICE,
          COUNT(DISTINCT CASE WHEN NOT ORDER_AUTO_REORDER_FLAG AND ORDER_STATUS='D' THEN ORDER_ID ELSE NULL END) AS NUM_ONETIME_ORDERS,
          COUNT(DISTINCT CASE WHEN NOT ORDER_AUTO_REORDER_FLAG AND ORDER_STATUS='D' THEN PRODUCT_ID ELSE NULL END) DISTINCT_PRODUCT_ONETIME_ORDERED,
          SUM(CASE WHEN NOT ORDER_AUTO_REORDER_FLAG AND ORDER_STATUS='D' THEN ORDER_LINE_QUANTITY ELSE 0 END) ONETIME_ORDER_QUANTITY,
          SUM(CASE WHEN NOT ORDER_AUTO_REORDER_FLAG AND ORDER_STATUS='D' THEN ORDER_LINE_TOTAL_PRICE ELSE 0 END) AS ONETIME_ORDER_PRICE
          
   
   FROM EDLDB.ECOM.ORDER_LINE
   WHERE DATE BETWEEN '2020-12-06' AND '2020-12-12'
   GROUP BY CUSTOMER_ID, DATE) T1
FULL OUTER JOIN
  (SELECT CUSTOMER_ID, to_char(GA_SESSIONS_DATE,'YYYY-MM-DD') AS DATE, 
          COUNT(DISTINCT PRODUCT_ID) AS DISTINCT_PRODUCT_HIT,
          COUNT(PRODUCT_ID) AS TOTAL_PRODUCT_HIT,
          COUNT(DISTINCT CASE WHEN IS_CLICK THEN PRODUCT_ID ELSE NULL END) AS DISTINCT_PRODUCT_CLICKED,
          COUNT(CASE WHEN IS_CLICK THEN PRODUCT_ID ELSE NULL END) AS TOTAL_PRODUCT_CLICKED,
          SUM(CASE TYPE WHEN 'PAGE' THEN 1 ELSE NULL END) AS TOTAL_PAGES_VIEWED,
          COUNT(DISTINCT CASE TYPE WHEN 'PAGE' THEN CONCAT(PAGE_PATH,',',PAGE_TITLE) ELSE NULL END) AS DISTINCT_PAGES_VIEWED
   FROM EDLDB.GA.GA_SESSIONS_HITS_PRODUCTS_UNION
   WHERE DATE BETWEEN '2020-12-06' AND '2020-12-12'
   GROUP BY CUSTOMER_ID, DATE) T2
ON (T1.CUSTOMER_ID = T2.CUSTOMER_ID AND T1.DATE = T2.DATE)  
ORDER BY CUSTOMER_ID
   

/* Create useful ga behavior infos as temp table for later use */
create or replace temporary table ga_behavior as
select customer_id, ga_sessions_date, event_category, event_label, event_action, list_category
from ga.ga_sessions_hits_products_union
where customer_id is not null
and customer_id != 0
and ga_sessions_date between '2019-07-01' and '2021-05-31'
and is_interaction

/* Create useful product click infos as temp table for later use */
create or replace temporary table product_click as
select customer_id, ga_sessions_date, product_id, product_category_level1, product_category_level2
from ga.ga_sessions_hits_products
where ga_sessions_date between '2019-07-01' and '2021-05-31'
and event_action = 'productClick'
and customer_id is not null 
and customer_id != 0;

/* Create useful orders infos as temp table for later use */
create or replace temporary table order_info as
select t1.customer_id, date(t1.order_placed_dttm) as date, t1.product_id, t2.category_level1, t2.category_level2
from(
  select customer_id, order_placed_dttm, product_id
  from ecom.order_line
  where order_placed_dttm between '2019-07-01' and '2021-05-31'
  and product_id is not null
  and customer_id is not null
  and customer_id != 0
)as t1
left join(
  select product_id, category_level1, category_level2
  from pdm.product
) as t2
on t1.product_id = t2.product_id;

/* Create useful city location infos as temp table for later use */
create or replace temporary table city_info as 
select customer_id, ga_sessions_date, geo_network:city as city
from ga.ga_sessions
where ga_sessions_date between '2019-07-01' and '2021-05-31'
and customer_id is not null
and customer_id != 0;

/* create city related features as city_features table*/
create or replace temporary table city_features as(
with prev_city as(
 select 
  t3.customer_id, 
  t3.ga_sessions_date, 
  count(distinct t4.city) as dinstinct_prev_city_cnts
 from(
  select 
   customer_id, 
   ga_sessions_date
  from ga.ga_sessions
  where ga_sessions_date between '2020-12-06' and '2021-05-31'
  and customer_id is not null
  and customer_id != 0
 ) as t3
 left join(
  select 
   customer_id, 
   ga_sessions_date, 
   city
  from city_info
 ) as t4
 on t3.customer_id = t4.customer_id
 and t3.ga_sessions_date > t4.ga_sessions_date
 group by t3.customer_id, t3.ga_sessions_date
),
/* column for distinct cities, new city conts and not set daily*/
daily_city as(
 select 
  t1.customer_id, 
  t1.ga_sessions_date, 
  count(distinct t1.city) as distinct_city_daily_cnts,
  count(distinct case when t2.city is null
       then t1.city
       else null
       end) as distinct_new_city_cnts,
  sum(case when t1.city = '(not set)'
     then 1
     else 0
     end) as city_not_set_flag
 from(
  select 
   customer_id, 
   ga_sessions_date,
   city
  from city_info
  where ga_sessions_date between '2020-12-06' and '2021-05-31'
 ) as t1
 left join(
  select 
   customer_id, 
   ga_sessions_date, 
   city
  from city_info
 ) as t2
 on t1.customer_id = t2.customer_id
 and t1.ga_sessions_date > t2.ga_sessions_date
 and t1.city = t2.city
 group by t1.customer_id, t1.ga_sessions_date
)
 /* join prev_city and daily_city columns for all city related features */
select 
 t1.customer_id,
 t1.ga_sessions_date, 
 t1.dinstinct_prev_city_cnts,
 t2.distinct_city_daily_cnts,
 t2.distinct_new_city_cnts,
 case when t2.city_not_set_flag > 0
  then 1
  else 0
 end as city_not_set_flag
from(
  select *
  from prev_city) as t1
left join(
  select *
  from daily_city) as t2
on t1.customer_id = t2.customer_id
and t1.ga_sessions_date = t2.ga_sessions_date
)

/* columns for product_click features*/
create or replace temporary table product_click_feature as(
  with prev_product_click as(
    select
     t1.customer_id,
     t1.ga_sessions_date,
     count(distinct t2.product_id) as dinstinct_prev_products_clicked_cnts,
     count(distinct t2.product_category_level1) as dinstinct_prev_products_type1_clicked_cnts,
     count(distinct t2.product_category_level1, t2.product_category_level2) as dinstinct_prev_products_type2_clicked_cnts
    from(
      select
       customer_id,
       ga_sessions_date
      from ga.ga_sessions
      where ga_sessions_date between '2020-12-06' and '2021-05-31'
      and customer_id is not null
      and customer_id != 0) as t1
    left join(
      select *
      from product_click) as t2
    on t1.customer_id = t2.customer_id
    and t1.ga_sessions_date > t2.ga_sessions_date
    group by t1.customer_id, t1.ga_sessions_date),
  
  daily_new_product_click as(
     select 
      t1.customer_id, 
      t1.ga_sessions_date, 
      count(distinct case when t2.product_id is null
       then t1.product_id
       else null
       end) as distinct_new_product_clicked_cnts
     from(
      select 
       customer_id, 
       ga_sessions_date,
       product_id
      from product_click
      where ga_sessions_date between '2020-12-06' and '2021-05-31'
     ) as t1
     left join(
      select 
       customer_id, 
       ga_sessions_date, 
       product_id
      from product_click
     ) as t2
     on t1.customer_id = t2.customer_id
     and t1.ga_sessions_date > t2.ga_sessions_date
     and t1.product_id = t2.product_id
     group by t1.customer_id, t1.ga_sessions_date),
  
  daily_new_product_type1_click as(
     select 
      t1.customer_id, 
      t1.ga_sessions_date, 
      count(distinct case when t2.type1 is null
       then t1.type1
       else null
       end) as distinct_new_product_type1_clicked_cnts
     from(
      select 
       customer_id, 
       ga_sessions_date,
       product_category_level1 as type1
      from product_click
      where ga_sessions_date between '2020-12-06' and '2021-05-31'
     ) as t1
     left join(
      select 
       customer_id, 
       ga_sessions_date, 
       product_category_level1 as type1
      from product_click
     ) as t2
     on t1.customer_id = t2.customer_id
     and t1.ga_sessions_date > t2.ga_sessions_date
     and t1.type1 = t2.type1
     group by t1.customer_id, t1.ga_sessions_date),
  
  daily_new_product_type2_click as(
     select 
      t1.customer_id, 
      t1.ga_sessions_date, 
      count(distinct case when t2.type2 is null
       then t1.type2
       else null
       end) as distinct_new_product_type2_clicked_cnts
     from(
      select 
       customer_id, 
       ga_sessions_date,
       concat(product_category_level1, product_category_level2) as type2
      from product_click
      where ga_sessions_date between '2020-12-06' and '2021-05-31'
     ) as t1
     left join(
      select 
       customer_id, 
       ga_sessions_date, 
       concat(product_category_level1, product_category_level2) as type2
      from product_click
     ) as t2
     on t1.customer_id = t2.customer_id
     and t1.ga_sessions_date > t2.ga_sessions_date
     and t1.type2 = t2.type2
     group by t1.customer_id, t1.ga_sessions_date)

select 
 t1.customer_id,
 t1.ga_sessions_date, 
 t1.dinstinct_prev_products_clicked_cnts,
 t1.dinstinct_prev_products_type1_clicked_cnts,
 t1.dinstinct_prev_products_type2_clicked_cnts,
 t2.distinct_new_product_clicked_cnts,
 t3.distinct_new_product_type1_clicked_cnts,
 t4.distinct_new_product_type2_clicked_cnts
from prev_product_click t1
left join daily_new_product_click t2
on t1.customer_id = t2.customer_id
and t1.ga_sessions_date = t2.ga_sessions_date
left join daily_new_product_type1_click t3
on t2.customer_id = t3.customer_id
and t2.ga_sessions_date = t3.ga_sessions_date
left join daily_new_product_type2_click t4
on t3.customer_id = t4.customer_id
and t3.ga_sessions_date = t4.ga_sessions_date
)
    
/* columns for product_purchase features*/
create or replace temporary table product_purchase_feature as(
  with prev_product_purchased as(
    select
     t1.customer_id,
     t1.ga_sessions_date,
     count(distinct t2.product_id) as dinstinct_prev_products_purchased_cnts,
     count(distinct t2.category_level1) as dinstinct_prev_products_type1_purchased_cnts,
     count(distinct t2.category_level1, t2.category_level2) as dinstinct_prev_products_type2_purchased_cnts
    from(
      select
       customer_id,
       ga_sessions_date
      from ga.ga_sessions
      where ga_sessions_date between '2020-12-06' and '2021-05-31'
      and customer_id is not null
      and customer_id != 0) as t1
    left join(
      select *
      from order_info) as t2
    on t1.customer_id = t2.customer_id
    and t1.ga_sessions_date > t2.date
    group by t1.customer_id, t1.ga_sessions_date),
  
  daily_new_product_purchased as(
     select 
      t1.customer_id, 
      t1.date,
      count(distinct case when t2.product_id is null
       then t1.product_id
       else null
       end) as distinct_new_product_purchased_cnts
     from(
      select 
       customer_id, 
       date,
       product_id
      from order_info
      where date between '2020-12-06' and '2021-05-31'
     ) as t1
     left join(
      select 
       customer_id, 
       date, 
       product_id
      from order_info
     ) as t2
     on t1.customer_id = t2.customer_id
     and t1.date > t2.date
     and t1.product_id = t2.product_id
     group by t1.customer_id, t1.date),
  
  daily_new_product_type1_purchased as(
     select 
      t1.customer_id, 
      t1.date, 
      count(distinct case when t2.type1 is null
       then t1.type1
       else null
       end) as distinct_new_product_type1_purchased_cnts
     from(
      select 
       customer_id, 
       date,
       category_level1 as type1
      from order_info
      where date between '2020-12-06' and '2021-05-31'
     ) as t1
     left join(
      select 
       customer_id, 
       date, 
       category_level1 as type1
      from order_info
     ) as t2
     on t1.customer_id = t2.customer_id
     and t1.date > t2.date
     and t1.type1 = t2.type1
     group by t1.customer_id, t1.date),
  
  daily_new_product_type2_purchased as(
     select 
      t1.customer_id, 
      t1.date, 
      count(distinct case when t2.type2 is null
       then t1.type2
       else null
       end) as distinct_new_product_type2_purchased_cnts
     from(
      select 
       customer_id, 
       date,
       concat(category_level1, category_level2) as type2
      from order_info
      where date between '2020-12-06' and '2021-05-31'
     ) as t1
     left join(
      select 
       customer_id, 
       date, 
       concat(category_level1, category_level2) as type2
      from order_info
     ) as t2
     on t1.customer_id = t2.customer_id
     and t1.date > t2.date
     and t1.type2 = t2.type2
     group by t1.customer_id, t1.date)

select 
 t1.customer_id,
 t1.ga_sessions_date, 
 t1.dinstinct_prev_products_purchased_cnts,
 t1.dinstinct_prev_products_type1_purchased_cnts,
 t1.dinstinct_prev_products_type2_purchased_cnts,
 t2.distinct_new_product_purchased_cnts,
 t3.distinct_new_product_type1_purchased_cnts,
 t4.distinct_new_product_type2_purchased_cnts
from prev_product_purchased t1
left join daily_new_product_purchased t2
on t1.customer_id = t2.customer_id
and t1.ga_sessions_date = t2.date
left join daily_new_product_type1_purchased t3
on t2.customer_id = t3.customer_id
and t2.date = t3.date
left join daily_new_product_type2_purchased t4
on t3.customer_id = t4.customer_id
and t3.date = t4.date
)

/* Columns for customer behavior features*/
create or replace temporary table ga_behavior_features as (
  with ga_behavior_raw as (
    select customer_id, ga_sessions_date, 
     case when list_category='reset-password' 
      then 1 else 0 end as reset_password,
     case when event_category='address book' and event_action='clicked add' 
      then 1 else 0 end as add_new_address,
     case when event_action='gift-card-applied'
      then 1 else 0 end as gift_card_applied,
     case when event_action='add-payment-method'
      then 1 else 0 end as add_payment_method,
     case when  event_action='autoship-change-frequency-increment'
      then 1 else 0 end as change_autoship_frequency,
     case when event_action='ship-now' and event_label='done'
      then 1 else 0 end as autoship_ship_now   
    from ga_behavior
    where ga_sessions_date between '2020-12-06' and '2021-05-31'
  )
  select
    customer_id, ga_sessions_date,
    max(reset_password) as has_reset_password_flag,
    max(add_new_address) as has_add_new_address_flag,
    max(gift_card_applied) as has_applied_giftcard_flag,
    max(add_payment_method) as has_add_payment_flag,
    max(change_autoship_frequency) as has_change_autoship_frequency_flag,
    max(autoship_ship_now) as has_autoship_ship_now_flag
  from ga_behavior_raw
  group by customer_id, ga_sessions_date
)

select *
from PRODUCT_PURCHASE_FEATURE
where distinct_new_product_purchased_cnts is not null

select *
from PRODUCT_CLICK_FEATURE
where distinct_new_product_clicked_cnts is not null

with base as (
  select customer_id, ga_sessions_date, case when event_category='address book' and event_action='clicked add' 
  then 1 else 0 end as add_new_address
  from ga_behavior
  where ga_sessions_date between '2020-12-06' and '2021-05-31')
 
select customer_id, ga_sessions_date, max(add_new_address) as add_new_address_flag
from base
group by (customer_id, ga_sessions_date)
order by add_new_address_flag desc

select *
from ga_behavior
where event_action='gift_card_applied'


select *
from ga_behavior
where event_action='ship-now'
and event_label='done'

create or replace temporary table new_features_set as(
 select
  t1.customer_id,
  t1.ga_sessions_date,
  t1.DINSTINCT_PREV_CITY_CNTS,
  t1.DISTINCT_CITY_DAILY_CNTS,
  t1.DISTINCT_NEW_CITY_CNTS,
  t1.CITY_NOT_SET_FLAG,
  t2.DINSTINCT_PREV_PRODUCTS_CLICKED_CNTS AS DISTINCT_PREV_PRODUCTS_CLICKED_CNTS,
  t2.DINSTINCT_PREV_PRODUCTS_TYPE1_CLICKED_CNTS AS DISTINCT_PREV_PRODUCTS_TYPE1_CLICKED_CNTS,
  t2.DINSTINCT_PREV_PRODUCTS_TYPE2_CLICKED_CNTS AS DISTINCT_PREV_PRODUCTS_TYPE2_CLICKED_CNTS,
  t2.DISTINCT_NEW_PRODUCT_CLICKED_CNTS,
  t2.DISTINCT_NEW_PRODUCT_TYPE1_CLICKED_CNTS,
  t2.DISTINCT_NEW_PRODUCT_TYPE2_CLICKED_CNTS,
  t3.DINSTINCT_PREV_PRODUCTS_PURCHASED_CNTS as DISTINCT_PREV_PRODUCTS_PURCHASED_CNT,
  t3.DINSTINCT_PREV_PRODUCTS_TYPE1_PURCHASED_CNTS as DISTINCT_PREV_PRODUCTS_TYPE1_PURCHASED_CNTS,
  t3.DINSTINCT_PREV_PRODUCTS_TYPE2_PURCHASED_CNTS as DISTINCT_PREV_PRODUCTS_TYPE2_PURCHASED_CNTS,
  t3.DISTINCT_NEW_PRODUCT_PURCHASED_CNTS,
  t3.DISTINCT_NEW_PRODUCT_TYPE1_PURCHASED_CNTS,
  t3.DISTINCT_NEW_PRODUCT_TYPE2_PURCHASED_CNTS,
  t4.HAS_RESET_PASSWORD_FLAG,
  t4.HAS_ADD_NEW_ADDRESS_FLAG,
  t4.HAS_APPLIED_GIFTCARD_FLAG,
  t4.HAS_CHANGE_AUTOSHIP_FREQUENCY_FLAG,
  t4.HAS_AUTOSHIP_SHIP_NOW_FLAG,
  t4.HAS_ADD_PAYMENT_FLAG
 from city_features t1
 left join product_click_feature t2
 on t1.customer_id = t2.customer_id
 and t1.ga_sessions_date = t2.ga_sessions_date
 left join product_purchase_feature t3
 on t2.customer_id = t3.customer_id
 and t2.ga_sessions_date = t3.ga_sessions_date
 left join ga_behavior_features t4
 on t3.customer_id = t4.customer_id
 and t3.ga_sessions_date = t4.ga_sessions_date
)

/* read old features set from s3 bucket*/
create or replace temporary table old_features_set(
  num0 number,
  date date, 
  customer_id number,
  TOTAL_NUM_ORDERS number,
  NUM_CANCELLED_ORDERS number,
  NUM_ONETIME_ORDERS number,
  DISTINCT_PRODUCT_ORDERED number,
  DISTINCT_PRODUCT_ONETIME_ORDERED number,
  ONETIME_ORDER_QUANTITY number,
  TOTAL_ORDER_QUANTITY number,
  TOTAL_ORDER_PRICE number,
  ONETIME_ORDER_PRICE number,
  DISTINCT_PRODUCT_HIT number,
  TOTAL_PRODUCT_HIT number,
  DISTINCT_PRODUCT_CLICKED number,
  TOTAL_PRODUCT_CLICKED number,
  TOTAL_PAGES_VIEWED number,
  DISTINCT_PAGES_VIEWED number
)

copy into old_features_set
from s3://fraud-user-profile-sandbox/dataset_all.csv
credentials=(AWS_KEY_ID='',
AWS_SECRET_KEY='')
file_format = (type = csv field_delimiter = ',' skip_header = 1)

/* merge the old feature columns with the new feature columns*/
create or replace temporary table ato_features_v2 as(
  select
   t1.customer_id,
   t1.ga_sessions_date,
   t1.DINSTINCT_PREV_CITY_CNTS,
   t1.DISTINCT_CITY_DAILY_CNTS,
   t1.DISTINCT_NEW_CITY_CNTS,
   t1.CITY_NOT_SET_FLAG,
   t1.DISTINCT_PREV_PRODUCTS_CLICKED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_TYPE1_CLICKED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_TYPE2_CLICKED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_CLICKED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE1_CLICKED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE2_CLICKED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_PURCHASED_CNT,
   t1.DISTINCT_PREV_PRODUCTS_TYPE1_PURCHASED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_TYPE2_PURCHASED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_PURCHASED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE1_PURCHASED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE2_PURCHASED_CNTS,
   t1.HAS_RESET_PASSWORD_FLAG,
   t1.HAS_ADD_NEW_ADDRESS_FLAG,
   t1.HAS_APPLIED_GIFTCARD_FLAG,
   t1.HAS_CHANGE_AUTOSHIP_FREQUENCY_FLAG,
   t1.HAS_AUTOSHIP_SHIP_NOW_FLAG,
   t1.HAS_ADD_PAYMENT_FLAG,
   t2.TOTAL_NUM_ORDERS ,
   t2.NUM_CANCELLED_ORDERS,
   t2.NUM_ONETIME_ORDERS,
   t2.DISTINCT_PRODUCT_ORDERED,
   t2.DISTINCT_PRODUCT_ONETIME_ORDERED,
   t2.ONETIME_ORDER_QUANTITY,
   t2.TOTAL_ORDER_QUANTITY,
   t2.TOTAL_ORDER_PRICE,
   t2.ONETIME_ORDER_PRICE,
   t2.DISTINCT_PRODUCT_HIT,
   t2.TOTAL_PRODUCT_HIT,
   t2.DISTINCT_PRODUCT_CLICKED,
   t2.TOTAL_PRODUCT_CLICKED,
   t2.TOTAL_PAGES_VIEWED,
   t2.DISTINCT_PAGES_VIEWED
  from new_features_set t1
  left join old_features_set t2
  on t1.customer_id = t2.customer_id
  and t1.ga_sessions_date = t2.date
)

/* upload the enhanced dataset into s3 bucket*/
COPY INTO ''
from (select *
      from ATO_FEATURES_V2)
credentials=(AWS_KEY_ID='',
AWS_SECRET_KEY='',
AWS_TOKEN='')
header=True
single=False
max_file_size=5000000000
file_format=(TYPE=CSV, NULL_IF=('NaN', 'NULL'), COMPRESSION=None);

/* merge the ato_cases with the enhanced dataset to get the labelled dataset for ATO */
create or replace temporary table ato_fraud_orders(
  order_id number, 
  customer_id number
)
copy into ato_fraud_orders
from s3://fraud-user-profile-sandbox/Labelled_data/ato_orders.csv
credentials=(AWS_KEY_ID='',
AWS_SECRET_KEY='',
AWS_TOKEN='')
file_format = (type = csv field_delimiter = '\t' skip_header = 1 error_on_column_count_mismatch=false)

create or replace temporary table ato_fraud_data as(
  select 
   t1.order_id,
   t1.customer_id,
   t2.date
  from(
    select 
     order_id,
     customer_id
    from ato_fraud_orders
  ) t1
  inner join(
    select 
     order_id,
     date(order_placed_dttm) as date
    from ecom.orders
    where order_placed_dttm between '2020-12-06' and '2021-05-29' 
  ) t2
  on t1.order_id = t2.order_id
)
/* remove duplicate rows from ato_fraud_data */
create or replace temporary table ato_fraud_data as(
 select *
  from(
   select customer_id, date, max(order_id)
   from ato_fraud_data
   group by customer_id, date)
)  

create or replace temporary table ato_fraud_data as(
  select 
   t1.customer_id,
   t1.ga_sessions_date,
   t1.DINSTINCT_PREV_CITY_CNTS,
   t1.DISTINCT_CITY_DAILY_CNTS,
   t1.DISTINCT_NEW_CITY_CNTS,
   t1.CITY_NOT_SET_FLAG,
   t1.DISTINCT_PREV_PRODUCTS_CLICKED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_TYPE1_CLICKED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_TYPE2_CLICKED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_CLICKED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE1_CLICKED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE2_CLICKED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_PURCHASED_CNT,
   t1.DISTINCT_PREV_PRODUCTS_TYPE1_PURCHASED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_TYPE2_PURCHASED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_PURCHASED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE1_PURCHASED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE2_PURCHASED_CNTS,
   t1.HAS_RESET_PASSWORD_FLAG,
   t1.HAS_ADD_NEW_ADDRESS_FLAG,
   t1.HAS_APPLIED_GIFTCARD_FLAG,
   t1.HAS_CHANGE_AUTOSHIP_FREQUENCY_FLAG,
   t1.HAS_AUTOSHIP_SHIP_NOW_FLAG,
   t1.HAS_ADD_PAYMENT_FLAG,
   t1.TOTAL_NUM_ORDERS ,
   t1.NUM_CANCELLED_ORDERS,
   t1.NUM_ONETIME_ORDERS,
   t1.DISTINCT_PRODUCT_ORDERED,
   t1.DISTINCT_PRODUCT_ONETIME_ORDERED,
   t1.ONETIME_ORDER_QUANTITY,
   t1.TOTAL_ORDER_QUANTITY,
   t1.TOTAL_ORDER_PRICE,
   t1.ONETIME_ORDER_PRICE,
   t1.DISTINCT_PRODUCT_HIT,
   t1.TOTAL_PRODUCT_HIT,
   t1.DISTINCT_PRODUCT_CLICKED,
   t1.TOTAL_PRODUCT_CLICKED,
   t1.TOTAL_PAGES_VIEWED,
   t1.DISTINCT_PAGES_VIEWED
  from ATO_FEATURES_V2 t1
  INNER JOIN ato_fraud_data t2
  on t1.customer_id = t2.customer_id
  and t1.ga_sessions_date = t2.date
)

/* upload the ato_fraud_cases into s3 bucket */
COPY INTO ''
from (select *
      from ATO_fRAUD_DATA)
credentials=(AWS_KEY_ID='',
AWS_SECRET_KEY='',
AWS_TOKEN='')
header=True
single=True
max_file_size=5000000000
file_format=(TYPE=CSV, NULL_IF=('NaN', 'NULL'), COMPRESSION=None);

/* merge the all type of frauds cases with the enhanced dataset to get the labelled dataset for all types of frauds */
create temporary table all_fraud_orders(order_id number, fraud_flag number, fraud_return number, chargeback_flag number)

copy into all_fraud_orders
from s3://emr-fraud-data-science-sandbox/input/fraud_orders.csv
credentials=(AWS_KEY_ID='',
AWS_SECRET_KEY='',
AWS_TOKEN='')
file_format = (type = csv field_delimiter = ',' skip_header = 1)

create or replace temporary table all_fraud_data as(
  select 
   t1.order_id,
   t2.customer_id,
   t2.date
  from(
    select 
     order_id
    from all_fraud_orders
  ) t1
  inner join(
    select 
     order_id,
     customer_id,
     date(order_placed_dttm) as date
    from ecom.orders
    where order_placed_dttm between '2020-12-06' and '2021-05-29' 
  ) t2
  on t1.order_id = t2.order_id
)
/* remove duplicate rows from all_frauds_data */
create or replace temporary table all_fraud_data as(
 select *
  from(
   select customer_id, date, max(order_id)
   from all_fraud_data
   group by customer_id, date)
)  

create or replace temporary table all_fraud_data as(
  select 
   t1.customer_id,
   t1.ga_sessions_date,
   t1.DINSTINCT_PREV_CITY_CNTS,
   t1.DISTINCT_CITY_DAILY_CNTS,
   t1.DISTINCT_NEW_CITY_CNTS,
   t1.CITY_NOT_SET_FLAG,
   t1.DISTINCT_PREV_PRODUCTS_CLICKED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_TYPE1_CLICKED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_TYPE2_CLICKED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_CLICKED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE1_CLICKED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE2_CLICKED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_PURCHASED_CNT,
   t1.DISTINCT_PREV_PRODUCTS_TYPE1_PURCHASED_CNTS,
   t1.DISTINCT_PREV_PRODUCTS_TYPE2_PURCHASED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_PURCHASED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE1_PURCHASED_CNTS,
   t1.DISTINCT_NEW_PRODUCT_TYPE2_PURCHASED_CNTS,
   t1.HAS_RESET_PASSWORD_FLAG,
   t1.HAS_ADD_NEW_ADDRESS_FLAG,
   t1.HAS_APPLIED_GIFTCARD_FLAG,
   t1.HAS_CHANGE_AUTOSHIP_FREQUENCY_FLAG,
   t1.HAS_AUTOSHIP_SHIP_NOW_FLAG,
   t1.HAS_ADD_PAYMENT_FLAG,
   t1.TOTAL_NUM_ORDERS ,
   t1.NUM_CANCELLED_ORDERS,
   t1.NUM_ONETIME_ORDERS,
   t1.DISTINCT_PRODUCT_ORDERED,
   t1.DISTINCT_PRODUCT_ONETIME_ORDERED,
   t1.ONETIME_ORDER_QUANTITY,
   t1.TOTAL_ORDER_QUANTITY,
   t1.TOTAL_ORDER_PRICE,
   t1.ONETIME_ORDER_PRICE,
   t1.DISTINCT_PRODUCT_HIT,
   t1.TOTAL_PRODUCT_HIT,
   t1.DISTINCT_PRODUCT_CLICKED,
   t1.TOTAL_PRODUCT_CLICKED,
   t1.TOTAL_PAGES_VIEWED,
   t1.DISTINCT_PAGES_VIEWED
  from ATO_FEATURES_V2 t1
  INNER JOIN all_fraud_data t2
  on t1.customer_id = t2.customer_id
  and t1.ga_sessions_date = t2.date
)

/* upload the ato_fraud_cases into s3 bucket */
COPY INTO 's3://fraud-user-profile-sandbox/Labelled_data/all_fraud_data.csv'
from (select *
      from ALL_fRAUD_DATA)
credentials=(AWS_KEY_ID='',
AWS_SECRET_KEY='C',
AWS_TOKEN='I')
header=True
single=True
max_file_size=5000000000
file_format=(TYPE=CSV, NULL_IF=('NaN', 'NULL'), COMPRESSION=None);
