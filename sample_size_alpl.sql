with register as (
select
  distinct id as user_id
from
  `mercari-anondb-jp-prod.anon_jp.users`
)

,list as (
select
  seller_id
  ,count(1) as cnt_listings
from
  `mercari-anondb-jp-prod.anon_jp.items`
where
  timestamp_add(created, interval 9 hour) between '2019-06-01 00:00:00' and '2019-08-31 23:59:59' 
group by
  seller_id
having
  cnt_listings >=6
)

,sold as (
select
  seller_id
  ,count(1) as cnt_sold
from
  `mercari-anondb-jp-prod.anon_jp.transaction_evidences`
where
  created between '2019-06-01 00:00:00' and '2019-08-31 23:59:59'
group by
  seller_id
having
  cnt_sold >= 2
)

,dmart as (
select
  register.user_id
  ,list.cnt_listings
  ,sold.cnt_sold
from
  register
left join
  list
on
  register.user_id = list.seller_id
left join
  sold
on
  register.user_id = sold.seller_id
where
  (cnt_listings between 6 and 50) and (cnt_sold >= 2)
)

,after_listings as (
select
  seller_id
  ,count(1) as cnt_listings
from
  `mercari-anondb-jp-prod.anon_jp.items`
where
  timestamp_add(created, interval 9 hour) between '2019-09-01 00:00:00' and '2019-11-30 23:59:59'
group by
  seller_id
)

,list_count as (
select
 dmart.user_id
 ,if(after_listings.cnt_listings is not null, after_listings.cnt_listings,0) as cnt_list
from
  dmart
left join
  after_listings
on
  dmart.user_id = after_listings.seller_id
)

select
  count(distinct user_id) as cnt_uu
  ,sum(cnt_list) as cnt_list
  ,sum(cnt_list)/count(distinct user_id) as avg_list
  ,avg(cnt_list) as avg_list_conf
  ,sqrt((1/count(distinct user_id)) * sum(pow(cnt_list, 2)) - pow((1/count(distinct user_id)) * sum(cnt_list), 2)) as std_list
  ,stddev(cnt_list) as std_list_conf
from
  list_count
