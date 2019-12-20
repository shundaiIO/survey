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
  timestamp_add(created, interval 9 hour) between '2019-07-20 00:00:00' and '2019-10-19 23:59:59' 
group by
  seller_id
having
  cnt_listings >= 6
)

,sold as (
select
  seller_id
  ,count(1) as cnt_sold
from
  `mercari-anondb-jp-prod.anon_jp.transaction_evidences`
where
  created between '2019-07-20 00:00:00' and '2019-10-19 23:59:59'
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
  (cnt_listings >=6) and (cnt_sold >= 2)
)

,after_listings as (
select
  seller_id
  ,count(1) as cnt_listings
from
  `mercari-anondb-jp-prod.anon_jp.items`
where
  timestamp_add(created, interval 9 hour) between '2019-10-20 00:00:00' and '2019-11-19 23:59:59'
group by
  seller_id
)


select
  count(distinct user_id) as target_uu
  ,count(distinct case when after_listings.cnt_listings >= 2 then user_id else null end) as hl_uu
from
  dmart
left join
  after_listings
on
  dmart.user_id = after_listings.seller_id