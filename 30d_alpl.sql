with target as (
select
  user_id
  ,variant
from
  `mercari-analytics-jp.z_shundai.pre_hl_20191203`
)

,listing as (
select
  seller_id
  ,count(1) as cnt_listings
from
  `mercari-anondb-jp-prod.anon_jp.items`
where
  timestamp_add(created, interval 9 hour) between '2019-12-04 00:00:00' and '2020-01-03 23:59:59'
group by
  seller_id
)

,dmart as (
select
  target.user_id 
  ,variant
  ,if(cnt_listings is not null, cnt_listings, 0) as cnt_listing
from
  target
left join
  listing
on
  target.user_id = listing.seller_id
)

select
  variant
  ,count(distinct user_id) as cnt_uu
  ,avg(cnt_listing) as avg_list
  ,var_samp(cnt_listing) as var_list
from
  dmart
group by
  variant
order by
  variant
