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
  ,if(cnt_listings is not null, 1, 0) as list_flag
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
  ,count(distinct case when list_flag =1 then user_id else null end) as list_uu
from
  dmart
group by
  variant
order by
  variant
