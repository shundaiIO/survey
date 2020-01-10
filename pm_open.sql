with target as (
select
  user_id
from
  `mercari-analytics-jp.z_shundai.pre_hl_20191203`
where
  variant = 'test'
)

,pm_open as (
SELECT distinct user_id FROM `mercari-bigquery-jp-prod.pascal_event_log.event_log_*`
WHERE _TABLE_SUFFIX >= '20191203' 
and event_id = 'notify_tap'　---notify tapだとPM開封率
and prop like '%CRE_20191204_LP_MJP-44905%' ---tracking idを指定
)

,dmart as (
select
  target.user_id
  ,if(pm_open.user_id is not null, 1, 0) as pm_open_flag
from
  target
left join
  pm_open
on
  target.user_id = pm_open.user_id
)

select
  count(distinct user_id) as cnt_uu
  ,count(distinct case when pm_open_flag = 1 then user_id else null end) as cnt_open_uu
from
  dmart
