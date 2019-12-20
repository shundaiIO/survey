#standardSQL
with item_rank as (----過去の出品物とその出品順
select
    seller_id
    ,id as item_id
    ,category_id
    ,brand_name
    ,timestamp_add(created, interval 9 hour) as created
    ,rank() over (partition by seller_id order by created) as rank
from
    `mercari-anondb-jp-prod.anon_jp.items`
)

,nov_first_list as (---11月23~12月2に初出品した人,売れていない人
select
    item_rank.seller_id
    ,item_rank.item_id
    ,if(te.item_id is not null,1,0) as sold_flag
    ,item_rank.category_id
    ,item_rank.brand_name
    ,rank 
from
    item_rank
left join
    `mercari-anondb-jp-prod.anon_jp.transaction_evidences` as te 
on
    item_rank.item_id = te.item_id
where
    (item_rank.created between '2019-11-01 00:00:00' and '2019-11-30 23:59:59') and (rank = 1) and (te.item_id is null)
)

,life_time_list as (---通算出品数が１つだけ
select
    seller_id
    ,count(1) as cnt_listings
from
    `mercari-anondb-jp-prod.anon_jp.items`
where
    timestamp_add(created, interval 9 hour) <= '2019-12-07 23:59:59'
group by
    seller_id
having
    cnt_listings = 1
)

,category as (---カテゴリマップ
  select
    c1.id as category_id
    ,case 
       when c1.level = 0 then c1.id
       when c1.level = 1 then c2.id
       when c1.level = 2 then c3.id 
       else null 
       end as category_id_level1
    ,case 
       when c1.level = 0 then c1.name
       when c1.level = 1 then c2.name
       when c1.level = 2 then c3.name 
       else null 
       end as category_level1 -- 第1階層カテゴリ 
    ,case 
       when c1.level = 0 then null
       when c1.level = 1 then c1.name
       when c1.level = 2 then c2.name 
       else null 
       end as category_level2 --第2階層カテゴリ ： 第1階層のレベルのものに関してはNullになる
    ,case 
       when c1.level = 0 then null
       when c1.level = 1 then null
       when c1.level = 2 then c1.name 
       else null 
       end as category_level3 -- 第3階層カテゴリ ： 第2階層以上のレベルのものに関してはNullになる
  from 
    `mercari-anondb-jp-prod.anon_jp.item_categories` as c1
  left join 
    `mercari-anondb-jp-prod.anon_jp.item_categories` as c2 
  on  c1.parent_id = c2.id
  left join 
    `mercari-anondb-jp-prod.anon_jp.item_categories` as c3 
  on  c2.parent_id = c3.id
  )

, raw_birthday as (
select
  ans.user_id
  ,ans.birthday
  ,updated
  ,row_number() OVER(PARTITION BY user_id order by updated desc) as row_num
from
  `mercari-anondb-jp-prod.anon_jp.anti_social` as ans
)

, birthdays as (
select
  user_id
  ,birthday
  ,date_diff(current_date(), cast(birthday as date), year) as age
from raw_birthday
where row_num = 1
)

,first_list_master as (---初出品がメンズ・レディスのノーブランドで売れなかった人
select
    seller_id
    ,item_id
    ,name_ja
    ,category_level1
from
    nov_first_list 
left join
    category
on
    nov_first_list.category_id = category.category_id
left join
  `mercari-anondb-jp-prod.anon_jp.item_brands` as brand
on
    nov_first_list.brand_name = brand.id
where
    (name_ja is null) and (category_level1 in ('メンズ','レディース'))
)

,master as (--1人に付きたくさんの出品があり得る、1:Nの関係
select
    first_list_master.seller_id
    ,first_list_master.category_level1 as first_category_level1
    ,if(life_time_list.seller_id is not null,1, 0) as list_once_flag
    ,case
        when age between 10 and 19 then 10
        when age between 20 and 29 then 20
        when age between 30 and 39 then 30
        when age >= 40 then 40
        else null
    end as age_range
from
    first_list_master
left join
    life_time_list
on
    first_list_master.seller_id = life_time_list.seller_id
left join
    birthdays
on
    first_list_master.seller_id = birthdays.user_id
)


,dmart as (
select
    master.seller_id as user_id
from
    master
where
    (age_range = 40) and first_category_level1 = 'レディース' and list_once_flag = 1
)

---ここから以下ABのVariantの振り分け
,var as ( 
    select 
        cast(format_date("%y%m%d", current_date("Asia/Tokyo")) as int64) as num -- 毎日入れ替わる
        ,cast(format_date("%y%m", current_date("Asia/Tokyo")) as int64) as num2  -- 毎月入れ替わる
)


,ab as (---abテスト用ランダマイズ
select
    user_id
    ,case
        when fp between 0 and 49 then 'control'
        when fp between 50 and 99 then 'test'
        else null
    end as variant
from (
select
    user_id
    ,mod(abs(farm_fingerprint(cast(user_id - (select num from var) + (select num2 from var) as string))),100) as fp
from
    dmart
)
)

select
    user_id
    ,variant
from
    ab