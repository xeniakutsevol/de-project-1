-- Исследование и проверки данных
select
min(order_ts) as min_date,
max(order_ts) as max_date,
count(case when order_ts is null then 1 end) as nulls_cnt
from production.orders;

select
min(payment) as min_payment,
max(payment) as max_payment,
count(case when payment is null then 1 end) as nulls_cnt
from production.orders;

select
o.status,
s.key,
count(o.*) cnt
from production.orders o
left join production.orderstatuses s
on o.status=s.id
group by o.status, s.key
order by o.status;

-- Создание представлений схемы production
create view analysis.orders as select * from production.orders;
create view analysis.orderstatuses as select * from production.orderstatuses;
create view analysis.orderstatuslog as select * from production.orderstatuslog;
create view analysis.orderitems as select * from production.orderitems;
create view analysis.products as select * from production.products;
create view analysis.users as select * from production.users;

-- DDL для dm_rfm_segments
drop table if exists analysis.dm_rfm_segments;

create table analysis.dm_rfm_segments (
user_id integer not null primary key,
recency smallint not null check(recency >= 1 AND recency <= 5),
frequency smallint not null check(frequency >= 1 AND frequency <= 5),
monetary_value smallint not null check(monetary_value >= 1 AND monetary_value <= 5)
);

-- Наполнение dm_rfm_segments
insert into analysis.dm_rfm_segments
with rfm_data as (
select
user_id,
max(order_ts) as recency,
count(*) as frequency,
sum(payment) as monetary_value
from production.orders o
inner join production.orderstatuses os
on o.status=os.id and os.key='Closed'
group by user_id
),
percentiles as (
select
case
  when k=0.2 then 1
  when k=0.4 then 2
  when k=0.6 then 3
  when k=0.8 then 4
  when k=1 then 5
end as frm_rank,
percentile_disc(k) within group (order by recency) as recency_group,
percentile_disc(k) within group (order by frequency) as frequency_group,
percentile_disc(k) within group (order by monetary_value) as monetary_value_group
from rfm_data, generate_series(0.2, 1.0, 0.2) as k
group by k
)
select
user_id,
case
  when recency<=(select recency_group from percentiles where frm_rank=1) then 1
  when recency<=(select recency_group from percentiles where frm_rank=2) then 2
  when recency<=(select recency_group from percentiles where frm_rank=3) then 3
  when recency<=(select recency_group from percentiles where frm_rank=4) then 4
  when recency<=(select recency_group from percentiles where frm_rank=5) then 5
end as recency,
case
  when frequency<=(select frequency_group from percentiles where frm_rank=1) then 1
  when frequency<=(select frequency_group from percentiles where frm_rank=2) then 2
  when frequency<=(select frequency_group from percentiles where frm_rank=3) then 3
  when frequency<=(select frequency_group from percentiles where frm_rank=4) then 4
  when frequency<=(select frequency_group from percentiles where frm_rank=5) then 5
end as frequency,
case
  when monetary_value<=(select monetary_value_group from percentiles where frm_rank=1) then 1
  when monetary_value<=(select monetary_value_group from percentiles where frm_rank=2) then 2
  when monetary_value<=(select monetary_value_group from percentiles where frm_rank=3) then 3
  when monetary_value<=(select monetary_value_group from percentiles where frm_rank=4) then 4
  when monetary_value<=(select monetary_value_group from percentiles where frm_rank=5) then 5
end as monetary_value
from rfm_data
order by user_id;

-- Проверки
select recency, count(user_id) users_cnt from analysis.dm_rfm_segments group by recency;
select frequency, count(user_id) users_cnt from analysis.dm_rfm_segments group by frequency;
select monetary_value, count(user_id) users_cnt from analysis.dm_rfm_segments group by monetary_value;
