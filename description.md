# Витрина RFM

## 1.1. Выясните требования к целевой витрине.

### Витрина RFM-сегментации клиентов
`Расположение: analysis.dm_rfm_segments.` <br>
`Частота обновлений: не обновляется.` <br>
`Описание: RFM-сегментация клиентов для успешно выполненных заказов (статус Closed) за часть 2022 года (2022-02-12 - 2022-03-14).` <br>
`Структура:` <br>
- user_id
- recency (число от 1 до 5)
- frequency (число от 1 до 5)
- monetary_value (число от 1 до 5)

-----------

## 1.2. Изучите структуру исходных данных.

`Используемые поля:` <br>
- orders.user_id (идентификатор пользователя)
- orders.order_ts (время заказа)
- orders.payment (сумма заказа)
- orders.status (код статуса заказа - фильтр заказа в статусе Closed - код 4)

-----------

## 1.3. Проанализируйте качество данных

## Оцените, насколько качественные данные хранятся в источнике.
`Были проверены поля, используемые для построения целевой витрины.` <br>
`Критерии проверки: наличие пропусков (null), адекватность значений (min и max для суммы платежа и времени заказа). Пропусков в целевых полях не обнаружено. Суммы платежей без выбросов. Данные присутствуют за период 2022-02-12 - 2022-03-14.` <br>
`Сверено количество уникальных пользователей в orders и users (таблицы в схеме не связаны через FK). Расхождений не обнаружено.` <br>
`Проверено распределение заказов по полю status: ~50% заказов имеют статус Closed, остальные - Cancelled.`

```SQL
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
```

## Укажите, какие инструменты обеспечивают качество данных в источнике.

| Таблицы             | Инструменты                      |
| ------------------- | --------------------------- |
| production.orders | orders_pkey (PK и btree index: order_id), <br> orders_check (проверка cost = (payment + bonus_payment)) |
| production.orderstatuses | orderstatuses_pkey (PK и btree index: id) |
| production.orderstatuslog | orderstatuslog_pkey (PK и btree index: id), <br> orderstatuslog_order_id_fkey (FK и btree index: order_id <-> orders(order_id)), <br> orderstatuslog_status_id_fkey (FK и btree index: status_id <-> orderstatuses(id)), <br> orderstatuslog_order_id_status_id_key (UNIQUE KEY и btree index: order_id, status_id) |
| production.orderitems | orderitems_pkey (PK и btree index: id), <br> orderitems_order_id_fkey (FK и btree index: order_id <-> orders(order_id)), <br> orderitems_product_id_fkey (FK и btree index: product_id <-> products(id)), <br> orderitems_order_id_product_id_key (UNIQUE KEY и btree index: order_id, product_id), <br> orderitems_check (проверка discount >= (0)::numeric) AND (discount <= price)), <br> orderitems_price_check (проверка price >= (0)::numeric), <br> orderitems_quantity_check (проверка quantity > 0) |
| production.products | products_pkey (PK и btree index: id), <br> products_price_check (проверка price >= (0)::numeric) |
| production.users | users_pkey (PK и btree index: id) |

-----------

## 1.4. Подготовьте витрину данных

### 1.4.1. Сделайте VIEW для таблиц из базы production.

```SQL
create view analysis.orders as select * from production.orders;
create view analysis.orderstatuses as select * from production.orderstatuses;
create view analysis.orderstatuslog as select * from production.orderstatuslog;
create view analysis.orderitems as select * from production.orderitems;
create view analysis.products as select * from production.products;
create view analysis.users as select * from production.users;
```

### 1.4.2. Напишите DDL-запрос для создания витрины.

```SQL
drop table if exists analysis.dm_rfm_segments;

create table analysis.dm_rfm_segments (
user_id integer not null primary key,
recency smallint not null check(recency >= 1 AND recency <= 5),
frequency smallint not null check(frequency >= 1 AND frequency <= 5),
monetary_value smallint not null check(monetary_value >= 1 AND monetary_value <= 5)
);
```

### 1.4.3. Напишите SQL запрос для заполнения витрины

```SQL
insert into analysis.dm_rfm_segments
with rfm_data as (
select
user_id,
max(order_ts) as recency,
count(*) as frequency,
sum(payment) as monetary_value
from production.orders
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
```

-----------

## 2.1. Обновите представление с учетом новых вводных

## В таблице production.orders больше нет поля статус. Значение в этом поле должно соответствовать последнему (по времени) значению статуса из таблицы production.orderstatuslog.

```SQL
create or replace view analysis.orders as
select distinct on (o.order_id)
o.order_id,
o.order_ts,
o.user_id,
o.bonus_payment,
o.payment,
o.cost,
o.bonus_grant,
osl.status_id as status
from production.orders o
inner join production.orderstatuslog osl
on o.order_id=osl.order_id
order by o.order_id, osl.dttm desc;
```
