-- Обновление представления
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

-- Проверки
select order_id, status from production.orders order by order_id;
select order_id, status from analysis.orders order by order_id;
select count(*) from production.orders;
select count(*) from analysis.orders;
