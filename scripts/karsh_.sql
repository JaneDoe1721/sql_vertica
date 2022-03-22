select
        t.id,
        case
            when t.type = 3 then 'подбор'
            when t.type = 2 and placing_i.supply_id is null then 'размещение моно'
            when t.type = 2 and placing_i.supply_id  is not null then 'размещение'
            when t.type = 17 then 'подпитка'
            when t.type = 6 then 'перемещение'
            when t.type = 19 then 'подбор моно'
        end,
        case when to_char(start_.at, 'HH24:mi:ss') < '08:00:00'
		    then to_char((start_.at - 1), 'dd.MM.yyyy')
		    else to_char(start_.at, 'dd.MM.yyyy') end as date,
        to_char(start_.at, 'YYYY-MM-DD HH24:MI:SS') as 'Начало задания',
        to_char(end_.at, 'dd.MM.yyyy HH24:MI:SS') as 'Конец задания',
        to_char((end_.at - start_.at), 'HH:MI:SS') as 'Время выполнения задания',
        us.name,
        case
            when ww_place + isnull(count_place, 0) is not null then  ww_place + isnull(count_place, 0)
            when ww_place + isnull(count_place, 0) is null then shift.qty
            end as 'Количество товара',
        us.Company
from wms_csharp_service_task.tasks t
join (select ul.user_id,
       ul.name,
       toe.company_name as Company,
       p1c.name as "Должность",
       listagg(distinct r.name) as "Название роли"
    from wms_service_employee.user_log ul
    join (
        select
             *
            from wms_service_employee."user"
        ) u on u.id = ul.user_id
    left join wms_service_employee.type_of_employment toe on u.type_of_employment_id = toe.id
    left join wms_service_employee.position_1c p1c on u.position_1c_id = p1c.id
    join wms_service_employee.user_role ur on u.id = ur.user_id
    join wms_service_employee.role r on ur.role_id = r.id and regexp_ilike(r.name,'Водитель' )
    where u.warehouse_id = 19262731541000
    group by ul.user_id, ul.name,toe.company_name, p1c.name) us on us.user_id = t.user_id
left join (select *
			from wms_csharp_service_task.tasks_log start_1
		where start_1.status = 20) start_ on t.id = start_.task_id
left join (select *
			from wms_csharp_service_task.tasks_log end_1
		where end_1.status = 30) end_ on t.id = end_.task_id
left join (select r1.reason_id,
                  r1.supply_id as supply_id,
			count(r1.id) - count(r1.quantity) as ww_place,
			sum(r1.quantity) as count_place
        from wms_csharp_service_storage_all.movement_log r1
        where from_stock_type = 1 and to_stock_type = 5
        group by r1.reason_id, r1.supply_id) placing_i on t.id = placing_i.reason_id
left join (select
              pi.task_id
            , sum( pi.qty )                as qty
          from (
               select
                   pi.task_id
                 , pi.quantity as qty
               from wms_csharp_service_picking.picked_items pi
               union all
               select
                   pi.task_id
                 , 1          as qty
               from wms_csharp_service_picking.picked_instances pi
               ) pi
          group by pi.task_id
          order by pi.task_id) shift on t.id = shift.task_id
where cast (start_.at AT TIME ZONE 'UTC' AT TIME ZONE 'MSK' as smalldatetime) >= '2022-02-28 00:00'
    and cast (start_.at AT TIME ZONE 'UTC' AT TIME ZONE 'MSK' as smalldatetime) < '2022-03-18 00:00'
    and t.type != 1 and t.type != 15 and t.type != 34
group by t.id,
         t.type,
         start_.at,
         end_.at,
         date,
         us.name,
         ww_place + isnull(count_place, 0),
         shift.qty,
         placing_i.supply_id,
         us.Company
order by
        date,
        us.name,
        start_.at