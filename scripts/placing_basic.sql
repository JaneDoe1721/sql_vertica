select
	--namber_id.supply_id as 'Номер поставки',
	tsk.id as task_id,
	sectr_inf.name as 'Сектор размещения',
	name.name as 'Оператор',
	case when to_char(start_.at + interval '3 hour', 'HH24:mi:ss') < '08:00:00'
		 then to_char((start_.at - 1) + interval '3 hour', 'dd.MM.yyyy')
		 else to_char(start_.at + interval '3 hour', 'dd.MM.yyyy') end as date,
	to_char(start_.at + interval '3 hour', 'dd.MM.yyyy HH24:MI:SS') as 'Начало задания',
	to_char(first_pl_pick + interval '3 hour', 'dd.MM.yyyy HH24:MI:SS') as 'Время первого размещения',
	to_char(end_.at + interval '3 hour', 'dd.MM.yyyy HH24:MI:SS') as 'Окончание задания',
	to_char((end_.at - start_.at), 'HH:MI:SS') as 'Время выполнения задания',
	to_char((first_pl_pick - start_.at), 'HH24:MI:SS') as 'Время до превого размещенного товара',
	ww + isnull(count_, 0) as 'Кол-во размещённого товара',
	si1.name as 'Изначальный сектор',
    sectr_inf.name as 'Конечный сектор'
    --start_s.sector_id,
    --pt.sector_id, 
	from wms_csharp_service_task.tasks tsk
		join (select *
			from wms_csharp_service_task.tasks_log start_1
		where start_1.status = 20) start_ on start_.task_id = tsk.id
		join (select *
			from wms_csharp_service_task.tasks_log end_1
		where end_1.status = 30) end_ on end_.task_id = tsk.id
		join (select task_id, boxing_id, sector_id from csharp_service_placing_raw.placing_tasks
		where placing_task_status = 1 and dbz_op = 'c') start_s on start_s.task_id = tsk.id
		join (select task_id, count(distinct sector_id) as r
    	from csharp_service_placing_raw.placing_tasks
    	where placing_task_status = 3
    	group by task_id) s on s.task_id = start_s.task_id
		join (select 
			r1.reason_id,
			count(r1.id) - count(r1.quantity) as ww,
			sum(r1.quantity) as count_
			--r1.supply_id,
			--r1.from_id
			from wms_csharp_service_storage_all.movement_log r1
			where to_type = 2 and from_type = 3 and from_stock_type = 1 and to_stock_type = 5 and reason = 1
			group by 
			r1.reason_id) namber_id on namber_id.reason_id = tsk.id
		join csharp_service_placing.placing_tasks sort on sort.task_id = tsk.id 
		left join wms_csharp_service_task.placing_tasks pt1 on pt1.task_id = tsk.id 
		left join wms_service_employee."user" u on u.id = start_.user_id
		left join wms_service_employee.type_of_employment toe on toe.id = u.type_of_employment_id
		left join wms_service_employee.user name on name.name = u.name 
left join wms_topology.sector_info sectr_inf on sectr_inf.id = sort.sector_id
left join wms_topology.sector_info si1 on si1.id = start_s.sector_id
left join (select 
			reason_id,
			min(at) as first_pl_pick 
			from wms_csharp_service_storage_all.movement_log
			where item_id > 0
			and to_type = 2
			group by reason_id) m1_first on m1_first.reason_id = tsk.id 
left join (select
			pi1.task_id,
			pi1.supply_id,
			pi1.boxing_id,
			count(pi1.placing_result) as q1
			from csharp_service_placing.placed_instances pi1
			where pi1.placing_result = 3
			group by 
			pi1.task_id,
			pi1.supply_id,
			pi1.boxing_id) pi1 on pi1.task_id = tsk.id --and pi1.boxing_id = namber_id.from_id
--join wms_csharp_service_boxing.boxings box on box.id = namber_id.from_id
where tsk.warehouse_id = 19262731541000
    and cast (start_.at AT TIME ZONE 'UTC' AT TIME ZONE 'MSK' as smalldatetime) >= '2022-02-28 00:00'
    and cast (start_.at AT TIME ZONE 'UTC' AT TIME ZONE 'MSK' as smalldatetime) < '2022-03-18 00:00'
group by
	tsk.id,
	sectr_inf.name,
	name.name,
	start_.at,
	end_.at,
	first_pl_pick,
	date,
	ww + isnull(count_, 0),
	si1.name
order BY 
	date,
	name.name,
	to_char(start_.at + interval '3 hour', 'dd.MM.yyyy HH24:MI:SS')