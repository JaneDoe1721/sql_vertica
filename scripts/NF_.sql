select
       u.name as 'Размещенец',
       skip.sm as "Метод сортировки",
       plac.task_id as 'Задание',
      -- boxing_purpose,
       si.name as "Наименование сектора",
       plac.item_id ,
       full_name as "Наименование ячейки",
       placed_at as "Дата и время размещения",
       count(plac.quantity) as "Размещенное количество",
       --instance_id,
       --supply,
      -- ml.quantity,
       --ml.at,
      -- ml.name,
       u2.name,
       skip.at as "дата и время перехода в NF"
from (select task_id, item_id, cell_id, placed_at, 1 as quantity, instance_id, supply_id as supply
      from csharp_service_placing.placed_instances pi
      where placing_result = 0
      union all
      select task_id, item_id, cell_id, placed_at, quantity, 0 as instance_id, bunch_id as supply
      from csharp_service_placing.placed_items pit
      where placing_result = 0
     ) plac
    join csharp_service_placing.placing_tasks pt on pt.task_id = plac.task_id
    join wms_topology.sector_info si on si.id = sector_id

         join wms_csharp_service_task.tasks t on t.id = plac.task_id
         join wms_service_employee."user" u on u.id = t.user_id
         join wms_topology.cell_info ci on ci.id = cell_id

         full join (select to_id,
                           isnull(instance_id, 0)      as inst,
                           isnull(supply_id, bunch_id) as s,
                           item_id,
                           quantity,
                           at,
                           user_id, name
                    from wms_csharp_service_storage.movement_log
                    join wms_service_employee."user" u1 on u1.id = user_id
                    where to_stock_type = 3
                      and reason = 7) ml on ml.item_id = plac.item_id and to_id = cell_id and inst = instance_id
         join (select s.task_id, cell_id, item_id, is_processed, at, user_id, sm.name as sm
               from wms_csharp_service_picking.skips s
                        join wms_csharp_service_task.tasks t on t.id = task_id
                        join wms_csharp_service_picking.tasks sort on sort.task_id = t.id
                        join wms_batching.batch b on b.batch_id = sort.batch_id
                        join wms_crud_settings_ss.sort_method sm on sm.id = b.sort_method_id
               where  at >= '2022-02-07 00:00' and at < '2022-02-24 00:00'
                 and t.warehouse_id = 19262731541000
             group by s.task_id, cell_id, item_id, is_processed, at, user_id, sm.name) skip on skip.item_id = plac.item_id and skip.cell_id = plac.cell_id
         join wms_service_employee."user" u2 on u2.id = skip.user_id

  and t.warehouse_id = 19262731541000
group by
	u.name,
	skip.sm,
	plac.task_id,
	si.name,
	plac.item_id,
	full_name,
	placed_at,
	plac.quantity,
	u2.name,
	skip.at