-- Подбор +
select
    to_char( pi.at , 'dd.MM.yyyy HH24:mi:ss' ) as "Время подбора"
  , u.name                                     as "Имя подборщика"
  , t.batch_id                                 as "Батч"
  , t.task_id                                  as "Таск"
  , z.description                              as "Зона подбора"
  , pi.item_id                                 as "Артикул товара"
  , pi.instance_id                             as "Штрихкод"
  , ci.name                                    as "Ячейка"
  , sum( pi.qty )                              as "Кол-во подобранного товара"
  , case
        when sum( pi.qty ) = 0 then 'Скип'
        else 'Подбор'
    end                                        as "Скип/Подбор"
from wms_csharp_service_picking.tasks t
join wms_csharp_service_task.tasks    tt on t.task_id = tt.id
join (
     select
         pi.task_id
       , pi.item_id
       , ir.serial_number   as instance_id
       , pi.at
       , pi.cell_id
       , sum( pi.quantity ) as qty
     from wms_csharp_service_picking.picked_items pi
     join item_source_service.item_rezon          ir on pi.item_id = ir.id
     where pi.at between '2022-02-15 08:00:00' and '2022-02-15 20:00:00'
     group by pi.task_id
            , pi.item_id
            , ir.serial_number
            , pi.at
            , pi.cell_id
     union all
     select
         pi.task_id
       , i.sku_id                as item_id
       , i.barcode               as instance_id
       , pi.at
       , pi.cell_id
       , count( pi.instance_id ) as qty
     from wms_csharp_service_picking.picked_instances pi
     join wms_csharp_service_item.instances           i on i.id = pi.instance_id
     where pi.at between '2022-02-15 08:00:00' and '2022-02-15 20:00:00'
     group by pi.task_id
            , i.sku_id
            , i.barcode
            , pi.at
            , pi.cell_id
     union all
     select
         s.task_id
       , s.item_id
       , null as instance_id
       , s.at
       , s.cell_id
       , 0    as qty
     from wms_csharp_service_picking.skips s
     where s.at between '2022-02-15 08:00:00' and '2022-02-15 20:00:00'
     group by s.task_id
            , s.item_id
            , s.at
            , s.cell_id
     )                                pi on pi.task_id = t.task_id
left join wms_topology.cell_info      ci on ci.id = pi.cell_id
left join (
          select
              si.id
            , zi.description
          from wms_topology.sector_info si
          join wms_topology.zone_info   zi on zi.id = si.zone_id
          where zi.is_deleted is false
          )                           z on z.id = t.sector_id
left join wms_service_employee."user" u on u.id = tt.user_id
where u.warehouse_id = 19262731541000
group by t.batch_id
       , u.name
       , to_char( pi.at , 'dd.MM.yyyy HH24:mi:ss' )
       , t.task_id
       , z.description
       , pi.item_id
       , pi.instance_id
       , ci.name
order by t.task_id;