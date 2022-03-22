--Скипы+ выхлоп детализация до типа ячейки
--выгрузка подбор по секторам
with
/*+ENABLE_WITH_CLAUSE_MATERIALIZATION */
par as (
    select w.clearing_id as warehouse_id
         , '2022-02-15 08:00:00'::timestamp at time zone w.timezone as bdate
         , '2022-02-15 20:00:00'::timestamp at time zone w.timezone as edate
         , w.timezone
         , w.name
    from whc_go_crud_warehouse.warehouses w
    where w.clearing_id = 19262731541000 -- подставить ID склада

),
     cell as (
         select ci.id
         from wms_topology.cell_info ci
         where is_deleted = 'false'
     ),
     topology as (
         select th4.id              as cell_id
              , ci.cell_type_id     as cell_type_id
              , ct.purpose          as cell_type_name
              , ct.type             as cell_type
              , ci.full_name
              , sr.rack_id          as rack_id
              , zi.floor_number + 1 as floor_number
              , zi.name             as zone
              , si.name             as sector
              , si.id               as id_sector
         from wms_topology.topology_hierarchy th1
                  join (select id, parent_id
                        from wms_topology.topology_hierarchy
                        where is_deleted = 'false') th2 on th2.parent_id = th1.id -- and th2.is_deleted = 'false'
                  join (select id, parent_id
                        from wms_topology.topology_hierarchy
                        where is_deleted = 'false') th3 on th3.parent_id = th2.id -- and th3.is_deleted = 'false'
                  join (select id, parent_id
                        from wms_topology.topology_hierarchy
                        where is_deleted = 'false') th4 on th4.parent_id = th3.id -- and th4.is_deleted = 'false'
                  join (select id, full_name, cell_type_id
                        from wms_topology.cell_info
                        where is_deleted = 'false'
                          and id in (
                            select cell.id
                            from cell
                        )) ci on ci.id = th4.id
                  join wms_topology.cell_type ct on ct.id = ci.cell_type_id
                  join (select id, floor_number,name
                        from wms_topology.zone_info) zi on th2.id = zi.id
                  join (select rack_id, sector_id
                        from wms_topology.sector_racks) sr on th3.id = sr.rack_id
                  join (select id, name
                        from wms_topology.sector_info
                        where type = 2
         ) si on sr.sector_id = si.id -- and si.type = 1
         where th1."type" = 1
           and th1.is_deleted = 'false'
           and th1.parent_id = 19262731541000 -- подставить ID склада
     )
,main as (select par.name                                                                         as warehouse
     , pi.zone                                                                                    as zone
     , pi.sector                                                                                    as sector
     , tl.task_id
     --, sm.name                                                                                    as sort_method
     --, pt.batch_id
     --, TO_CHAR(tl20.start_at::timestamptz at time zone par.timezone, 'YYYY-MM-DD HH24:MI:SS')     as start_at
     --, TO_CHAR(pi2.first_pick_at::timestamptz at time zone par.timezone, 'YYYY-MM-DD HH24:MI:SS') as first_pick_at
     --, TO_CHAR(tl.at::timestamptz at time zone par.timezone, 'YYYY-MM-DD HH24:MI:SS')             as finish_at
     --, tl20.start_at::timestamptz at time zone par.timezone     as start_at
     --, pi2.first_pick_at::timestamptz at time zone par.timezone as first_pick_at
     --, tl.at::timestamptz at time zone par.timezone             as finish_at
     --, pt.dop
     --, pi.cnt_cells
     --, pi.cnt_racks
     --, pi.cnt_items
     --, tb.cnt_boxings
     , pi.cell_type_id
     , pi.cell_type
     , pi.cell_type_name
     , sum(isnull(pi.skip_qty,0)) as skips
     , sum(isnull(pi.pick_qty,0)) as picked
     , t.user_id
     --, name.name as user_name
     --, toe.company_name as company
     , case
           when to_char(tl20.start_at::timestamptz at time zone par.timezone, 'HH24:mi:ss') < '08:00:00'
               then to_char((tl20.start_at - 1)::timestamptz at time zone par.timezone, 'dd.MM.yyyy')
           else to_char(tl20.start_at::timestamptz at time zone par.timezone, 'dd.MM.yyyy') end   as date
     , case
           when to_char(tl20.start_at::timestamptz at time zone par.timezone, 'HH24:mi:ss') < '08:00:00'
               or to_char(tl20.start_at::timestamptz at time zone par.timezone, 'HH24:mi:ss') > '20:00:00'
               then 'ночь'
           else 'день' end                                                                        as Smena
     , week_iso(to_timestamp(case when to_char(tl20.start_at::timestamptz at time zone par.timezone, 'HH24:mi:ss') < '08:00:00'
               then to_char((tl20.start_at - 1)::timestamptz at time zone par.timezone, 'dd.MM.yyyy')
               else to_char(tl20.start_at::timestamptz at time zone par.timezone, 'dd.MM.yyyy') end,'DD.MM.YYYY')) as Operation_Week
     --, u.type_of_employment_id
from wms_csharp_service_task.tasks_log tl
         join wms_csharp_service_task.tasks t on t.id = tl.task_id
         /*left join wms_service_employee."user" u on u.id = t.user_id
         left join wms_service_employee.type_of_employment toe on toe.id = u.type_of_employment_id
         left join wms_service_employee.user name on name.name = u.name*/
         join par on par.warehouse_id = t.warehouse_id
         /*left join (
    select pt.task_id, pt.batch_id, pt.sector_id, case when pt.is_wrap_up = 'true' then 1 else 0 end as dop
    from wms_csharp_service_picking.tasks pt
    order by pt.task_id
) pt on pt.task_id = tl.task_id*/
         join (
    select tl20.task_id
         , min(tl20.at) as start_at
    from wms_csharp_service_task.tasks_log tl20
    where tl20.status = 20
    group by tl20.task_id
    order by tl20.task_id
) tl20 on tl20.task_id = tl.task_id
         join (
    select distinct pi.task_id
         , sum(isnull(pi.skip,0))                     as skip_qty
         , sum(isnull(pi.qty,0))                      as pick_qty
         , pi.cell_id
         , topology.cell_type_id
         , topology.cell_type
         , topology.cell_type_name
         , topology.sector
         , topology.zone
         , topology.id_sector
    from (
             select pi.task_id
                  , pi.item_id
                  , pi.cell_id
                  , 0           as skip
                  , pi.quantity as qty
             from wms_csharp_service_picking.picked_items pi
             union all
             select pi.task_id
                  , 0 as item_id
                  , pi.cell_id
                  , 0          as skip
                  , 1          as qty
             from wms_csharp_service_picking.picked_instances pi
             union all
             select s.task_id
                  , s.item_id
                  , s.cell_id
                  , count(s.id) as skip
                  , 0           as qty
             from wms_csharp_service_picking.skips s
             group by s.task_id, s.cell_id,s.item_id
         ) pi
             join topology
                  on topology.cell_id = pi.cell_id
    group by pi.task_id
           , pi.cell_id
           , topology.cell_type_id
           , topology.cell_type
           , topology.cell_type_name
           , topology.sector
           , topology.zone
           , topology.id_sector
) pi on pi.task_id = tl.task_id
         /*left join (
    select b.batch_id
         , b.sort_method_id
    from wms_batching.batch b
             join par on par.warehouse_id = b.warehouse_id
    where b.created_at > par.bdate - interval '7' day
    order by b.batch_id
) b on b.batch_id = pt.batch_id*/
         --left join wms_crud_settings_ss.sort_method sm on sm.id = b.sort_method_id
--left join topology on topology.cell_id = s.cell_id
where tl.status = 30
  and t.type = 3
  and tl20.start_at --> par.bdate
      between par.bdate and par.edate
  and (isnull(pi.skip_qty, 0) + isnull(pi.pick_qty, 0)) > 0
  --and isnull(pi.qty, 0) > 0 --только для инфо по выводу
  --and tl.at != 'null'
  --and sm.id in (1155, 1156, 1157, 1294, 1295, 1293, 1167, 1120, 1236) --только моно
  --and sm.id in (1155, 1156, 1157, 1294, 1295, 1293, 1167) --только моно сорт
  --and sm.id in (1120, 1236) --только моно сорт
  --and pt.parent_task_id = 'null' --убиваю допобор и хвосты
  --and sm.id not in (1298, 1302, 1226, 1235)
  --and sm.name != 'null'
  --and tl.task_id = 58724947
  --and pi.qty isnull
group by par.name
       , pi.zone
       , pi.sector
       , tl.task_id
       , pi.cell_type_id
       , pi.cell_type
       , pi.cell_type_name
       , t.user_id
       --, name.name
       --, toe.company_name
       , date
       , smena
       , Operation_Week
order by tl.task_id
    )
select    warehouse
        , Operation_Week
        , date --,task_id
        --,smena
        , zone
        , sector
        , main.cell_type_id
        , main.cell_type
        , main.cell_type_name
        --,company
        --,isnull(sum(dop),0) as cnt_dop
        --,isnull(count(task_id),0) as cnt_tasks
        --,sum(isnull(picked,0))
        --,sum(isnull(skips,0))
        , sum(picked) as qty_picked
        , sum(skips) as qty_skipped
from main
group by warehouse, Operation_Week,date--,smena
        --,task_id
        ,zone, sector
        , main.cell_type_id
        , main.cell_type
        , main.cell_type_name
        --,picked,skips
       --, company*/
order by warehouse, Operation_Week,date--,smena
        --,zone, sector
       --, company
;