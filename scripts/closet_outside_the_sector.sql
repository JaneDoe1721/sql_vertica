with
/*+ENABLE_WITH_CLAUSE_MATERIALIZATION */ item as (
    select inip.place_id, inip.item_id, count(inip.id) as qty, inip.stock_type
    from wms_csharp_service_storage_all.instance_in_place inip
    where inip.place_type = 2
    group by inip.place_id, inip.item_id, inip.stock_type
    union all
    select iip.place_id, iip.item_id, iip.quantity as qty, iip.stock_type
    from wms_csharp_service_storage_all.item_in_place iip
    where iip.place_type = 2
    group by iip.place_id, iip.item_id, iip.quantity, iip.stock_type
),
     cell as (
    select ci.id
         ,ci.full_name
    from wms_topology.cell_info ci
),
     topology as (
    select th4.id              as cell_id
         , ci.full_name        as cell_name
         , zi.name             as zone
         , zi.floor_number + 1 as floor_number
         , th1.parent_id       as warehouse_id
         , wh.name             as warehouse_name
    from wms_topology.topology_hierarchy th1
             join (select id, parent_id
                   from wms_topology.topology_hierarchy
                   where is_deleted = 'false') th2 on th2.parent_id = th1.id -- and th2.is_deleted = 'false'
             join (select id, parent_id
                   from wms_topology.topology_hierarchy
                   where is_deleted = 'false') th3 on th3.parent_id = th2.id -- and th3.is_deleted = 'false'
             join (select id, parent_id
                   from wms_topology.topology_hierarchy
                   where is_deleted = 'false'
                 ) th4 on th4.parent_id = th3.id -- and th4.is_deleted = 'false'
             join (select id, full_name
                        from wms_topology.cell_info
                        where is_deleted = 'false'
                          and id in (
                            select cell.id
                            from cell
                        )) ci on ci.id = th4.id
             join (select id, floor_number, name
                   from wms_topology.zone_info
                 where zone_info.is_deleted = 'false'
    ) zi on th2.id = zi.id
    join whc_go_crud_warehouse.warehouses wh on wh.clearing_id = th1.parent_id
    where th1."type" = 1
      and th1.is_deleted = 'false'
      and th1.parent_id in (
        18044341087000, -- Новосибирск_РФЦ_НОВЫЙ
        15431806189000, -- ХОРУГВИНО_РФЦ
        18044249781000 -- Санкт_Петербург_РФЦ
        ,18044494830000, -- Казань_РФЦ_НОВЫЙ
        19262731541000, -- Хоругвино_НЕГАБАРИТ
        18044570445000, -- Екатеринбург_РФЦ_НОВЫЙ
        17717042026000, -- Ростов_на_Дону_РФЦ
        21226826263000, -- Новая_Рига_РФЦ
        21225173751000, -- Хабаровск_РФЦ
        22327039387000, -- НУР_СУЛТАН_МРФЦ
        22294782253000, -- КАЛИНИНГРАД_МРФЦ
        22296628035000  -- КРАСНОЯРСК_МРФЦ
    )
)
select topology.warehouse_id,
       topology.warehouse_name,
       item.place_id,
       topology.cell_name,
       item.item_id,
       item.stock_type,
       sum(qty) as qty1
from item
join topology on topology.cell_id = item.place_id
where item.place_id not in (
    select distinct th.id
    from wms_topology.sector_racks sr
        join wms_topology.topology_hierarchy th on th.parent_id = sr.rack_id
    join wms_topology.sector_info si on si.id = sr.sector_id
    where si.type = 2
    and si.is_deleted = 'false'
    )
group by item.place_id,
         topology.warehouse_name,
         topology.warehouse_id,
         topology.cell_name,
         item.item_id,
         item.stock_type
order by topology.warehouse_id,
         topology.cell_name,
         item.item_id












