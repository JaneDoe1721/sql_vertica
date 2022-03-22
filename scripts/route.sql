--выгрузка связи ячейка-сектор
with topology as (
    select th4.id              as cell_id
         , ci.full_name        as cell_name
         , ct1.type            as cell_short_type
         , ct1.purpose         as cell_type_name
         , ct1.id              as cell_type_id
         , ci.restriction_tags  as cell_tags
         , ci.restriction_tag_aliases  as cell_tags1
         , zi.floor_number + 1 as floor_number
         , si.name             as sector
         , si.id               as id_sector
         , ci.closed_for_placing as cell_is_closed
         , ri.closed_for_placing as rack_is_closed
         , ct1.max_capacity
         , ci.picking_order
         , ct1.sku_qty
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
join (select id, full_name, cell_type_id, restriction_tags, restriction_tag_aliases, closed_for_placing, picking_order
from wms_topology.cell_info
                   where is_deleted = 'false'
    --and cell_type_id = 230
) ci on ci.id = th4.id
left join (
    select id,purpose, max_capacity, sku_qty, type
    from wms_topology.cell_type ct1
    where warehouse_id = 19262731541000--18044249781000--18044249781000--21226826263000
     ) ct1 on ct1.id = ci.cell_type_id
left join (select id, floor_number
from wms_topology.zone_info
        --where id = 5372043
    ) zi on th2.id = zi.id
left join (select rack_id, sector_id
from wms_topology.sector_racks) sr on th3.id = sr.rack_id
left join (select id, closed_for_placing
    from wms_topology.rack_info)ri on ri.id = sr.rack_id
join (select id, name
from wms_topology.sector_info
where type = 2 --сектор подбора
--where type = 1 --сектор размещения
    and id != 356 and id !=357
    --and id in (2319)
    --and id in (591)
) si on sr.sector_id = si.id -- and si.type = 1
where th1."type" = 1
and th1.is_deleted = 'false'
and th1.parent_id = 19262731541000--18044249781000--18044249781000--21226826263000
)
select
sector                        as 'Сектор'
,topology.cell_name
,cell_id
,cell_short_type
,topology.cell_type_name
,cell_type_id
,topology.sku_qty
,topology.cell_tags
,topology.cell_tags1
,topology.max_capacity
,topology.cell_is_closed
,topology.rack_is_closed
,topology.picking_order
from topology
where picking_order isnull
--where cell_name like '%K%'
group by sector, topology.cell_name , topology.cell_type_name,topology.cell_tags ,topology.cell_tags1,topology.cell_is_closed,topology.rack_is_closed, cell_type_id
,topology.max_capacity, topology.picking_order, topology.sku_qty
,cell_short_type
,cell_id
order by sector
,cell_short_type, topology.cell_name