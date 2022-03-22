with
stock as (
    select iip.id
         , iip.place_id
         , iip.scan_it
         , sum(iip.qty) as qty
         , count(iip.place_id) as qnt_cells
         , iip.stock_type
         , iip.supply_id
    from (
             select inip.item_id as id
                  , inip.place_id
                  , inip.id      as scan_it
                  ,inip.supply_id as supply_id
                  , 1            as qty
                  , inip.stock_type
             from wms_csharp_service_storage_all.instance_in_place inip
             where inip.place_type = 2
             union all
             select itip.item_id  as id
                  , itip.place_id
                  , 0 as supply_id
                  , 0             as scan_it
                  , itip.quantity as qty
                  , itip.stock_type
             from wms_csharp_service_storage_all.item_in_place itip
             where itip.place_type = 2
         ) iip
    group by iip.id
           , iip.place_id
           , iip.scan_it
           , iip.stock_type
           , iip.supply_id
)
, topology as (
    select th4.id         as cell_id
                    , ci.full_name   as cell_name
                    , si.name        as sector_name
                    , si.id          as sector_id
                    , ri.description as row
                    , ri.name        as rack
                    , th1.parent_id  as warehouse
               from wms_topology.topology_hierarchy th4
                        join (select th3.id
                                   , th3.parent_id
                              from wms_topology.topology_hierarchy th3
                              where th3.type = 3
                                and th3.is_deleted is false) th3 on th3.id = th4.parent_id
                        join (select th2.id
                                   , th2.parent_id
                              from wms_topology.topology_hierarchy th2
                              where th2.type = 2
                                and th2.is_deleted is false) th2 on th2.id = th3.parent_id
                        join (select th1.id
                                   , th1.parent_id
                              from wms_topology.topology_hierarchy th1
                              where th1.type = 1
                                and th1.is_deleted is false) th1 on th1.id = th2.parent_id
                        join (select si.zone_id
                                   , si.name
                                   , si.id
                              from wms_topology.sector_info si
                              where si.type = 2) si on si.zone_id = th2.id
                        join (select ri.id
                                   , ri.name
                                   , ri.description
                              from wms_topology.rack_info ri) ri on th3.id = ri.id
                        join (select ci.id
                                   , ci.full_name
                                   , ci.restriction_tags
                              from wms_topology.cell_info ci) ci on ci.id = th4.id
               where th4.type = 4 and th1.parent_id = 19262731541000 --and si.id = (129)
)
, itemovh as (
    select
                i.sourcekey as item_id
            , n.name      as item_name
            , h.Height as height
            , d.Depth as lenght
            , w.Width as width
            , we.Weight as weight
            , (h.Height * d.Depth * w.Width ) * 1000 as volume
            from dwh_data.anc_item i
            left join dwh_data.atr_item_name n using ( itemid )
            left join dwh_data.Atr_Item_Height h using ( itemid )
            left join dwh_data.Atr_Item_Depth d using ( itemid )
            left join dwh_data.Atr_Item_Width w using ( itemid )
            left join dwh_data.Atr_Item_Weight we using ( itemid )
    where SourceKey in (select id from stock)
)
, itwms as (
    select
                            ai.sourcekey as item_id
                         , listagg(distinct ain.name) as tag_name
                    from dwh_data.tie_item_itemtag tii
                             join dwh_data.anc_item ai using (itemid)
                             join dwh_data.atr_itemtag_name ain using (itemtagid)
                             join dwh_data.atr_itemtag_owner aio using (itemtagid)
                             join dwh_data.knot_itemtagowner kio
                                  on kio.itemtagownerid = aio.ownerid and kio.ItemTagOwner in ( 'wms' , 'AMS')
                    where SourceKey in (select id from stock)
                    group by ai.sourcekey
)
select distinct
     --  t.warehouse                  as warehouse
      t.cell_name                  as cell
     , s.id                    as item_id
     , ai.item_name                 as itemName
     , ai.height                    as height
     , ai.width                     as width
     , ai.lenght                    as lenght
     , ai.weight                    as weight
     , ai.volume                    as volume
     , it.tag_name                  as tagItemllWMS
     , qty                          as qty
from  stock s
         join topology t on t.cell_id = s.place_id
         left join itemovh ai on s.id = ai.item_id
         left join itwms it on s.id = it.item_id
--where regexp_like(t.cell_name, '041052');