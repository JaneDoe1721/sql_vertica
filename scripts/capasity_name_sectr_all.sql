with par as (select w.clearing_id                                   as warehouse_id,
                  w.name,
                  '2021-12-27'::timestamp at time zone w.timezone as bdate,
				  '2022-01-12'::timestamp at time zone w.timezone as edate
             from whc_go_crud_warehouse.warehouses w
             where w.clearing_id = 19262731541000 
 )
     ,
    topology   as (
                   select distinct
                       th1.id as building_id
                     , th2.id as zone_id
                     , th3.id as rack_id
                     , th4.id as cell_id
                     , sectr_inf.name as name_sectr
                     , ci.cell_type_id
                     , ci.height
                     , ci.width
                     , ci."length"
                   from wms_topology.topology_hierarchy th1
                   join wms_topology.topology_hierarchy th2 on th2.parent_id = th1.id and th2.is_deleted = 'false'
                   join wms_topology.topology_hierarchy th3 on th3.parent_id = th2.id and th3.is_deleted = 'false'
                   join wms_topology.topology_hierarchy th4 on th4.parent_id = th3.id and th4.is_deleted = 'false'
                   join wms_topology.sector_info sectr_inf on th2.id = sectr_inf.zone_id 
                   join wms_topology.cell_info          ci on ci.id = th4.id
                   where th1."type" = 1
                     and th1.is_deleted = 'false'
                     and ci.is_deleted = 'false'
                   )
   ,
/* считаем объем товара, лежащего на местах хранени€*/
    item_volume as (
                   select
                       iip.place_id
                     , iip.item_volume
                     , sum( iip.qty_item )           as qty_item
                     , count( distinct iip.qty_sku ) as qty_sku
                   from (
                        select
                            iip.place_id
                          , sum( ir.width * ir.height * ir.depth * 1000 ) as item_volume
                          , sum( iip.quantity )                                         as qty_item
                          , iip.item_id                                                 as qty_sku
                        from wms_csharp_service_storage_all.item_in_place iip
                        join (
            select
               i.sourcekey as id
             , w.Width as width
             , h.Height as height
             , d.Depth as depth
            from dwh_data.anc_item i
            join dwh_data.Atr_Item_Height h using ( itemid )
            join dwh_data.Atr_Item_Depth d using ( itemid )
            join dwh_data.Atr_Item_Width w using ( itemid ))           ir on ir.id = iip.item_id
                        where iip.place_type = 2 /* €чейка*/
                        group by iip.place_id
                               , iip.item_id
                        union all
                        select
                            inip.place_id
                          , sum( ir.width * ir.height * ir.depth * 1000 ) as item_volume
                          , count( inip.id )                                 as qty_item
                          , inip.item_id                                     as qty_item
                        from wms_csharp_service_storage_all.instance_in_place inip
                        join (
            select
               i.sourcekey as id
             , w.Width as width
             , h.Height as height
             , d.Depth as depth
            from dwh_data.anc_item i
            join dwh_data.Atr_Item_Height h using ( itemid )
            join dwh_data.Atr_Item_Depth d using ( itemid )
            join dwh_data.Atr_Item_Width w using ( itemid ))                ir on ir.id = inip.item_id
                        where inip.place_type = 2 /* €чейка*/
                        group by inip.place_id
                               , inip.item_id
                        ) iip
                   group by iip.place_id
                          , iip.item_volume
                   )
select 
    date_trunc('hour',localtimestamp)                                                                                   as data
  --, par.name                                                                                    as ff
  , t.name_sectr
  --, CAST( concat( concat( concat( zi.name , ' (' ) , zi.description ) , ')' ) as varchar(255) ) as zone_name
  --, CAST( ct.purpose as varchar(255) )                                                          as type_name
  , CAST(
            sum( isnull( ct.max_capacity , t.width * t.height * t."length" / 1e6 ) ) as float ) as cell_volume /* за полезный объем принимаем макс_капасити на типе €чейки, если он null, то считаем объем из габаритов €чейки*/
  , CAST( round( isnull( sum( iv.item_volume ) , 0 ) , 2 ) as float )                           as item_volume
  , CAST( round( isnull( sum( iv.item_volume ) , 0 ) /
                 sum( isnull( t.width * t.height * t."length" / 1e6 , ct.max_capacity ) ) ,
                 2 ) as float )                                                                 as useful_storage
  , count( t.cell_id )
  , sum( iv.qty_item )                                                                          as qty_item
  , sum( iv.qty_sku )                                                                           as qty_sku
from topology               t
join wms_topology.cell_type ct on ct.id = t.cell_type_id
join par on par.warehouse_id = ct.warehouse_id
join wms_topology.zone_info zi on zi.id = t.zone_id and zi.zone_type = 3 and zi.is_deleted = 'false'
join wms_topology.rack_info ri
     on ri.id = t.rack_id and ri.purpose_type = 0 and ri.rack_type = 0 and ri.is_deleted = 'false'
left join item_volume       iv on iv.place_id = t.cell_id
group by concat( concat( concat( zi.name , ' (' ) , zi.description ) , ')' )
       , ct.purpose, par.name, t.name_sectr


