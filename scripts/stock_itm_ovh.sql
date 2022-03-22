with  stock as (
    select iip.warehouse_id
         , iip.stock_type
         , iip.place_type
         , iip.place_id
         , iip.item_id
         , null         as instance_id
         , iip.quantity as qty
         , iip.bunch_id as bunch_id
         , b.supply_id  as supply_id
         , max(itp.at)  as at
    from wms_csharp_service_storage_all.item_in_place iip
             left join (
        select itp.item_id
             , itp.at
        from wms_inbound.inbound_task_progress itp
    ) itp on (iip.item_id = itp.item_id)
             left join (select b.id
                             , b.supply_id
                        from wms_csharp_service_supply.bunch b
    ) b on b.id = iip.bunch_id
    where iip.warehouse_id = 19262731541000
    group by iip.item_id
           , b.supply_id
           , iip.warehouse_id
           , iip.stock_type
           , iip.place_type
           , iip.place_id
           , iip.item_id
           , null
           , iip.quantity
           , iip.bunch_id
           , null
    union all
    select iip.warehouse_id
         , iip.stock_type
         , iip.place_type
         , iip.place_id
         , iip.item_id
         , iip.id        as instance_id
         , 1             as qty
         , null          as bunch_id
         , iip.supply_id as supply_id
         , itp.at        as at
    from wms_csharp_service_storage_all.instance_in_place iip
             left join (select itp.item_id
                             , max(itp.at) as at
                             , itp.supply_id
                        from wms_inbound.inbound_task_progress itp
                        group by itp.item_id
                               , itp.supply_id
    ) itp on (iip.supply_id = itp.supply_id and iip.item_id = itp.item_id)
    where iip.warehouse_id = 19262731541000
      and iip.stock_type = 1
)
select distinct
            i.sourcekey as item_id
            , stock.stock_type as stock_type
            , sum(stock.qty) as qty
            , n.name      as item_name
            , h.Height as height
            , d.Depth as lenght
            , w.Width as width
            , we.Weight as weight
            , (h.Height * d.Depth * w.Width ) * 1000 as volume
            from dwh_data.anc_item i
            join stock on stock.item_id = i.SourceKey
            left join dwh_data.atr_item_name n using ( itemid )
            left join dwh_data.Atr_Item_Height h using ( itemid )
            left join dwh_data.Atr_Item_Depth d using ( itemid )
            left join dwh_data.Atr_Item_Width w using ( itemid )
            left join dwh_data.Atr_Item_Weight we using ( itemid )
where stock.stock_type = 1
group by
    i.sourcekey, stock.stock_type, n.name, h.Height, d.Depth , w.Width , we.Weight


