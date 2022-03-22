--Выгрузка СТОКА. месторасположения по товару. Для Еженедельного отчёта v2
with cell as (
    select ci.id
         ,ci.full_name
    from wms_topology.cell_info ci
)
   ,item as (
    select i.rezonitemid as Id
         ,i.Tag
    from (select ai.SourceKey as rezonitemid,
			tn.name as Tag,
			yt.name as item_name,
			matrix,
			re.Tag_OVH as Ovh
			from dwh.Fact_Item_ItemTag it
			join dwh_data.Anc_Item ai on it.ItemID = ai.ItemID
    			left join dwh_data.atr_item_name yt on yt.ItemID = ai.ItemID
			join dwh_data.Atr_ItemTag_Name tn on tn.ItemTagID = it.ItemTagID
			left join OP_team.Dim_Item_Matrix ma on ma.sku=ai.SourceKey
			left join OP_team.Item_Tag re on re.RezonItemID = ai.SourceKey) i
)
   , topology as (
    select th4.id              as cell_id
         , ci.full_name        as cell_name
         , zi.name             as zone
         , zi.floor_number + 1 as floor_number
         , si.name             as sector
         , si.id               as id_sector
         , ci.max_capacity
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
             left join (select id, full_name, max_capacity
                        from wms_topology.cell_info
                        where is_deleted = 'false'
                          and id in (
                            select cell.id
                            from cell
                        )) ci on ci.id = th4.id
             join (select id, floor_number, name
                   from wms_topology.zone_info
        --where id = 5372043
    ) zi on th2.id = zi.id
             join (select rack_id, sector_id
                   from wms_topology.sector_racks) sr on th3.id = sr.rack_id
             join (select id, name
                   from wms_topology.sector_info
                   where type = 1
--where type = 2
        --and id in (561, 590, 591)
        --and id in (352, 660, 661, 712, 689) -- паллетка внутри мезонина
    ) si on sr.sector_id = si.id -- and si.type = 1
    where th1."type" = 1
      and th1.is_deleted = 'false'
      and th1.parent_id = 19262731541000
)
   , iip as (
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
    /*, v as (
     select --v.name
   item_name1
 , sv.sku
 , sv.id
 from sellercenter_product_service_data.sku_variant sv
              --join (select v1.id, v1.name
 --from sellercenter_product_service_data.variant v1) v on sv.variant_id = v.id
 left join(*/
   , v as (  select
                 i1.id
                  , i1.name as item_name1
             from rezon.ItemAttributes i1
             where --(i1.name like '%зеркал%' or i1.name like '%Зеркал%' or i1.name like 'Зеркал%'or i1.name like 'зеркал%')
                   --and (i1.name not like '%средство%' or '%настол%' )
                   --and
                   i1.id in (
                       select iip.id
                       from iip
                       --where iip.id in (229858098)
                   )
) 
   , i as (
    select ir.id
         ,ir.serial_number
         ,ir.availability
         ,ir.delivery_schema
         ,sku.width
         ,sku.height
         ,sku.depth
         ,sku.weight
         ,sku.height*sku.depth*sku.width / 10 ^ 6 as v1
         ,rezonitem.Height*rezonitem.Depth*rezonitem.Width/10 ^ 6 as v2
         ,ir.width*ir.height*ir.depth/10^6 as v3
         ,sku.VolumeLiter as v4
         ,idisc.id_disc
         ,idisc.reason_damaged
    from item_source_service.item_rezon ir
             left join (
        select id as id_disc, reason_damaged
        from item_source_service.item_discounted
    )idisc on idisc.id_disc = ir.id
             left join (
        select
            RezonItemID
             ,Width
             ,Height
             ,Depth
             ,Weight
             ,MetazonItemID
             ,ItemGroupID
             ,NameRus
             ,volumeliter
             ,AccountGroupID
        from metazonbeeeye.item
    )sku on sku.RezonItemID = ir.id
        /*left join (
            select RezonItemId,
                   MetazonItemID,
                   ItemGroupID,
                   NameRus,
                   volumeliter,
                   AccountGroupID
            from metazonbeeeye.item) i on i.rezonitemid = ir.id*/
             left join(
        select id, Height, Width, Depth
        from rezon.Item
    )rezonitem on rezonitem.id = ir.id
    where ir.id in (
        select iip.id
        from iip
    )
    --and ir.availability <> 1
)
select iip.id                  as 'ID товара'
     ,v.item_name1                  as 'Название товара'
--,iip.supply_id
--,TagIds                        as 'ID Тэга'
     ,CASE
          when regexp_ilike(TagIds,',4,' ) then 'Закрытая зона'
          when regexp_ilike(TagIds,'247' ) then '1 класс опасности'
          when regexp_ilike(TagIds,'248' ) then '2 класс опасности'
          when regexp_ilike(TagIds,'249' ) then '3 класс опасности'
          when regexp_ilike(TagIds,'250' ) then '4 класс опасности'
          when regexp_ilike(TagIds,'251' ) then '5 класс опасности'
          when regexp_ilike(TagIds,'252' ) then '6 класс опасности'
          when regexp_ilike(TagIds,'253' ) then '7 класс опасности'
          when regexp_ilike(TagIds,'254' ) then '8 класс опасности'
          when regexp_ilike(TagIds,'255' ) then '9 класс опасности'
          when regexp_ilike(TagIds,',37,' ) then 'Опасники'
          when regexp_ilike(TagIds,'569' ) then 'Товары для дома и дачи'
          when regexp_ilike(TagIds,'571' ) then 'Товары для животных'
          when regexp_ilike(TagIds,'565' ) then 'Подгузники и туалетная бумага'
          when regexp_ilike(TagIds,'555' ) then 'Бытовая техника'
          when regexp_ilike(TagIds,'356' ) then 'Продукты +17'
          when regexp_ilike(TagIds,',2,' ) and regexp_ilike(TagIds,'307' ) then 'Жидкость + Еда'
    --when regexp_ilike(TagIds,'362' ) then 'КГТ Мезонин'
          when regexp_ilike(TagIds,',2,' ) then 'Жидкость'---- жидкость приоритетнее еды
          when regexp_ilike(TagIds,',1,' ) then 'Книга'
          when regexp_ilike(TagIds,',47,' ) then 'Одежда'
          when regexp_ilike(TagIds,',48,' ) then 'Обувь'
          when regexp_ilike(TagIds,'307' ) then 'Еда'
          when regexp_ilike(TagIds,'412' ) then 'Косметика'
          else 'Микс товар'
    end as TAG_ST
     , CASE
           when regexp_ilike(TagIds,'210' ) then 'Sort'
           when regexp_ilike(TagIds,'211' ) then 'NonSortM'
           when regexp_ilike(TagIds,'212' ) then 'NonSortP'
           when regexp_ilike(TagIds,'214' ) then 'light&bulky'
           when regexp_ilike(TagIds,'213' ) then 'light&long'
           when regexp_ilike(TagIds,'215' ) then 'heavy&bulky'
           when regexp_ilike(TagIds,'221' ) then 'OverSize'
           else ''
    end as Ovh
     , CASE
           when regexp_ilike(TagIds,',26,') then 'Экз'
           else '!!!'
    end as II
     ,sector                        as 'Сектор'
     ,cell_name                     as 'Ячейка'
--,qnt_cells                     as 'Количество ячеек'
     ,sum(iip.qty)                  as 'Кол-во товара'
--,i.weight/1000                 as 'Вес айтема'
     ,i.weight*sum(iip.qty)/1000    as 'Вес товара в ячейке'
--,i.width
--,i.height
--,i.depth
--,v1
--,v2
--,v3
--,v4
     ,v1*sum(iip.qty)               as 'Объём товара в ячейке'
--, topology.max_capacity        as 'Объём ячейки'
,iip.stock_type                as 'Тип стока'
--,i.availability                as 'Доступность'
--,i.reason_damaged              as 'Причина уценки'
--,i.delivery_schema             as 'Чей товар'
--,to_char(max(replenishment_time)::timestamptz at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as 'Когда принято реплеем'
--,to_char(max(placing_time)::timestamptz at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as 'Когда размещено реплеем'
--,Price
--,BasePrice
--,DiscountPrice
--,case when i.id_disc > 0 then 'Уценка' end as Ycenka
from iip
         join item on iip.Id = item.Id
         left join (
    select RezonItemID,BasePrice, Price, sum(Price-Discount) as DiscountPrice
    from metazonbeeeye.Item bp1
    group by RezonItemID, Price,BasePrice)bp1 on bp1.RezonItemID = item.Id
         join topology on topology.cell_id = iip.place_id
/*left join (
    select ml1.item_id , max(ml1.at) as replenishment_time ,ml1.supply_id--, ml1.from_id
    from wms_csharp_service_storage_spb.movement_log ml1
    --where reason = 5
    where reason = 37 --вход реплей
    group by ml1.item_id, ml1.supply_id
    )ml1 on ml1.item_id = item.Id and ml1.supply_id = iip.supply_id
left join (
    select ml2.item_id, ml2.to_id, max(ml2.at) as placing_time ,ml2.supply_id--, ml1.from_id
    from wms_csharp_service_storage_spb.movement_log ml2
    --where reason = 5
    where reason = 33 -- размещение
    group by ml2.item_id, ml2.to_id, ml2.supply_id
    )ml2 on ml2.item_id = ml1.item_id and ml2.to_id = iip.place_id and ml2.supply_id = ml1.supply_id*/
         join v on v.id = iip.id
         join i on i.id = iip.id
     --where iip.id = 185700657
     --where BasePrice > 10000
--where stock_type = 1
--and (ovh = 'Oversize' or tag_st ='null')
--and last_pick_up < '2020-12-31'
group by iip.id
       , v.item_name1
       , ovh
       , TAG_ST
       , II
       , sector
       , cell_name
       , v1
       --, v2
       --, v3
       --, v4
       --, TagIds
       , iip.stock_type
       --, i.availability
       --, i.delivery_schema
       --, topology.max_capacity
       --, replenishment_time
       --, placing_time
       --, qnt_cells
       --, qnt_cells
       --, Price,BasePrice
       --, DiscountPrice
       --, i.id_disc
--,iip.supply_id
       --, i.reason_damaged
       --, Ycenka
       --,i.width
       --,i.height
       --,i.depth
       ,i.weight