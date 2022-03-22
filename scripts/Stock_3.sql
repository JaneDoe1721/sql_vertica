--Выгрузка СТОКА. месторасположения по товару. Для Еженедельного отчёта v2
with cell as (
    select ci.id
         ,ci.full_name
    from wms_topology.cell_info ci
)
   ,item as (
       select distinct
                            ai.sourcekey as item_id
                            ,n.Name as item_name
                            --,ain.ItemTagId
                            ,listagg(distinct ain.Name) as TagName
                            ,listagg(distinct ain.ItemTagId) as TagIDs
                            , w.Width as width
                            , h.Height as height
                            , d.Depth as depth
                            , cast(round(Width*Height*Depth*1000,2) as float8) as volume
                            , we.Weight as weight
                            --,kio.ItemTagOwner
                    from dwh_data.tie_item_itemtag tii
                        left join dwh_data.atr_item_name n using ( itemid )
                        left join dwh_data.Atr_Item_Height h using ( itemid )
                        left join dwh_data.Atr_Item_Depth d using ( itemid )
                        left join dwh_data.Atr_Item_Width w using ( itemid )
                        left join dwh_data.Atr_Item_Weight we using ( itemid )
                        left join dwh_data.anc_item ai using (itemid)
                        left join dwh_data.atr_itemtag_name ain using (itemtagid)
                        left join dwh_data.atr_itemtag_owner aio using (itemtagid)
                        left join dwh_data.knot_itemtagowner kio
                                  on kio.itemtagownerid = aio.ownerid and kio.ItemTagOwner in ( 'wms' , 'AMS')
       where ain.ItemTagId in (
                           9155987984438942942 -- 'Закрытая зона'
           , 4277141987851467413 -- '1 класс опасности'
           , 5091327832147112866 -- '2 класс опасности'
           , 8689657220703050304 -- '3 класс опасности'
           , 6082072584947048150 -- '4 класс опасности'
           , 8021102035680062104 -- '5 класс опасности'
           , 9151922122914558308 -- '6 класс опасности'
           , 716321342426122317 -- '7 класс опасности'
           , 4230875389627088545 -- '8 класс опасности'
           , 604157070827996885 -- '9 класс опасности'
           , 4504569599984049805 -- 'Опасники'
           , 1961279131436568296 -- 'Товары для дома и дачи'
           , 7283246062394458654 -- 'Товары для животных'
           , 6173212468840074022 -- 'Подгузники и туалетная бумага'
           , 7140891154007995232 -- 'Бытовая техника'
           , 8845881085734335059 -- 'Продукты +17'
           , 7064590679319570490 -- 'Бытовая химия'
           , 1775465531864137165 -- 'Вода'
           , 6051268096596768889 -- 'Игра, Канцтовары, Товары для детей'
           , 6522452963607389817 -- 'Книга'
           , 2747538581386642344 -- 'Одежда, Текстиль, Сумки'
           , 4980143551095494073 -- 'Обувь'
           , 1543024544232972842 -- 'Продукты'
           , 9216358951387548216 -- Sort
           , 7696218150898608887 -- Sort
           , 4217572752416528871 -- НонСортМ
           , 7758223693989033451 -- НонСорт-стеллаж
           , 5849049604398793466 -- Крупногабарит
           , 3170660884653643404 -- Лёгкий крупногабарит
           , 8089256920405702796 -- Лёгкий Длинномер
           , 3745374236461014676 -- Длинномер
           , 3589162341112388495 -- OverSize
           , 3038921900645476706 -- КГТ-Мезонин
           , 7402551360560286616 -- Хрупкий
           , 7584565283120360546 -- Яд
           , 8527555746102696740 -- Экземплярный
           )
group by  ai.sourcekey, n.Name, w.Width, h.Height, d.Depth, we.Weight
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
                   where is_deleted = 'false'
                 ) th4 on th4.parent_id = th3.id -- and th4.is_deleted = 'false'
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
        --and id in (396, 946)
        --and id = 482
        --and id in (352, 660, 661, 712, 689) -- паллетка внутри мезонина
    ) si on sr.sector_id = si.id -- and si.type = 1
    where th1."type" = 1
      and th1.is_deleted = 'false'
      and th1.parent_id =
        19262731541000 -- Хоругвино_НЕГАБАРИТ
      --and si.id in (413, 908) --фронталка + паллетка у фронталки
     and si.id in (611, 130)
       --and si.id = 236 -- Закрытая Зона
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
   , i as (
    select ir.id
         ,ir.serial_number
         ,ir.availability
         ,ir.delivery_schema
         ,idisc.id_disc
         ,idisc.reason_damaged
    from item_source_service.item_rezon ir
             left join (
        select id as id_disc, reason_damaged
        from item_source_service.item_discounted
    )idisc on idisc.id_disc = ir.id
             /*left join (
        select
            RezonItemID
             ,MetazonItemID
             ,ItemGroupID
             ,NameRus
             ,AccountGroupID
        from beeeye.item
    )sku on sku.RezonItemID = ir.id
    where ir.id in (
        select iip.id
        from iip
    )*/
    --and ir.availability <> 1
)
select iip.id                  as 'ID товара'
     ,item.item_name                  as 'Название товара'
     --,iip.supply_id
     ,CASE
          when regexp_ilike(TagIDs,'9155987984438942942' ) then 'Закрытая зона'
          when regexp_ilike(TagIDs,'4277141987851467413' ) then '1 класс опасности'
          when regexp_ilike(TagIDs,'5091327832147112866' ) then '2 класс опасности'
          when regexp_ilike(TagIDs,'8689657220703050304' ) then '3 класс опасности'
          when regexp_ilike(TagIDs,'6082072584947048150' ) then '4 класс опасности'
          when regexp_ilike(TagIDs,'8021102035680062104' ) then '5 класс опасности'
          when regexp_ilike(TagIDs,'9151922122914558308' ) then '6 класс опасности'
          when regexp_ilike(TagIDs,'716321342426122317' )  then '7 класс опасности'
          when regexp_ilike(TagIDs,'4230875389627088545' ) then '8 класс опасности'
          --when regexp_ilike(TagIDs,'604157070827996885' ) then '9 класс опасности'
          when regexp_ilike(TagIDs,'4504569599984049805' ) then 'Опасники'
          when regexp_ilike(TagIDs,'8845881085734335059' ) then 'Продукты +17'
          when regexp_ilike(TagIDs,'4980143551095494073' ) then 'Обувь'
          when regexp_ilike(TagIDs,'7064590679319570490' ) then 'Бытовая химия'
          when regexp_ilike(TagIDs,'1775465531864137165')  then 'Вода'
          when regexp_ilike(TagIDs,'1543024544232972842' ) then 'Продукты'
          when regexp_ilike(TagIDs,'2747538581386642344' ) then 'Одежда, Текстиль, Сумки'
          when regexp_ilike(TagIDs,'7140891154007995232' ) then 'Бытовая техника'
          when regexp_ilike(TagIDs,'6051268096596768889' ) then 'Игра, Канцтовары, Товары для детей'
          when regexp_ilike(TagIDs,'7283246062394458654' ) then 'Товары для животных'
          when regexp_ilike(TagIDs,'6173212468840074022' ) then 'Подгузники и туалетная бумага'
          when regexp_ilike(TagIDs,'6522452963607389817' ) then 'Книга'
          when regexp_ilike(TagIDs,'1961279131436568296' ) then 'Товары для дома и дачи'
          else 'Микс товар'
    end as TAG_ST
     , CASE
           when regexp_ilike(TagIDs,'7696218150898608887' ) then 'Sort'
           when regexp_ilike(TagIDs,'9216358951387548216' ) then 'Sort'
           when regexp_ilike(TagIDs,'4217572752416528871' ) then 'NonSortM'
           when regexp_ilike(TagIDs,'7758223693989033451' ) then 'NonSortP'
           when regexp_ilike(TagIDs,'3170660884653643404' ) then 'light&bulky'
           when regexp_ilike(TagIDs,'8089256920405702796' ) then 'light&long'
           when regexp_ilike(TagIDs,'3745374236461014676' ) then 'long'
           when regexp_ilike(TagIDs,'5849049604398793466' ) then 'heavy&bulky'
           when regexp_ilike(TagIDs,'3589162341112388495' ) then 'OverSize'
           else ''
    end as Ovh
     , CASE
          when regexp_ilike(TagIDs,'3038921900645476706' ) then 'КГТ Мезонин'
              end as KGT_M
     , CASE
          when regexp_ilike(TagIDs,'7402551360560286616' ) then 'Хрупкий'
              end as Fragile
     , CASE
          when regexp_ilike(TagIDs,'7584565283120360546' ) then 'Яд'
              end as Poison
     , CASE
           when regexp_ilike(TagIDs,'8527555746102696740') then 'Экз'
           else '!!!'
    end as II
     ,sector                        as 'Сектор'
     ,cell_name                     as 'Ячейка'
--,qnt_cells                     as 'Количество ячеек'
     ,sum(iip.qty)                  as 'Кол-во товара'
--,i.weight/1000                 as 'Вес айтема'
     --,i.weight*sum(iip.qty)/1000    as 'Вес товара в ячейке'
,volume
     ,volume*sum(iip.qty)               as 'Объём товара в ячейке'
--, topology.max_capacity        as 'Объём ячейки'
--,iip.stock_type                as 'Тип стока'
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
         join item on iip.Id = item.item_id
         /*left join (
    select RezonItemID,BasePrice, Price, sum(Price-Discount) as DiscountPrice
    from beeeye.Item bp1
    group by RezonItemID, Price,BasePrice)bp1 on bp1.RezonItemID = item.item_id*/
         join topology on topology.cell_id = iip.place_id
         --join volume on volume.item_id = item.item_id
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
         --join i on i.id = iip.id
where stock_type = 1  --вот тут я отсекаю всё кроме валидного товара
--and item.item_id = 146427170
--and last_pick_up < '2020-12-31'
group by iip.id
       , item.item_name
       , ovh
       , TAG_ST
       , II
       , Poison
       , KGT_M
       , Fragile
       , sector
       , cell_name
       --, iip.stock_type
       --, i.availability
       --, i.delivery_schema
       --, topology.max_capacity
       --, replenishment_time
       --, placing_time
       --, qnt_cells
       --, Price,BasePrice
       --, DiscountPrice
       --, i.id_disc
--,iip.supply_id
       --, i.reason_damaged
       --, Ycenka
       , volume
order by sector,cell_name,iip.id