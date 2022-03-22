with item as (select distinct
                            ai.sourcekey as item_id
                            --ain.ItemTagId
                            ,listagg(distinct ain.Name) as TagName
                            ,listagg(distinct ain.ItemTagId) as TagIDs
                            --,kio.ItemTagOwner
                    from dwh_data.tie_item_itemtag tii
                             join dwh_data.anc_item ai using (itemid)
                             join dwh_data.atr_itemtag_name ain using (itemtagid)
                             join dwh_data.atr_itemtag_owner aio using (itemtagid)
                             join dwh_data.knot_itemtagowner kio
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
           , 1775465531864137165 -- 'Вода'
           , 7064590679319570490 -- 'Бытовая химия'
           , 6522452963607389817 -- 'Книга'
           , 2747538581386642344 -- 'Одежда, Текстиль, Сумки'
           , 4980143551095494073 -- 'Обувь'
           , 1543024544232972842 -- 'Продукты'
           , 6051268096596768889 -- 'Игра, Канцтовары, Товары для детей'
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
           , 1697110219020307156 -- Паллетка
           )
        group by  ai.sourcekey
    )
select
       u.name as 'Пользователь',
       tp.reason_id as 'Номер задания',
       tp.supply_id as 'Номер поставки',
       to_char(tl.at_ + interval '3 hour', 'YYYY-MM-DD HH24:MI:SS') as 'Начало задания',
       to_char(first_pl_pick + interval '3 hour','YYYY-MM-DD HH24:MI:SS') as 'Первый принятый товар',
       to_char(q.at_ + interval '3 hour', 'YYYY-MM-DD HH24:MI:SS') as 'Окончание задания',
       to_char(((q.at_ + interval '3 hour') - (tl.at_ + interval '3 hour')), 'HH:MI:SS') as 'Время выполнения задания',
       to_char(((first_pl_pick + interval '3 hour') - (tl.at_ + interval '3 hour')), 'HH24:MI:SS') as 'Время до превого принятого товара',
       case when to_char(tl.at_ + interval '3 hour', 'HH24:mi:ss') < '08:00:00'
		 then to_char((tl.at_ - 1) + interval '3 hour', 'dd.MM.yyyy')
		 else to_char(tl.at_ + interval '3 hour', 'dd.MM.yyyy') end as date,
       count_ as 'Кол-во принятого товара',
       tsk_ovh.volume as volume,
       tsk_ovh.weight as weight,
       CASE
          when regexp_ilike(TagIDs,'9155987984438942942' ) then 'Закрытая зона'
          when regexp_ilike(TagIDs,'4277141987851467413' ) then 'Класс опасности'
          when regexp_ilike(TagIDs,'5091327832147112866' ) then 'Класс опасности'
          when regexp_ilike(TagIDs,'8689657220703050304' ) then 'Класс опасности'
          when regexp_ilike(TagIDs,'6082072584947048150' ) then 'Класс опасности'
          when regexp_ilike(TagIDs,'8021102035680062104' ) then 'Класс опасности'
          when regexp_ilike(TagIDs,'9151922122914558308' ) then 'Класс опасности'
          when regexp_ilike(TagIDs,'716321342426122317' ) then 'Класс опасности'
          when regexp_ilike(TagIDs,'4230875389627088545' ) then 'Класс опасности'
          when regexp_ilike(TagIDs,'604157070827996885' ) then 'Класс опасности'
          when regexp_ilike(TagIDs,'4504569599984049805' ) then 'Опасники'
          when regexp_ilike(TagIDs,'1775465531864137165')  then 'Вода'
          when regexp_ilike(TagIDs,'8845881085734335059' ) then 'Продукты'
          when regexp_ilike(TagIDs,'1543024544232972842' ) then 'Продукты'
          when regexp_ilike(TagIDs,'7064590679319570490' ) then 'Бытовая химия'
          when regexp_ilike(TagIDs,'7283246062394458654' ) then 'Товары для животных'
          when regexp_ilike(TagIDs,'6051268096596768889' ) then 'Игра, Канцтовары, Товары для детей'
          when regexp_ilike(TagIDs,'6173212468840074022' ) then 'Подгузники и туалетная бумага'
          when regexp_ilike(TagIDs,'7140891154007995232' ) then 'Бытовая техника'
          when regexp_ilike(TagIDs,'6522452963607389817' ) then 'Книга'
          when regexp_ilike(TagIDs,'2747538581386642344' ) then 'Одежда, Текстиль, Сумки'
          when regexp_ilike(TagIDs,'4980143551095494073' ) then 'Обувь'
          when regexp_ilike(TagIDs,'1961279131436568296' ) then 'Товары для дома и дачи'
          when regexp_ilike(TagIDs,'1697110219020307156' ) then 'Паллетка'
          else 'Микс товар' end as TAG_ST
     ,CASE
           when regexp_ilike(TagIDs,'7696218150898608887' ) then 'Sort'
           when regexp_ilike(TagIDs,'9216358951387548216' ) then 'Sort'
           when regexp_ilike(TagIDs,'4217572752416528871' ) then 'НонСорт-Мезонин'
           when regexp_ilike(TagIDs,'7758223693989033451' ) then 'НонСорт-стеллаж'
           when regexp_ilike(TagIDs,'3170660884653643404' ) then 'Лёгкий крупногабарит'
           when regexp_ilike(TagIDs,'8089256920405702796' ) then 'Лёгкий Длинномер'
           when regexp_ilike(TagIDs,'3745374236461014676' ) then 'Длинномер'
           when regexp_ilike(TagIDs,'5849049604398793466' ) then 'Крупногабарит'
           when regexp_ilike(TagIDs,'3589162341112388495' ) then 'OverSize'
           else 'Без тега'
    end  as Ovh
    from wms_csharp_service_storage_all.movement_log tp
    join wms_csharp_service_supply.supply_log sl on sl.warehouse_id = 19262731541000
    join (select
                tl1.task_id,
                max(tl1.at) as at_,
                t.user_id
            from wms_csharp_service_task_raw.tasks_log tl1
        join wms_csharp_service_task.tasks t on t.id = task_id
    where tl1.status = 20
        and tl1.user_id is not null
    and warehouse_id = 19262731541000
        group by tl1.task_id, t.user_id) tl on tl.task_id = tp.reason_id
    join (select
                q.task_id,
                max(q.at) as at_
            from wms_csharp_service_task.tasks_log q
        join wms_csharp_service_task.tasks f on f.id = task_id
    where q.status = 30
    and f.warehouse_id = 19262731541000
        group by q.task_id) q on q.task_id = tp.reason_id
    left join (select
                reason_id,
                min(at) as first_pl_pick
            from wms_csharp_service_storage_all.movement_log
    where item_id is not null
    and to_type = 3
        group by  reason_id) ml_fist on ml_fist.reason_id = tp.reason_id
    join (select
			r1.reason_id,
            --r1.item_id as item_id,
			--count(r1.id) - count(r1.quantity) as ww,
			sum(r1.quantity) as count_
			--r1.supply_id
		    from wms_csharp_service_storage_all.movement_log r1
	    where to_type = 3 and reason = 19 and to_stock_type = 1  and warehouse_id = 19262731541000
		group by r1.reason_id) namber_id on namber_id.reason_id = tp.reason_id
    join wms_service_employee."user" u on u.id = tl.user_id
    join (
        select
                tp.task_id,
                sum(volume) as volume,
                sum(weight) as weight
        from wms_inbound.inbound_task_progress tp
    join (
             select
             i.sourcekey as item_id
            , w.Width as width
            , h.Height as height
            , d.Depth as depth
            , we.Weight as weight
            , (w.Width * h.Height * d.Depth) * 1000 as volume
            from dwh_data.anc_item i
            left join dwh_data.Atr_Item_Height h using ( itemid )
            left join dwh_data.Atr_Item_Depth d using ( itemid )
            left join dwh_data.Atr_Item_Width w using ( itemid )
            left join dwh_data.Atr_Item_Weight we using ( itemid )
        ) ai on ai.item_id = tp.item_id
        group by tp.task_id) tsk_ovh on tsk_ovh.task_id = tp.reason_id
    left join item on tp.item_id = item.item_id
where tp.supply_id = sl.supply_id
    and cast (tl.at_ AT TIME ZONE 'UTC' AT TIME ZONE 'MSK' as smalldatetime) >= '2022-02-28 00:00'
    and cast (tl.at_ AT TIME ZONE 'UTC' AT TIME ZONE 'MSK' as smalldatetime) < '2022-03-18 00:00'
group by
    u.name,
    tp.reason_id,
    tp.supply_id,
    tl.at_,
    first_pl_pick,
    q.at_,
    date,
    count_,
    volume,
    weight,
    item.TagIDs
order by
    date,
    u.name,
    tl.at_