with par as (
            select
                storage.clearing_id                                   as warehouse_id
              , '2022-02-28'::timestamp at time zone storage.timezone as bdate --начало периода
              , '2022-03-18'::timestamp at time zone storage.timezone as edate --окончание периода
              , storage.timezone
              , storage.name
            from whc_go_crud_warehouse.warehouses storage
            where storage.clearing_id = 19262731541000 --название ФФ
            )
select
	method_sort.sort_method_name,
	task.task_id,
--	ppl1.batch_id,
	user_.name,
	case when to_char(p.at::timestamptz at time zone par.timezone, 'HH24:mi:ss') < '08:00:00'
         then to_char((p.at - 1)::timestamptz at time zone par.timezone, 'dd.MM.yyyy')
         else to_char(p.at::timestamptz at time zone par.timezone, 'dd.MM.yyyy') end as date,
	count(ppl1.posting_id) as 'кол-во постингов',
	to_char(min(spp.item_pick)::timestamptz at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as 'Дата начала упаковки постинга',
	to_char(p.at::timestamptz at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as 'Дата начала задания',
	to_char(y.at::timestamptz at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as 'Дата окончания задания'
	from wms_csharp_service_packing.packing_tasks as task
join (
	select
    ppl1.task_id,
    ppl1.batch_id,
    ppl1.posting_id,
    ppl1.user_id,
    ppl1.host_name,
    ppl1.warehouse_id
        from wms_csharp_service_packing.packing_posting_log ppl1
    join par on par.warehouse_id = ppl1.warehouse_id
        where operation = 1
        and cast(ppl1.created_at AT TIME ZONE 'UTC' AT TIME ZONE par.timezone as smalldatetime) between bdate and edate
        group by ppl1.task_id, ppl1.batch_id, ppl1.posting_id, ppl1.user_id, ppl1.host_name, ppl1.warehouse_id
    ) ppl1 on ppl1.task_id = task.task_id
join par on par.warehouse_id = ppl1.warehouse_id
left join (
     select p1.at,
           p1.task_id
     from wms_csharp_service_task.tasks_log p1
     where p1.status = 20
     ) p on p.task_id = task.task_id
left join (
     select y1.at,
           y1.task_id
     from wms_csharp_service_task.tasks_log y1
     where y1.status = 30
     ) y on y.task_id = task.task_id
left join (
     select max(p1.created_at) as item_pick,
           p1.posting_id,
           p1.task_id,
           p1.batch_id
     from wms_csharp_service_packing.packing_posting_log p1
     where p1.operation = 0
    group by p1.posting_id, p1.task_id, p1.batch_id
     ) spp on spp.posting_id = ppl1.posting_id and spp.batch_id = ppl1.batch_id and spp.task_id=ppl1.task_id
left join (
     select y1.created_at,
           y1.task_id,
           y1.batch_id,
           y1.posting_id
     from wms_csharp_service_packing.packing_posting_log y1
     where y1.operation = 1
    group by y1.task_id, y1.batch_id, y1.posting_id, y1.created_at
     ) epp on epp.posting_id = ppl1.posting_id and epp.batch_id = ppl1.batch_id and epp.task_id=ppl1.task_id
left join wms_batching.batch b on b.batch_id = ppl1.batch_id
left join (
        select method_sort.id   as sort_method_id,
             method_sort.name as sort_method_name
        from wms_crud_settings_ss.sort_method method_sort
    ) method_sort on method_sort.sort_method_id = b.sort_method_id
join wms_service_employee."user" as user_ on user_.id = ppl1.user_id --тяну айди пользователя для имени
join (select to_char(min(at)::timestamptz at time zone 'MSK', 'dd.MM.yyyy') as first_date, user_id
    from wms_service_employee.user_log
    group by user_id) ul on ul.user_id = ppl1.user_id
left join (
select
     pn1.posting_number,
     pn1.posting_id
    from wms_batching.posting pn1
    ) pn1 on pn1.posting_id = ppl1.posting_id
left join (
    select
    iid1.posting_id,
    iid1.batch_id,
    isnull(count(iid1.packing_batch_item_id), 0) as qty
    from wms_csharp_service_packing.packing_batch_items iid1
    where status = 2
        group by iid1.posting_id,
                 iid1.batch_id
    ) iid1 on iid1.posting_id = ppl1.posting_id and iid1.batch_id = ppl1.batch_id
left join (
    select distinct
    pbp1.posting_id,
    pbp1.batch_id,
    pbp1.container_id
    from wms_csharp_service_packing.packing_batch_postings pbp1
    group by pbp1.posting_id, pbp1.batch_id, pbp1.container_id
        ) pbp1 on pbp1.posting_id = ppl1.posting_id and pbp1.batch_id = ppl1.batch_id
where       p.at between bdate and edate
group by
par.name,
par.timezone,
date,
user_.name,
ul.first_date,
task.task_id,
ppl1.batch_id,
method_sort.sort_method_name,
to_char(p.at::timestamptz at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS'),
to_char(y.at::timestamptz at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS')
order by 
date,
user_.name,
to_char(p.at::timestamptz at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS')

