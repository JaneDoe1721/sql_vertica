select distinct
                ml.user_id,
                ul.name,
                toe.company_name as "Компания",
                p1c.name as "Должность",
                listagg(distinct r.name) as "Название роли"

from wms_csharp_service_storage_all.movement_log ml
    join (
        select user_id,
            name,
            min(at) as exp
        from wms_service_employee.user_log
        group by user_id, name
        ) ul on ml.user_id = ul.user_id
    join (
        select
             *
            from wms_service_employee."user"
        ) u on u.id = ul.user_id
    left join wms_service_employee.type_of_employment toe on u.type_of_employment_id = toe.id
    left join wms_service_employee.position_1c p1c on u.position_1c_id = p1c.id
    join wms_service_employee.user_role ur on u.id = ur.user_id
    join wms_service_employee.role r on ur.role_id = r.id
    join (select
                ml.user_id,
                (MIN (ml.at) + (3/24))::date AS startmom
                from wms_csharp_service_storage_all.movement_log ml
                group by user_id) t2 on t2.user_id = ul.user_id
where ml.warehouse_id = 19262731541000
        --and cast(ml.at as date) >= '2022-01-01'
        --and cast(ml.at as date) < '2022-02-01'
        --and regexp_ilike(r.name,'Водитель' )
group by
    ml.user_id,
    ul.name,
    toe.company_name,
    ul.exp,
    p1c.name,
    t2.startmom

