select u.login,
       ul.name,
       toe.company_name as "Компания",
       u.warehouse_id,
       r.name as "Название роли"
    from wms_service_employee.user_log ul
    join (
        select
             *
            from wms_service_employee."user"
        ) u on u.id = ul.user_id
    left join wms_service_employee.type_of_employment toe on u.type_of_employment_id = toe.id
    left join wms_service_employee.position_1c p1c on u.position_1c_id = p1c.id
    join wms_service_employee.user_role ur on u.id = ur.user_id
    join wms_service_employee.role r on ur.role_id = r.id
    where u.warehouse_id = 19262731541000 and u.is_fired = FALSE
    group by u.login, ul.name,toe.company_name, u.warehouse_id, r.name