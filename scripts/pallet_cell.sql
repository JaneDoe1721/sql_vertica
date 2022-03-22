select distinct
                ci.id,
                pt.task_id
   --  , ci.full_name as "ίχεικΰ"
     --, case
        -- when ct.type = 'Pallet' then 'Οΰλλες'
       --  end
       -- as "Òθο ÿχεικθ"
                      from wms_topology.topology_hierarchy th1
                               join (select id, parent_id
                                     from wms_topology.topology_hierarchy
                                     where is_deleted = 'false') th2
                                    on th2.parent_id = th1.id
                               join (select id, parent_id
                                     from wms_topology.topology_hierarchy
                                     where is_deleted = 'false') th3
                                    on th3.parent_id = th2.id
                               join (select id, parent_id
                                     from wms_topology.topology_hierarchy
                                     where is_deleted = 'false') th4
                                    on th4.parent_id = th3.id
                               join (select *
                                     from wms_topology.cell_info
                                     where is_deleted = 'false'
                                       ) ci on ci.id = th4.id
                               join (select id, floor_number, description
                                     from wms_topology.zone_info) zi on th2.id = zi.id
                               join (select rack_id, sector_id
                                     from wms_topology.sector_racks) sr on th3.id = sr.rack_id
                               join (select id, closed_for_placing, coordinate_x, coordinate_y, name, description
                                     from wms_topology.rack_info
                                   ) ri on ri.id = sr.rack_id
                               join whc_go_crud_warehouse.warehouses w on w.clearing_id = th1.parent_id
                               join wms_topology.cell_type ct on ci.cell_type_id = ct.id
                               join wms_csharp_service_task.placing_tasks pt on pt.start_cell_id = ci.id
where th1."type" = 1
and ct.type = 'Pallet'
and th1.is_deleted is false
and w.is_sandbox is false
and w.clearing_id = 19262731541000
