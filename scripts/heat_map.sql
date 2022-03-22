select w.name as "�����"
     , ci.full_name as "������"
     /*, case
         when ct.type = 'Tray' then '�����'
         when ct.type = 'Pallet' then '������'
         when ct.type = 'Shelf' then '�����'
         when ct.type = 'Pocket' then '������'
         when ct.type = 'HungUp' then '������'
         when ct.type = 'Box' then '�����'
         else 'Dividers'
         end
        as "��� ������"*/
     , ci.restriction_tag_aliases as "���� ������"
--     , ci.width as "������ (��)"
--     , ci.height as "������ (��)"
--     , ci.length as "������� (��)"
     , ci.sku_qty as "���-�� ���������� SKU"
     , ci.min_sku_dim as "����������� ������� SKU (min)"
     , ci.max_min_sku_dim as "����������� ������� SKU (max)"
     , ci.min_max_sku_dim as "������������ ������� SKU (min)"
     , ci.max_sku_dim as "������������ ������� SKU (max)"
     , ci.min_avg_sku_dim as "������� ������� SKU (min)"
     , ci.max_avg_sku_dim as "������� ������� SKU (max)"
     , ci.max_sku_weight as "������������ ��� SKU"
     , ci.max_capacity as "������������ ����� ��������"
     , to_char(((ci.width * ci.height * ci.length)/1e6), '99999.99' ) as "����� ������"
     , to_char((ci.max_capacity / ((ci.width * ci.height * ci.length)/1e6)), '999.99' ) as "����������� %"
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
where th1."type" = 1
and th1.is_deleted is false
and w.is_sandbox is false
and w.clearing_id = 19262731541000
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

