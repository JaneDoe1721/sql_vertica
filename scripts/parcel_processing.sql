with par as (
           select
               w.rezon_id
             , w.clearing_id
             , '2022-02-28 00:00:00'::timestamp at time zone w.timezone as bdate --������ ������
             , '2022-03-18 00:00:00'::timestamp at time zone w.timezone as edate --��������� �������
             , w.timezone
             , w.name  from whc_go_crud_warehouse.warehouses w
           where w.name in ('���������_���������'
           --'���������_���', '�����_���������_���', '������_��_����_���',--'������_���_�����', '�����������_���_�����', '���������_���������',
           --'������������_���', '�����_���','������������_���_�����'
                           )
             )
select -- �������� �� WMS
             par.name as "�����"
           , bp.batch_id as "����"
           , p.posting_id as "������� id"
           , pt.tl20_at::timestamptz at time zone par.timezone   as "�������"
           , py.tl20_at::timestamptz at time zone par.timezone   as "������ �������"
           , ppl_1.d_1::timestamptz at time zone par.timezone    as "��������� ��������"
           , q1.name as "����� ����������"
from wms_batching.posting p
         join  
                   par on par.clearing_id = p.warehouse_id
         left join (         --���������� ������ Batch_id & Posting_id
                   select
                       max(bp.batch_id) as batch_id
                     , bp.posting_id
                   from wms_batching.batch_posting bp
                   group by bp.posting_id
                   order by bp.posting_id
                   )
                   --���������� � �������� ��������
                   bp on bp.posting_id = p.posting_id
          left join (
                   select
                      q.name
                    , q.id
                   from  wms_crud_settings_ss.sort_method q
                    ) 
                    q1 on q1.id = p.sort_method_id
         left join (--������� ����� ������������ batch
                  select
                     bsl.batch_id
                   , bsl.created_at as tl20_at
                  from wms_batching.batch_status_log bsl
                  where bsl.status_to_id = 100 -- 100 - ������ ������������ �����
                   )
                   --����������� � �������� ������
                   pt on pt.batch_id = bp.batch_id
                    left join (--������� ����� ������������ batch
                  select
                     bsl.batch_id
                   , bsl.created_at as tl20_at
                  from wms_batching.batch_status_log bsl
                  where bsl.status_to_id = 300 -- 300 - ������ ������ �������
                   )
                   --����������� � �������� ������
                   py on py.batch_id = bp.batch_id
         left join (
                   select
                       max(ppl.created_at) as d_1
                     , ppl.posting_id
                   from wms_csharp_service_packing.packing_posting_log ppl
                   where ppl.operation = 1
                   group by ppl.posting_id
                   order by ppl.posting_id
                   ) ppl_1 on ppl_1.posting_id = p.posting_id
where pt.tl20_at  between par.bdate and par.edate
and p.status_id = 40
order by ppl_1.d_1::timestamptz at time zone par.timezone desc