select distinct
                user_.name,
                p.posting_number,
                ppl.created_at
from wms_csharp_service_packing.packing_posting_log as ppl
join wms_service_employee."user" as user_ on user_.id = ppl.user_id
join wms_batching.posting as p on ppl.posting_id = p.posting_id
where ppl.warehouse_id = 19262731541000 and ppl.created_at >= '2022-02-10'
