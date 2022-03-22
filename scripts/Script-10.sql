select ppl.created_at, ppl.id
from wms_csharp_service_packing.packing_posting_log as ppl
where ppl.created_at >= date '2022-03-14' and warehouse_id = 19262731541000
order by 1 desc;