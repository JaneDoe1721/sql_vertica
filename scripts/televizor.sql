--select p.PostingStateID from oms.Posting p where p.CreatedAt > '2022-02-07 00:00:00' and p.PostingStateID in (350,50)
--select * from oms.Shipment limit 100
select
    p.ID,
    p.CreatedAt,
	--pn.posting_id,
	pbi.item_id,
	pbi.name
from oms.Posting p
right join wms_csharp_service_packing.packing_batch_items as pbi on pbi.posting_id = p.ID
where /*p.CreatedAt > '2022-02-07 00:00:00'*/ p.SellerWarehouseID = 19262731541000 and regexp_ilike(pbi.name,'Телевизор' ) and p.PostingStateID in (350,50)







