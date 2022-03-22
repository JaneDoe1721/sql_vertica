select
	    p.posting_id,
        p.posting_number,
        --dp.Number,
        dm.StoreName,
        p.cut_off
	from wms_batching.posting p
    left join dwh.Dim_Posting dp on p.posting_number = dp.number
    left join dwh.Fact_ParcelIncoming_DataMart dm on dm.ParcelRezonSKey = dp.RezonSourceKey
    where p.warehouse_id = 19262731541000
        and cast (p.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'MSK' as smalldatetime) >= '2022-01-21 08:00'
        and cast (p.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'MSK' as smalldatetime) < '2022-02-03 08:00'