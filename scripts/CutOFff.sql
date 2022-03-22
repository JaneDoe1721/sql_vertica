with par as (select w.clearing_id                                        as warehouse_id
                  , w.timezone
                  , w.name
                  , '2022-03-14 03:00'::timestamp at time zone w.timezone as bdate /* ���� ������ �������, ��������������� �� ������� ���� ������*/
                  , '2022-03-17 03:00'::timestamp at time zone w.timezone as edate /* ���� ��������� �������, ��������������� �� ������� ���� ������*/
             from whc_go_crud_warehouse.warehouses w
             where w.clearing_id = 19262731541000 /* ���������_���������*/
)
   , co as (select min(ts.DateFrom) as DateFrom
                  ,ts.PostingID
                  ,max(ts.CutOffTime) as CutOff
                  ,row_number() over (partition by ts.PostingID order by ts.DateFrom) as rn
             from oms.PostingTimeSlotLog ts group by ts.PostingID, ts.DateFrom, ts.CutOffTime
)
select
		 p.posting_number as '�������',
        co.DateFrom  as "�������",
        co.CutOff  as "CutOff"
from wms_batching.posting p
		join par on par.warehouse_id = p.warehouse_id
		join co on co.PostingID = p.posting_id
where co.DateFrom between par.bdate and par.edate