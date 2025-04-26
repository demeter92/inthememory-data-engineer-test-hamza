select * from iceberg.db.transactions;
select count(*) from iceberg.db.transactions;


SELECT snapshot_id
FROM iceberg.db."transactions$snapshots"
ORDER BY committed_at DESC;

SELECT * from iceberg.db."transactions$history";

drop table iceberg.db.transactions;