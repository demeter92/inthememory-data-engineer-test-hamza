SELECT * from iceberg.db.products LIMIT 10;
SELECT count(*) from iceberg.db.products;
SELECT * FROM iceberg.db."products$manifests";
SELECT * from iceberg.db."products$properties";
SELECT * from iceberg.db."products$history";
SELECT * FROM iceberg.db."products$snapshots"; 
SELECT * FROM iceberg.db."products$files"; 


SELECT * FROM iceberg.db.products FOR TIMESTAMP AS OF TIMESTAMP '2025-04-26 :59:29.803 Europe/Paris';



