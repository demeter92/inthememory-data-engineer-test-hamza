# inthememory-data-engineer-test-hamza
Data Engineer Test for the company InTheMemory

In case of problemen when running spark  linked to the syntax of the files spark-master.sh and spark-worker.sh, 
please make sure tthe end line is LF and not CRLF. It may cause problem.


Voila un exemple de requete trino sur un table iceberg :
SELECT * from iceberg.db.products;
SELECT count(*) from iceberg.db.products;
SELECT * FROM iceberg.db."products$manifests";
SELECT * from iceberg.db."products$properties";
SELECT * from iceberg.db."products$history";
SELECT * FROM iceberg.db."products$snapshots"; 
SELECT * FROM iceberg.db."products$files"; 

SELECT * FROM iceberg.db.products FOR TIMESTAMP AS OF TIMESTAMP '2025-04-26 :59:29.803 Europe/Paris';
