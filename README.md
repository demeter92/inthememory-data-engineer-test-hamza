# inthememory-data-engineer-test-hamza
Data Engineer Test for the company InTheMemory



Problems when handling transactions table :
- Comments on some input files starting with #
- The id of client was not unique so duplicates in table transactions avec la jointure (26 transactions concernés)
- Sometimes the date was not good so i make sure to get the date from the file name
- This was a problem on my side about a regex which made not get the date in some files


Added column :
I added the file ingestion_ts for the needs of the audit


