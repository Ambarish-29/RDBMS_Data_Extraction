# RDBMS_Batch_Data_Extraction
1. This Repository will contain the codes the fetch iterative chunks of data from different RDBMS based on the spark cluster configs and store in datalake/ RDBMS
2. *Kinldy Note that all the main logics used in the code are deleted, and only the skeleton file is available. This is done to ensure to protect the owners code privacy*
3. *Most of the Spark ETL Pipelines will fetch data at a whole only, but using this codes we can fetch data in batches*
4. *This will be useful in scenarios where the cluster size is small and the size of data is huge*
5. Failure Recovery Scenarios Also Handled - seperate pipeline ensures the data extraction relaibility
6. Based on the cluster size, the pipleine will progress:
   Example: A small cluster comprises of 2 nodes with 32 GB memory and 4 cores will extract and load a table(size of 64 GB) in 3 batches (example calculation not original one)
7. With use of RDBMS advantages like Primary, Foreign Keys, Indexing, Partitions the extraction is ensured to be done in a very effective manner.
8. Proper Error Handling,Comments, Documentation for the code is prepared.


### Supported RDBMS: Mysql, Maria, Postgres, IBM DB2, Microsoft SQL server

### Language: Python

### Prerequisites:
1. Spark Latest Version
2. RDBMS Latest versions
3. pycryptodome==3.20.0 python package
4. tqdm==4.66.1 python package
5. pandas python package




