######################################################

#----------------------------------------------------


#           Prerequisistes for the code: pip install pycryptodome==3.20.0
#                                        pip install tqdm==4.66.1
#                                        pip install pandas


#----------------------------------------------------


#                        Product: Data Studio
#                Code written by: Ambarish,Sai Tharun
#                    Reviewed By:
#                       Language: Pyspark
#                        Purpose: To extract data from RDBMS in batches based on our cluster properties
#                Supported RDBMS: Mysql, Maria, Postgres, IBM DB2, Microsoft SQL server
#              Supported Targets: Azure Blob, Mysql, Maria, Postgres, IBM DB2, Microsoft SQL server
#  Code Documentation & Pre Read: https://crayon.atlassian.net/wiki/spaces/DO/pages/3081568293/Onboarding+-+RDBMS+Data+Extraction+in+batches



#######################################################



#Code Starts from here


#function to get the current timestamp
def cur_timestamp_func():
    from datetime import datetime
    current_timestamp = datetime.now()
    formatted_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_timestamp



#tracker table - to get the log_id based on previous id
def tracker_table_prep_func(spark,master_tracker_get_json):
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    from pyspark.sql.functions import max
    global task_log_num
    
    
    if len(master_tracker_get_json):
        schema = StructType([
            StructField("log_id", IntegerType(), True),
            StructField("source_id", IntegerType(), True),
            StructField("source_name", StringType(), True),
            StructField("rdbms_type", StringType(), True),
            StructField("database_name", StringType(), True),
            StructField("schema_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("load_status", StringType(), True),
            StructField("start_timestamp", StringType(), True),
            StructField("end_timestamp", StringType(), True)
        ]) 
        df = spark.createDataFrame(master_tracker_get_json,schema=schema)
        task_log_num = df.select(max('log_id')).collect()[0][0]
    else:
        task_log_num = 0

    return task_log_num


#to iterate the log_id by 1
def task_logid():
    globals()['task_log_num'] = globals()['task_log_num']+1
    return globals()['task_log_num']
    

'''
Commenting the output tracker table for now, since we need to pass this to endpoint

#chunk level log tracker table
def write_task_log(spark,log,log_details,blob_log_details,type):
    
    if type == 'append':
        columns = ['log_id','source_name','rdbms_type', 'database_name', 'table_name','write_detail','write_sub_detail','chunk_size','extraction_approach','iteration','query'
                ,'read_status','write_status','mask_read_status','mask_write_status','start_timestamp','end_timestamp','failure_reason']
        log = spark.createDataFrame([(log)], schema = columns)

        if blob_log_details is not None:
            log.write.format(blob_log_details['log_write_format']).option('header',True).mode('append').save(f"abfss://{blob_log_details['container_name']}@{blob_log_details['account_name']}.dfs.core.windows.net/{blob_log_details['folder_structures']['chunk_log_folder_structure']}")

        else:  
            log.write.jdbc(url=log_details['db_url'], table=log_details['chunk_table_name'], mode="append", properties=log_details['log_connection_details'])


    elif type == 'overwrite':
        
        from pyspark.sql.functions import col,when
        
        if blob_log_details is not None:
            
            chunk_log_df = spark.read.format(blob_log_details['log_write_format']).option('recursiveFileLookup',True).option('header',True).option('inferschema',True).load(f"abfss://{blob_log_details['container_name']}@{blob_log_details['account_name']}.dfs.core.windows.net/{blob_log_details['folder_structures']['chunk_log_folder_structure']}/")
            chunk_log_df = chunk_log_df.withColumn("end_timestamp", when((col('log_id') == log['log_id']) & (col('iteration') == log['iteration']), log['end_timestamp']).otherwise(col('end_timestamp')))\
                                        .withColumn("read_status", when((col('log_id') == log['log_id']) & (col('iteration') == log['iteration']), log['read_status']).otherwise(col('read_status')))\
                                        .withColumn("write_status", when((col('log_id') == log['log_id']) & (col('iteration') == log['iteration']), log['write_status']).otherwise(col('write_status')))\
                                        .withColumn("mask_read_status", when((col('log_id') == log['log_id']) & (col('iteration') == log['iteration']), log['mask_read_status']).otherwise(col('mask_read_status')))\
                                        .withColumn("mask_write_status", when((col('log_id') == log['log_id']) & (col('iteration') == log['iteration']), log['mask_write_status']).otherwise(col('mask_write_status')))
            chunk_log_df.write.option('header',True).format(blob_log_details['log_write_format']).mode('overwrite').save(f"abfss://{blob_log_details['container_name']}@{blob_log_details['account_name']}.dfs.core.windows.net/chunk_level_table_temp")
            temp_df = spark.read.format(blob_log_details['log_write_format']).option('recursiveFileLookup',True).option('header',True).option('inferschema',True).load(f"abfss://{blob_log_details['container_name']}@{blob_log_details['account_name']}.dfs.core.windows.net/chunk_level_table_temp/")
            temp_df.write.option('header',True).format(blob_log_details['log_write_format']).mode('overwrite').save(f"abfss://{blob_log_details['container_name']}@{blob_log_details['account_name']}.dfs.core.windows.net/{blob_log_details['folder_structures']['chunk_log_folder_structure']}")
            
            
        else:
            chunk_log_df = spark.read.jdbc(url=log_details['db_url'],table = log_details["chunk_table_name"]
                        , properties=log_details["log_connection_details"])
            chunk_log_df = chunk_log_df.withColumn("end_timestamp", when((col('log_id') == log['log_id']) & (col('iteration') == log['iteration']), log['end_timestamp']).otherwise(col('end_timestamp')))\
                                        .withColumn("read_status", when((col('log_id') == log['log_id']) & (col('iteration') == log['iteration']), log['read_status']).otherwise(col('read_status')))\
                                        .withColumn("write_status", when((col('log_id') == log['log_id']) & (col('iteration') == log['iteration']), log['write_status']).otherwise(col('write_status')))\
                                        .withColumn("mask_read_status", when((col('log_id') == log['log_id']) & (col('iteration') == log['iteration']), log['mask_read_status']).otherwise(col('mask_read_status')))\
                                        .withColumn("mask_write_status", when((col('log_id') == log['log_id']) & (col('iteration') == log['iteration']), log['mask_write_status']).otherwise(col('mask_write_status')))
            chunk_log_df.write.jdbc(url=log_details['db_url']
                                                 ,table='new_log_test_table', mode = 'overwrite',properties=log_details['log_connection_details'])
            temp_df = spark.read.jdbc(url=log_details['db_url'],table = "new_log_test_table"
                        , properties=log_details["log_connection_details"])
            temp_df.write.jdbc(url=log_details['db_url']
                                                 ,table=log_details["chunk_table_name"], mode = 'overwrite',properties=log_details['log_connection_details'])
            
            
#master level log tracker table
def table_log(spark,log_list,log_details,type,blob_log_details):
    from datetime import datetime
    if type.lower()=='append':
        columns=['log_id','source_name', 'rdbms_type', 'database_name', 'table_name', 'load_status','start_time_stamp', 'end_time_stamp']
        log = spark.createDataFrame([(log_list)], schema = columns)
        
        if blob_log_details is not None:
            log.write.format(blob_log_details['log_write_format']).option('header',True).mode('append').save(f"abfss://{blob_log_details['container_name']}@{blob_log_details['account_name']}.dfs.core.windows.net/{blob_log_details['folder_structures']['master_log_folder_structure']}")

        else:
            log.write.jdbc(url=log_details['db_url'], table=log_details['master_table_name'], mode="append", properties=log_details['log_connection_details'])
            
    
    else:
        from pyspark.sql.functions import when

        if blob_log_details is not None:
            table_log_df = spark.read.format(blob_log_details['log_write_format']).option('recursiveFileLookup',True).option('header',True).option('inferschema',True).load(f"abfss://{blob_log_details['container_name']}@{blob_log_details['account_name']}.dfs.core.windows.net/{blob_log_details['folder_structures']['master_log_folder_structure']}/")
            table_log_df = table_log_df.withColumn("load_status"
                                                               ,when(table_log_df["log_id"]==log_list[0],log_list[5]).otherwise(table_log_df["load_status"])).withColumn('end_time_stamp', when(table_log_df["log_id"]==log_list[0],datetime.now().strftime("%Y-%m-%d %H:%M:%S")).otherwise(table_log_df["end_time_stamp"]))
            table_log_df.write.option('header',True).format(blob_log_details['log_write_format']).mode('overwrite').save(f"abfss://{blob_log_details['container_name']}@{blob_log_details['account_name']}.dfs.core.windows.net/master_level_table_temp")
            temp_df = spark.read.format(blob_log_details['log_write_format']).option('recursiveFileLookup',True).option('header',True).option('inferschema',True).load(f"abfss://{blob_log_details['container_name']}@{blob_log_details['account_name']}.dfs.core.windows.net/master_level_table_temp/")
            temp_df.write.option('header',True).format(blob_log_details['log_write_format']).mode('overwrite').save(f"abfss://{blob_log_details['container_name']}@{blob_log_details['account_name']}.dfs.core.windows.net/{blob_log_details['folder_structures']['master_log_folder_structure']}")

            

        else:
            table_log_df = spark.read.jdbc(url=log_details['db_url'],table = log_details["master_table_name"]
                        , properties=log_details["log_connection_details"])
            table_log_df = table_log_df.withColumn("load_status"
                                                               ,when(table_log_df["log_id"]==log_list[0],log_list[5]).otherwise(table_log_df["load_status"])).withColumn('end_time_stamp', when(table_log_df["log_id"]==log_list[0],datetime.now().strftime("%Y-%m-%d %H:%M:%S")).otherwise(table_log_df["end_time_stamp"]))
            
            table_log_df.write.jdbc(url=log_details['db_url']
                                                 ,table='new_log_test_table', mode = 'overwrite',properties=log_details['log_connection_details'])
            temp_df = spark.read.jdbc(url=log_details['db_url'],table = "new_log_test_table"
                        , properties=log_details["log_connection_details"])
            temp_df.write.jdbc(url=log_details['db_url']
                                                 ,table=log_details["master_table_name"], mode = 'overwrite',properties=log_details['log_connection_details'])

'''


#chunk level log tracker table
def write_task_log(spark,log,type,chunk_json_array=None):
    import json
    if type == 'append':
        log_id,source_id,source_name,rdbms_type,database_name,schema_name,table_name,write_detail,write_sub_detail,chunk_size,extraction_approach,iteration,query,start_timestamp=log
        chunk_json_array=dict()
        chunk_json_array['log_id'] = log_id
        chunk_json_array['client_source_id'] = source_id
        chunk_json_array['source_name'] = source_name
        chunk_json_array['rdbms_type'] = rdbms_type
        chunk_json_array['database_name'] = database_name
        chunk_json_array['schema_name'] = schema_name
        chunk_json_array['table_name'] = table_name
        chunk_json_array['write_detail'] = write_detail
        chunk_json_array['write_sub_detail'] = write_sub_detail
        chunk_json_array['chunk_size'] = chunk_size
        chunk_json_array['extraction_approach'] = extraction_approach
        chunk_json_array['iteration'] = iteration
        chunk_json_array['query'] = query
        chunk_json_array['start_timestamp'] = start_timestamp
    
    else:
        chunk_json_array['end_timestamp'] = log['end_timestamp']
        chunk_json_array['read_status'] = log['read_status']
        chunk_json_array['write_status'] = log['write_status']
        chunk_json_array['mask_read_status'] = log['mask_read_status']
        chunk_json_array['mask_write_status'] = log['mask_write_status']
        try:
            if log['failure_reason']:
                chunk_json_array['failure_reason'] = log['failure_reason']
        except KeyError:
            pass
    return chunk_json_array



#master level log tracker table
def table_log(spark,log_list,type,main_json_array=None):
    from datetime import datetime
    import json
    if type.lower()=='append':
        log_id, source_id, source_name, rdbms_type, database_name, schema_name, table_name, load_status,start_time_stamp = log_list
        main_json_array = dict()
        main_json_array['log_id'] = log_id
        main_json_array['client_source_id'] = source_id
        main_json_array['source_name'] = source_name
        main_json_array['rdbms_type'] = rdbms_type
        main_json_array['database_name'] = database_name
        main_json_array['schema_name'] = schema_name
        main_json_array['table_name'] = table_name
        main_json_array['load_status'] = load_status
        main_json_array['start_timestamp'] = start_time_stamp
   
    else:
        main_json_array['end_timestamp'] = cur_timestamp_func() 
        main_json_array['load_status'] = log_list[6]

    return main_json_array




#To read data from RDBMS via jdbc queries
def read_func(spark,driver,url,query,username,password,read_rdbms_name=None):
    string = f"({query})" if read_rdbms_name == 'oracle' else f"({query}) as temp_table"
    df_name = spark.read.format("jdbc").option('driver',driver)\
                    .option("url", url) \
                    .option("dbtable", string) \
                    .option("user", username) \
                    .option("password", password) \
                    .load()
    return df_name


#To write data to RDBMS via jdbc queries
def write_func(dff_name,mode,driver,url,table,username,password):
    write_rdbms_name = 'mysql' if 'mysql' in url else 'postgres' if 'postgres' in url else 'ibm' if 'db2' in url else 'mssql' if 'sqlserver' in url else None
    table = f'`{table}`' if write_rdbms_name not in ('postgres','ibm','mssql') else f'"{table}"' if write_rdbms_name in ('postgres','ibm') else f'[{table}]'
    dff_name.write.mode(mode).format("jdbc").option('driver',driver)\
                    .option("url", url) \
                    .option("dbtable", table) \
                    .option("user", username) \
                    .option("password", password) \
                    .save()


#To write a dataframe to azure blob
def write_func_blob(dff_name,mode,account_name,container_name,folder_structure,write_format):
    dff_name.write.mode(mode).option("header", "true")\
    .format(write_format)\
    .option("delta.columnMapping.mode", "name")\
    .save(f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{folder_structure}") 


#To write incremental data to azure blob with watermark approach
def write_func_blob_incremental(dff_name,account_name,container_name,folder_structure,column_name):
    from delta.tables import DeltaTable
    deltaTablePath = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{folder_structure}"
    delta_table = DeltaTable.forPath(spark, deltaTablePath)
    primary_key_column_list = column_name if isinstance(column_name, list) else [column_name]
    merge_condition = " AND ".join([f"target.`{col}` = source.`{col}`" for col in primary_key_column_list])
    source_columns = [col for col in dff_name.columns if f"`{col}`" != 'Hash_Column']
    update_expr = {f"`{col}`": f"source.`{col}`" for col in source_columns}
    delta_table.alias("target") \
        .merge(
            dff_name.alias("source"),
            merge_condition
        ) \
        .whenMatchedUpdate(set=update_expr) \
        .whenNotMatchedInsert(values=update_expr) \
        .execute()
  

#To write incremental data to azure blob with hashing approach
def write_func_blob_incremental_hashing(dff_name,account_name,container_name,folder_structure,column_name,hash_column):
    from delta.tables import DeltaTable
    deltaTablePath = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{folder_structure}"
    delta_table = DeltaTable.forPath(spark, deltaTablePath)
    primary_key_column_list = column_name if isinstance(column_name, list) else [column_name]
    merge_condition = " AND ".join([f"target.`{col}` = source.`{col}`" for col in primary_key_column_list])
    columns_to_update = [col for col in dff_name.columns if f"`{col}`" not in primary_key_column_list]
    update_dict = {f"`{col}`": f"source.`{col}`" for col in columns_to_update}
    delta_table.alias("target").merge(
        dff_name.alias("source"),
        merge_condition
        ).whenMatchedUpdate(
            condition=f"target.{hash_column} != source.{hash_column}",
            set=update_dict
        ).whenNotMatchedInsert(
            values=update_dict
        ).execute()
                    

#Clustered approach query
def clustered_query_func(read_rdbms_name,read_table_name,column_name,maxval,cluster_threshold,read_schema_name):
    clustered_query = {'ibm': f'select * from "{read_table_name}" where "{column_name}" > {maxval} order by "{column_name}" limit {cluster_threshold}',
                       'postgres': f'select * from "{read_table_name}" where "{column_name}" > {maxval} order by "{column_name}" limit {cluster_threshold}',
                        'mysql': f'select * from `{read_table_name}` where `{column_name}` > {maxval} order by `{column_name}` limit {cluster_threshold}',
                      'maria': f'select * from `{read_table_name}` where `{column_name}` > {maxval} order by `{column_name}` limit {cluster_threshold}',
                      'mssql':f'SELECT * FROM [{read_schema_name}].[{read_table_name}] WHERE [{column_name}] > {maxval} ORDER BY [{column_name}] OFFSET 0 ROWS FETCH NEXT {cluster_threshold} ROWS ONLY'}

    return clustered_query[read_rdbms_name]
    


#To identify whether the table has primary key or not
def primary_key_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name=None):
    prim = {'mysql': f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = '{read_db_name}' AND TABLE_NAME = '{read_table_name}' ORDER BY ORDINAL_POSITION",
            'maria': f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = '{read_db_name}' AND TABLE_NAME = '{read_table_name}' ORDER BY ORDINAL_POSITION",
            'postgres': f"SELECT a.attname AS column_name FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) JOIN pg_class t ON t.oid = c.conrelid WHERE t.relname = '{read_table_name}' AND c.contype = 'p' ORDER BY array_position(c.conkey, a.attnum)",
            'ibm': f"select colname from syscat.KEYCOLUSE where tabschema = '{read_schema_name}' and tabname = '{read_table_name}'",
            'mssql': f"SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE  WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1  AND TABLE_SCHEMA = '{read_schema_name}' AND TABLE_NAME = '{read_table_name}'"
}

    return prim[read_rdbms_name]



#Limit Approach Query
def limit_func(read_rdbms_name,read_table_name,cluster_threshold,offset_number,table_columns,read_schema_name):
    if read_rdbms_name not in ('postgres','ibm','mssql'): 
        quoted_columns = [f'`{col}`' for col in table_columns]
        order_by_clause = ', '.join(quoted_columns)
    elif read_rdbms_name in ('postgres','ibm'):
        quoted_columns = [f'"{col}"' for col in table_columns]
        order_by_clause = ', '.join(quoted_columns)
    else:
        quoted_columns = [f'[{col}]' for col in table_columns]
        order_by_clause = ', '.join(quoted_columns)
        
    lmt = {'mysql': f"select * from `{read_table_name}` order by {order_by_clause} limit {cluster_threshold} offset {offset_number}",
          'maria': f"select * from `{read_table_name}` order by {order_by_clause} limit {cluster_threshold} offset {offset_number}",
          'postgres': f'select * from "{read_table_name}" order by {order_by_clause} limit {cluster_threshold} offset {offset_number}',
          'ibm': f'select * from "{read_schema_name}"."{read_table_name}" order by {order_by_clause} limit {cluster_threshold} offset {offset_number}',
          'mssql': f"SELECT * FROM [{read_schema_name}].[{read_table_name}] ORDER BY {order_by_clause} OFFSET {offset_number} ROWS FETCH NEXT {cluster_threshold} ROWS ONLY"}
    
    return lmt[read_rdbms_name]


#To find the total table size
def table_size_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name=None):
    avg_dct = {'mysql': f"SELECT (data_length + index_length) /  (1024 * 1024)  AS table_size_mb FROM information_schema.tables WHERE table_type = 'BASE TABLE' and table_schema = '{read_db_name}' AND table_name = '{read_table_name}'",
               'maria': f"SELECT (data_length + index_length) / (1024 * 1024)  AS table_size_mb FROM information_schema.tables WHERE table_type = 'BASE TABLE' and table_schema = '{read_db_name}' AND table_name = '{read_table_name}'",
               'postgres': f"SELECT pg_relation_size('public.\"{read_table_name}\"') / (1024 * 1024) AS table_size_mb",
               'ibm': f"SELECT (DATA_OBJECT_P_SIZE + INDEX_OBJECT_P_SIZE + LONG_OBJECT_P_SIZE + LOB_OBJECT_P_SIZE + XML_OBJECT_P_SIZE)/1024 AS TOTAL_SIZE_IN_MB FROM SYSIBMADM.ADMINTABINFO WHERE TABSCHEMA NOT LIKE 'SYS%' and TABSCHEMA = '{read_schema_name}' and TABNAME = '{read_table_name}'",
              'mssql': f"SELECT sum((ps.used_page_count * 8)/1024) AS table_size_mb FROM sys.tables t JOIN sys.indexes i ON t.object_id = i.object_id JOIN sys.dm_db_partition_stats ps ON t.object_id = ps.object_id AND i.index_id = ps.index_id WHERE t.schema_id = SCHEMA_ID('{read_schema_name}') AND t.name='{read_table_name}'"}

    return avg_dct[read_rdbms_name]
               

#To convert a dataframe to dictionary
def con_to_dictionary(df, key, key2=None, key3=None,key4=None,key5=None):
    data_itr = df.rdd.toLocalIterator()
    dictionary_1 = {}
    masked_dict = {}
    for row in data_itr:
        i = 0
        dictionary_val = {}
        for column in df.columns:
            dictionary_val[column] = row[i]
            #print(dictionary_val[column])
            i = i+1
        try:
            dict_key = f'{dictionary_val[key2]}_{dictionary_val[key3]}_{dictionary_val[key5]}_{dictionary_val[key]}_{dictionary_val[key4]}'
        except KeyError:
            try:
                dict_key = f'{dictionary_val[key2]}_{dictionary_val[key3]}_{dictionary_val[key4]}_{dictionary_val[key]}'
            except KeyError:
                    dict_key = dictionary_val[key]
        
        dictionary_1[dict_key] = dictionary_val
    return dictionary_1


#To mask the data if configure sensitivity is selected
def masking_encrypt_data(data):
    print("Masking in progress..")
    from Crypto.Cipher import AES
    from Crypto.Random import get_random_bytes
    import base64
    import struct
    from Crypto.Util.Padding import pad

    key = b'\x01\x23\x45\x67\x89\xab\xcd\xef\xfe\xdc\xba\x98\x76\x54\x32\x10'
    
    # Convert float to bytes
    if isinstance(data, float):
        data_bytes = struct.pack('!f', data)
    else:
        data_new = str(data)
        data_bytes = data_new.encode('utf-8')
   
    # Pad the input data for the algorithm
    data_padded = pad(data_bytes, AES.block_size)
 
    # Encrypt data using Advanced Encryption Standard(AES)
    cipher = AES.new(key, AES.MODE_ECB)
    ciphertext = cipher.encrypt(data_padded)
   
    # Encode to base64 for better representation
    base64_ciphertext = base64.b64encode(ciphertext).decode('utf-8')
 
    return base64_ciphertext


#To get the distinct values for non cluster approach
def distinct_index_query_func(read_rdbms_name,read_db_name,read_table_name,index_column,read_schema_name):
    distinct_dct = {'mysql': f"select distinct `{index_column}` from `{read_db_name}`.`{read_table_name}`",
                    'maria': f"select distinct `{index_column}` from `{read_db_name}`.`{read_table_name}`",
                    'postgres': f'select distinct("{index_column}") from "{read_table_name}"',
                    'ibm': f'select distinct("{index_column}") from "{read_schema_name}"."{read_table_name}"',
                    'mssql': f"select distinct [{index_column}] from [{read_schema_name}].[{read_table_name}]"}
    
    return distinct_dct[read_rdbms_name]


#Non cluster approach query
def non_clustered_query_func(read_rdbms_name,read_table_name,index_column_list,index_column_count,dist,cluster_threshold,offset_value,table_columns,read_schema_name,column_data_type):

    order_by_clause = order_by_clause_func(read_rdbms_name,table_columns)
    order_by_clause = ',' + order_by_clause if len(order_by_clause) !=0 else order_by_clause
    index_column = composite_non_clustered_query_func(read_rdbms_name,index_column_count,index_column_list)
        
    non_cluster_dct = {
        'int':{ 'mysql': f"select * from `{read_table_name}` where `{index_column_list[0]}` = {dist} order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}",
                'maria': f"select * from `{read_table_name}` where `{index_column_list[0]}` = {dist} order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}",
                'postgres': f'select * from "{read_table_name}" where "{index_column_list[0]}" = {dist} order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}',
                'ibm': f'select * from "{read_schema_name}"."{read_table_name}" where "{index_column_list[0]}" = {dist} order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}',
                'mssql': f"select * from [{read_schema_name}].[{read_table_name}] where [{index_column_list[0]}] = {dist} order by {index_column}{order_by_clause} offset {offset_value} rows fetch next {cluster_threshold} rows only"},

                       
        'string':{'mysql': f"select * from `{read_table_name}` where `{index_column_list[0]}` = '{dist}' order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}",
                  'maria': f"select * from `{read_table_name}` where `{index_column_list[0]}` = '{dist}' order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}",
                  'postgres': f'select * from "{read_table_name}" where "{index_column_list[0]}" = \'{dist}\' order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}',
                  'ibm': f'select * from "{read_schema_name}"."{read_table_name}" where "{index_column_list[0]}" = \'{dist}\' order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}',
                  'mssql': f"select * from [{read_schema_name}].[{read_table_name}] where [{index_column_list[0]}] = \'{dist}\' order by {index_column}{order_by_clause} offset {offset_value} rows fetch next {cluster_threshold} rows only"}
                      }

    return non_cluster_dct[column_data_type][read_rdbms_name]



#Non cluster Approach query for null values
def non_clustered_null_query_func(read_rdbms_name,read_table_name,index_column_list,index_column_count,dist,cluster_threshold,offset_value,table_columns,read_schema_name):
    order_by_clause = order_by_clause_func(read_rdbms_name,table_columns)
    order_by_clause = ',' + order_by_clause if len(order_by_clause) !=0 else order_by_clause
    index_column = composite_non_clustered_query_func(read_rdbms_name,index_column_count,index_column_list)
        
    non_cluster_dct = {
        'mysql': f'select * from `{read_table_name}` where `{index_column_list[0]}` is NUll order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}',
                       'maria': f'select * from `{read_table_name}` where `{index_column_list[0]}` is NUll order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}',
                       'postgres': f'select * from "{read_table_name}" where "{index_column_list[0]}" is NUll order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}',
                      'ibm': f'select * from "{read_schema_name}"."{read_table_name}" where "{index_column_list[0]}" is NUll order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_value}',
                      'mssql': f'select * from [{read_schema_name}].[{read_table_name}] where [{index_column_list[0]}] is NUll order by {index_column}{order_by_clause} offset {offset_value} rows fetch next {cluster_threshold} rows only'
                      }

    return non_cluster_dct[read_rdbms_name]



#To find non cluster index on a table(if exists or not)
def index_query_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name=None):
    idx_dct = {'mysql': f"SELECT column_name FROM information_schema.STATISTICS WHERE table_schema = '{read_db_name}' AND table_name = '{read_table_name}' and lower(index_name) != 'primary' order by seq_in_index",
               'maria': f"SELECT column_name FROM information_schema.STATISTICS WHERE table_schema = '{read_db_name}' AND table_name = '{read_table_name}' and lower(index_name) != 'primary' order by seq_in_index",
               'postgres': f"SELECT a.attname AS column_name FROM pg_class c JOIN pg_index i ON c.oid = i.indrelid JOIN pg_attribute a ON a.attrelid = c.oid WHERE c.relname = '{read_table_name}' AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public') AND i.indisprimary = 'f' AND i.indisunique = 'f' AND a.attnum = ANY(i.indkey) ORDER BY array_position(i.indkey, a.attnum)",
              'ibm': f"SELECT CASE WHEN SUBSTR(COLNAMES, 1, 1) = '+' THEN SUBSTR(COLNAMES, 2) ELSE COLNAMES END AS INDEXED_COLUMN FROM SYSCAT.INDEXES WHERE TABSCHEMA = '{read_schema_name}' AND TABNAME = '{read_table_name}' AND UNIQUERULE != 'P'",
              'mssql': f"SELECT c.name FROM sys.indexes i JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id WHERE i.object_id = OBJECT_ID('{read_schema_name}.{read_table_name}') AND i.name IS NOT NULL AND i.is_primary_key != 1"}

    return idx_dct[read_rdbms_name]


#to dynamically create order by clause
def order_by_clause_func(read_rdbms_name,table_columns):
    if read_rdbms_name not in ('postgres','ibm','mssql'): 
        quoted_columns = [f'`{col}`' for col in table_columns]
        order_by_clause = ', '.join(quoted_columns)
    elif read_rdbms_name in ('postgres','ibm'):
        quoted_columns = [f'"{col}"' for col in table_columns]
        order_by_clause = ', '.join(quoted_columns)
    else:
        quoted_columns = [f'[{col}]' for col in table_columns]
        order_by_clause = ', '.join(quoted_columns)

    order_by_clause = order_by_clause if len(table_columns) != 0 else table_columns[0]

    return order_by_clause
    

#to find datatype of a column
def column_datatype_func(column_name,column_list):
    for key,value in column_list:
        if key == column_name:
            val = value

    if val.lower() in ('int','bigint','smallint','float','double') or 'decimal' in val.lower():
        column_data_type = 'int'
    elif val.lower() in ('string','timestamp','date'):
        column_data_type = 'string'
    elif val.lower() in ('binary'):
        column_data_type = 'binary'
    elif 'array' in val.lower():
        column_data_type = 'array'
    elif 'boolean' in val.lower():
        column_data_type = 'boolean'

    return column_data_type


#clustered query appraoch
def composite_clustered_query_func(read_rdbms_name,read_table_name,main_key,symbol,maxval,cluster_threshold,table_type,read_schema_name,order_by_clause=None,watermark_column=None,last_inserted_date=None,source_max_date=None):
    
    column_data_type = column_datatype_func(main_key,table_type)
    
    clustered_query = {
        'int': {
        'mysql': f'select * from `{read_table_name}` where `{main_key}` {symbol} {maxval} order by `{main_key}` limit {cluster_threshold}' if watermark_column is False else f'select * from `{read_table_name}` where `{main_key}` {symbol} {maxval} and `{watermark_column}` > "{last_inserted_date}" and `{watermark_column}` <= "{source_max_date}" order by `{main_key}` limit {cluster_threshold}',
        'maria': f'select * from `{read_table_name}` where `{main_key}` {symbol} {maxval} order by `{main_key}` limit {cluster_threshold}' if watermark_column is False else f'select * from `{read_table_name}` where `{main_key}` {symbol} {maxval} and `{watermark_column}` > "{last_inserted_date}" and `{watermark_column}` <= "{source_max_date}" order by `{main_key}` limit {cluster_threshold}',
        'postgres': f'select * from "{read_table_name}" where "{main_key}" {symbol} {maxval} order by "{main_key}" limit {cluster_threshold}' if watermark_column is False else f'select * from "{read_table_name}" where "{main_key}"  {symbol} {maxval} and "{watermark_column}" > "{last_inserted_date}" and "{watermark_column}" <= "{source_max_date}" order by "{main_key}" limit {cluster_threshold}',
        'ibm': f'select * from "{read_schema_name}"."{read_table_name}" where "{main_key}" {symbol} {maxval} order by "{main_key}" limit {cluster_threshold}' if watermark_column is False else f'select * from "{read_schema_name}"."{read_table_name}" where "{main_key}" {symbol} {maxval} and "{watermark_column}" > "{last_inserted_date}" and "{watermark_column}" <= "{source_max_date}" order by "{main_key}" limit {cluster_threshold}',
        'mssql': f'SELECT * FROM [{read_schema_name}].[{read_table_name}] WHERE [{main_key}] {symbol} {maxval} ORDER BY [{main_key}] OFFSET 0 ROWS FETCH NEXT {cluster_threshold} ROWS ONLY' if watermark_column is False else f'SELECT * FROM [{read_schema_name}].[{read_table_name}] WHERE [{main_key}] {symbol} {maxval} and [{watermark_column}] > "{last_inserted_date}" and [{watermark_column}] <= "{source_max_date}" ORDER BY [{main_key}] OFFSET 0 ROWS FETCH NEXT {cluster_threshold} ROWS ONLY'
    },
        'string': {
        'mysql': f'select * from `{read_table_name}` where `{main_key}` {symbol} \'{maxval}\' order by `{main_key}` limit {cluster_threshold}' if watermark_column is False else f'select * from `{read_table_name}` where `{main_key}` {symbol} \'{maxval}\' and `{watermark_column}` > "{last_inserted_date}" and `{watermark_column}` <= "{source_max_date}" order by `{main_key}` limit {cluster_threshold}',
        'maria': f'select * from `{read_table_name}` where `{main_key}` {symbol} \'{maxval}\' order by `{main_key}` limit {cluster_threshold}' if watermark_column is False else f'select * from `{read_table_name}` where `{main_key}` {symbol} \'{maxval}\' and `{watermark_column}` > "{last_inserted_date}" and `{watermark_column}` <= "{source_max_date}" order by `{main_key}` limit {cluster_threshold}',
        'postgres': f'select * from "{read_table_name}" where "{main_key}" {symbol} \'{maxval}\' order by "{main_key}" limit {cluster_threshold}' if watermark_column is False else f'select * from "{read_table_name}" where "{main_key}" {symbol} \'{maxval}\' and "{watermark_column}" > "{last_inserted_date}" and "{watermark_column}" <= "{source_max_date}" order by "{main_key}" limit {cluster_threshold}',
        'ibm': f'select * from "{read_schema_name}"."{read_table_name}" where "{main_key}" {symbol} \'{maxval}\' order by "{main_key}" limit {cluster_threshold}' if watermark_column is False else f'select * from "{read_schema_name}"."{read_table_name}" where "{main_key}" {symbol} \'{maxval}\' and "{watermark_column}" > "{last_inserted_date}" and "{watermark_column}" <= "{source_max_date}" order by "{main_key}" limit {cluster_threshold}',
        'mssql': f'SELECT * FROM [{read_schema_name}].[{read_table_name}] WHERE [{main_key}] {symbol} \'{maxval}\' ORDER BY [{main_key}] OFFSET 0 ROWS FETCH NEXT {cluster_threshold} ROWS ONLY' if watermark_column is False else f'SELECT * FROM [{read_schema_name}].[{read_table_name}] WHERE [{main_key}] {symbol} \'{maxval}\' and [{watermark_column}] > "{last_inserted_date}" and [{watermark_column}] <= "{source_max_date}" ORDER BY [{main_key}] OFFSET 0 ROWS FETCH NEXT {cluster_threshold} ROWS ONLY'
    }
}

    comp_clustered_query = {
        'int': {
        'mysql': f'select * from `{read_table_name}` where `{main_key}` {symbol} {maxval} order by {order_by_clause} limit {cluster_threshold}',
        'maria': f'select * from `{read_table_name}` where `{main_key}` {symbol} {maxval} order by {order_by_clause} limit {cluster_threshold}',
        'postgres': f'select * from "{read_table_name}" where "{main_key}" {symbol} {maxval} order by {order_by_clause} limit {cluster_threshold}',
        'ibm': f'select * from "{read_schema_name}"."{read_table_name}" where "{main_key}" {symbol} {maxval} order by {order_by_clause} limit {cluster_threshold}',
        'mssql': f'SELECT * FROM [{read_schema_name}].[{read_table_name}] WHERE [{main_key}] {symbol} {maxval} ORDER BY {order_by_clause} OFFSET 0 ROWS FETCH NEXT {cluster_threshold} ROWS ONLY'
    },
        'string': {
        'mysql': f'select * from `{read_table_name}` where `{main_key}` {symbol} \'{maxval}\' order by {order_by_clause} limit {cluster_threshold}',
        'maria': f'select * from `{read_table_name}` where `{main_key}` {symbol} \'{maxval}\' order by {order_by_clause} limit {cluster_threshold}',
        'postgres': f'select * from "{read_table_name}" where "{main_key}" {symbol} \'{maxval}\' order by {order_by_clause} limit {cluster_threshold}',
        'ibm': f'select * from "{read_schema_name}"."{read_table_name}" where "{main_key}" {symbol} \'{maxval}\' order by {order_by_clause} limit {cluster_threshold}', 
        'mssql': f'SELECT * FROM [{read_schema_name}].[{read_table_name}] WHERE [{main_key}] {symbol} \'{maxval}\' ORDER BY {order_by_clause} OFFSET 0 ROWS FETCH NEXT {cluster_threshold} ROWS ONLY'
    }
}

    if order_by_clause is not None:
        return comp_clustered_query[column_data_type][read_rdbms_name]
    else:
        return clustered_query[column_data_type][read_rdbms_name]


def composite_clustered_query_incremental_func(read_rdbms_name,read_table_name,offset_value,cluster_threshold,read_schema_name,order_by_clause=None,watermark_column=None,last_inserted_date=None,source_max_date=None):
    comp_clustered_incre_query={
            'mysql':f'select * from `{read_table_name}` where `{watermark_column}` > "{last_inserted_date}" AND `{watermark_column}` <= "{source_max_date}" order by {order_by_clause} limit {cluster_threshold} offset {offset_value}',
            'maria':f'select * from `{read_table_name}` where `{watermark_column}` > "{last_inserted_date}" AND `{watermark_column}` <= "{source_max_date}" order by {order_by_clause} limit {cluster_threshold} offset {offset_value}',
            'postgres':f'select * "{read_table_name}" where "{watermark_column}" > "{last_inserted_date}" AND "{watermark_column}" <= "{source_max_date}" order by {order_by_clause} limit {cluster_threshold} offset {offset_value}',
            'ibm':f'select * from "{read_schema_name}"."{read_table_name}" where "{watermark_column}" > "{last_inserted_date}" AND "{watermark_column}" <= "{source_max_date}" order by {order_by_clause} limit {cluster_threshold} offset {offset_value}',
            'mssql':f'SELECT * FROM [{read_schema_name}].[{read_table_name}] where [{watermark_column}] > "{last_inserted_date}" AND [{watermark_column}] <= "{source_max_date}" order by {order_by_clause} offset {offset_value} rows fetch next {cluster_threshold} rows only'
    }

    return comp_clustered_incre_query[read_rdbms_name]
        
    

#To prepare dynamic sql queries for validations
def dynamic_group_by_validation(read_rdbms_name,read_db_name,read_schema_name,read_table_name,spark,full_column_list,driver,url,username,password,agg):
    
    if agg == 'min':
        min_dynamic_query = ''
        
        for item in range(len(full_column_list)):
                
            if read_rdbms_name not in ('postgres','ibm','mssql'):
                min_dynamic_query += f'SELECT "{full_column_list[item]}" as column_name, CAST(IFNULL(MIN(`{full_column_list[item]}`) ,0) AS DECIMAL(50,5)) AS min_value from `{read_table_name}`'
            elif read_rdbms_name in ('ibm'):
                min_dynamic_query += f'SELECT \'{full_column_list[item]}\' as column_name, COALESCE(FLOAT(MIN("{full_column_list[item]}")),0) AS min_value from "{read_schema_name}"."{read_table_name}"'
            elif read_rdbms_name in ('postgres'):
                min_dynamic_query += f'SELECT \'{full_column_list[item]}\' as column_name, COALESCE(MIN("{full_column_list[item]}")::NUMERIC, 0) AS min_value FROM "{read_table_name}"'
            else:
                min_dynamic_query += f'SELECT \'{full_column_list[item]}\' as column_name, CAST(COALESCE(MIN([{full_column_list[item]}]),0) AS DECIMAL(30,5)) AS min_value from [{read_schema_name}].[{read_table_name}]'
    
                
            if item != (len(full_column_list) - 1):
                min_dynamic_query += ' UNION '

      
        min_source_validation_df = read_func(spark,driver,url,min_dynamic_query,username,password)
        return min_source_validation_df

    
    
    elif agg == 'max':
        max_dynamic_query = ''
        for item in range(len(full_column_list)):
                
            if read_rdbms_name not in ('postgres','ibm','mssql'):
                max_dynamic_query += f'SELECT "{full_column_list[item]}" as column_name, CAST(IFNULL(MAX(`{full_column_list[item]}`),0) AS DECIMAL(50,5)) AS max_value from `{read_table_name}`'
            elif read_rdbms_name in ('ibm'):
                max_dynamic_query += f'SELECT \'{full_column_list[item]}\' as column_name, COALESCE(FLOAT(MAX("{full_column_list[item]}")),0) AS max_value from "{read_schema_name}"."{read_table_name}"'
            elif read_rdbms_name in ('postgres'):
                max_dynamic_query += f'SELECT \'{full_column_list[item]}\' as column_name, COALESCE(MAX("{full_column_list[item]}")::NUMERIC, 0) AS max_value from "{read_table_name}"'
            else:
                max_dynamic_query += f'SELECT \'{full_column_list[item]}\' as column_name, CAST(COALESCE(MAX([{full_column_list[item]}]),0) AS DECIMAL(30,5)) AS max_value from [{read_schema_name}].[{read_table_name}]'
    
                
            if item != (len(full_column_list) - 1):
                max_dynamic_query += ' UNION '
                
        max_source_validation_df = read_func(spark,driver,url,max_dynamic_query,username,password)
        return max_source_validation_df


def dynamic_group_by_validation_temp(read_rdbms_name,read_db_name,read_schema_name,read_table_name,spark,full_column_list,driver,url,username,password,agg):
    
    if agg == 'min':
        min_dynamic_query = 'SELECT '
        for item in range(len(full_column_list)):
            if read_rdbms_name not in ('postgres','ibm','mssql'):
                min_dynamic_query += f'CAST(IFNULL(MIN(`{full_column_list[item]}`) ,0) AS DECIMAL(50,5)) AS "{full_column_list[item]}"'
            elif read_rdbms_name in ('ibm'):
                min_dynamic_query += f'COALESCE(FLOAT(MIN("{full_column_list[item]}")),0) AS "{full_column_list[item]}"'
            elif read_rdbms_name in ('postgres'):
                min_dynamic_query += f'COALESCE(MIN("{full_column_list[item]}")::NUMERIC, 0) AS "{full_column_list[item]}"'
            else:
                min_dynamic_query += f'CAST(COALESCE(MIN([{full_column_list[item]}]),0) AS DECIMAL(30,5)) AS "{full_column_list[item]}"'
    
                
            if item != (len(full_column_list) - 1):
                min_dynamic_query += ", "
            
        
        if read_rdbms_name == 'ibm':
            min_dynamic_query += f' FROM "{read_schema_name}"."{read_table_name}"'
        elif read_rdbms_name in ('postgres', 'mssql'):
            min_dynamic_query += f' FROM "{read_table_name}"' if read_rdbms_name == 'postgres' else f' FROM [{read_schema_name}].[{read_table_name}]'
        else:
            min_dynamic_query += f' FROM `{read_table_name}`'

        min_source_validation_df = read_func(spark,driver,url,min_dynamic_query,username,password)
        return min_source_validation_df
   
    elif agg == 'max':
        max_dynamic_query = 'SELECT '
        for item in range(len(full_column_list)):
            if read_rdbms_name not in ('postgres','ibm','mssql'):
                max_dynamic_query += f'CAST(IFNULL(MAX(`{full_column_list[item]}`) ,0) AS DECIMAL(50,5)) AS "{full_column_list[item]}"'
            elif read_rdbms_name in ('ibm'):
                max_dynamic_query += f'COALESCE(FLOAT(MAX("{full_column_list[item]}")),0) AS "{full_column_list[item]}"'
            elif read_rdbms_name in ('postgres'):
                max_dynamic_query += f'COALESCE(MAX("{full_column_list[item]}")::NUMERIC, 0) AS "{full_column_list[item]}"'
            else:
                max_dynamic_query += f'CAST(COALESCE(MAX([{full_column_list[item]}]),0) AS DECIMAL(30,5)) AS "{full_column_list[item]}"'
    
                
            if item != (len(full_column_list) - 1):
                max_dynamic_query += ", "
            
        
        if read_rdbms_name == 'ibm':
            max_dynamic_query += f' FROM "{read_schema_name}"."{read_table_name}"'
        elif read_rdbms_name in ('postgres', 'mssql'):
            max_dynamic_query += f' FROM "{read_table_name}"' if read_rdbms_name == 'postgres' else f' FROM [{read_schema_name}].[{read_table_name}]'
        else:
            max_dynamic_query += f' FROM `{read_table_name}`'

        max_source_validation_df = read_func(spark,driver,url,max_dynamic_query,username,password)
        return max_source_validation_df     
    

#Validation first part
def validation_preparation_first(spark,table_columns,df,tmp,max_df=None,min_df=None):
    from pyspark.sql.functions import col,max,min,round
    
    max_column_value = []
    min_column_value = []

    tmp_write_validation_count = df.count()
            

    if len(table_columns) < 10:
        for tmp_table_column_count in range(len(table_columns)):
            max_column_value.append(df.agg(round(max(col(table_columns[tmp_table_column_count])),1)).collect()[0][0])
            min_column_value.append(df.agg(round(min(col(table_columns[tmp_table_column_count])),1)).collect()[0][0])

    else:
        sliced_column_list = table_columns[0:9]
        for tmp_table_column_count in range(len(sliced_column_list)):
            max_column_value.append(df.agg(round(max(col(sliced_column_list[tmp_table_column_count])),1)).collect()[0][0])
            min_column_value.append(df.agg(round(min(col(sliced_column_list[tmp_table_column_count])),1)).collect()[0][0])

    full_column_list = table_columns.copy() if len(table_columns) < 10 else sliced_column_list.copy()

    max_column_value = [0 if value is None else value for value in max_column_value]
    min_column_value = [0 if value is None else value for value in min_column_value]

    if tmp == 1:
        write_table_column_count = len(df.columns)
        max_df = spark.createDataFrame([max_column_value], full_column_list)
        min_df = spark.createDataFrame([min_column_value], full_column_list)
        return tmp_write_validation_count,max_df,min_df,full_column_list,write_table_column_count

    else:
        tmp_max_df = spark.createDataFrame([max_column_value], full_column_list)
        tmp_min_df = spark.createDataFrame([min_column_value], full_column_list)
        max_df = max_df.union(tmp_max_df)
        min_df = min_df.union(tmp_min_df)
        return tmp_write_validation_count,max_df,min_df,full_column_list


#validation second part
def validation_preparation_second(spark,min_df,max_df):
    from pyspark.storagelevel import StorageLevel
    from pyspark.sql.functions import max,min
    from pyspark.sql.types import StructType, StructField, StringType
    from datetime import datetime
    from tqdm import tqdm

    #exception handling
    try:
        min_df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        max_df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        min_values = []
        max_values = []
        progress_bar = tqdm(total=len(min_df.columns), desc="Processing", unit="iteration", bar_format="{l_bar}{bar} [ {elapsed} / {remaining} ]")
        for column in min_df.columns:
            
            #Find the minimum value for each column
            min_value = str(min_df.agg(min(column)).collect()[0][0])


            #Error Handling
            if 'E-' in min_value:
                min_value = min_value.split('E')[0]
            
            if 'e-0' in min_value:
                min_value = min_value.replace('e', 'E').replace('+0', '').replace('-0', '-')

            
            
            #Check if the string can be converted to a float
            try:
                if '.' not in min_value:
                    min_value = min_value + '.0'
                else:
                    pass
                
            except ValueError as ve:
                pass


            #Error handling
            try:
                integer_part, decimal_part = min_value.split('.')
                new_part = decimal_part[:1]
                check = int(decimal_part[1:])
                if check == 0:
                    min_value = "{}.{}".format(integer_part, new_part)
                      
            except ValueError:
                pass
    
            except AttributeError:
                pass
    

            #appending min value in list and getting max value
            min_values.append((column, min_value))
            max_value = str(max_df.agg(max(column)).collect()[0][0])
    

            #Error Handling
            if 'E-' in max_value:
                max_value = max_value.split('E')[0]
    
            if 'e-0' in max_value:
                max_value = max_value.replace('e', 'E').replace('+0', '').replace('-0', '-')
                
            try:
                if '.' not in max_value:
                    max_value = max_value + '.0'
                else:
                    pass
                
            except ValueError as ve:
                pass


            try:
                integer_part, decimal_part = max_value.split('.')
                new_part = decimal_part[:1]
                check = int(decimal_part[1:])
                if check == 0:
                    max_value = "{}.{}".format(integer_part, new_part)
                    
                      
            except ValueError:
                pass
    
            except AttributeError:
                pass
    

            #appending max value in list
            max_values.append((column, max_value))

            progress_bar.update(1)
            

        #unpersisting data
        progress_bar.close()
        min_df.unpersist()
        max_df.unpersist()


        #creating df based on the min max values
        min_result_df = spark.createDataFrame(min_values, ["column_name","min_values"])
        max_result_df = spark.createDataFrame(max_values, ["column_name","max_values"])
        

    #exception handling
    except ValueError as ve:
        max_result_df,min_result_df = None,None

    return max_result_df,min_result_df
    

    
#Final validation between source and targets
def validations(read_rdbms_name,read_db_name,read_table_name,table_count,write_validation_count,max_result_df,min_result_df,table_column_count,write_table_column_count,spark,full_column_list,read_driver,read_url,read_username,read_password,masked_values,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,read_schema_name,source_id,validation_url):
    from py4j.protocol import Py4JJavaError
    from pyspark.sql.functions import col,round,lit
    from functools import reduce
    mask_val = 0
    data_val = 0
    def results_validation_pass():
        validation_key = 'RDBMS-' + read_rdbms_name + '_______' + 'DATABASE-' + read_db_name + '_______' + 'TABLE-' + read_table_name
        validation_value = 'Success'

        return validation_key,validation_value

    def results_validation_fail():
        validation_key = 'RDBMS-' + read_rdbms_name + '_______' + 'DATABASE-' + read_db_name + '_______' + 'TABLE-' + read_table_name
        validation_value = 'Failed'

        return validation_key,validation_value

    def results_validation_partial_pass():
        validation_key = 'RDBMS-' + read_rdbms_name + '_______' + 'DATABASE-' + read_db_name + '_______' + 'TABLE-' + read_table_name
        validation_value = 'Partial Success'

        return validation_key,validation_value
        
        

    if len(masked_values) > 0:
        try:
            print('\n--------Masked Data Validation Starts--------')
            masked_min_source_validation_df = dynamic_group_by_validation(read_rdbms_name,read_db_name,read_schema_name,read_table_name,spark,masked_full_column_list,read_driver,read_url,read_username,read_password,'min')
            masked_max_source_validation_df = dynamic_group_by_validation(read_rdbms_name,read_db_name,read_schema_name,read_table_name,spark,masked_full_column_list,read_driver,read_url,read_username,read_password,'max')
            masked_min_source_validation_df = masked_min_source_validation_df.withColumn("min_value", round(col("min_value"), 1))
            masked_max_source_validation_df = masked_max_source_validation_df.withColumn("max_value", round(col("max_value"), 1))
            masked_min_sub_df = masked_min_source_validation_df.exceptAll(masked_min_result_df)
            masked_max_sub_df = masked_max_source_validation_df.subtract(masked_max_result_df)
            masked_row_count_validation = True if table_count == masked_write_validation_count else False
            masked_column_count_validation = True if (len(masked_values) * 2) == masked_write_table_column_count else False
            masked_min_validation = True if masked_min_sub_df.isEmpty() else False
            masked_max_validation = True if masked_max_sub_df.isEmpty() else False
            mask_val = 1
            masked_min_sub_df.show()
            masked_max_sub_df.show()
            print("masked_row_count_validation:", masked_row_count_validation, "masked_column_count_validation:", masked_column_count_validation)


        #if string columns only available means, then we are only checking counts
        except Py4JJavaError as pj:
            print('Will perform Only count validation, since no numerical column present We cant be able to perform min/max validations')
            masked_row_count_validation = True if table_count == masked_write_validation_count else False
            masked_column_count_validation = True if (len(masked_values) * 2) == masked_write_table_column_count else False
            print("masked_row_count_validation:", masked_row_count_validation, "masked_column_count_validation:", masked_column_count_validation)
            mask_val = 2

    #Exception Handling for string columns
    try:
        print('\n--------Data validation starts-----------')
        #passing dynamic queries to obtain results from source
        min_source_validation_df = dynamic_group_by_validation_temp(read_rdbms_name,read_db_name,read_schema_name,read_table_name,spark,full_column_list,read_driver,read_url,read_username,read_password,'min')     
        max_source_validation_df = dynamic_group_by_validation_temp(read_rdbms_name,read_db_name,read_schema_name,read_table_name,spark,full_column_list,read_driver,read_url,read_username,read_password,'max')
  
        row = min_source_validation_df.first()
        values = [(col_name, row[col_name]) for col_name in min_source_validation_df.columns]
        min_source_validation_df1 = spark.createDataFrame(values, ["column_name", "min_values"])  

        row1 = max_source_validation_df.first()
        values = [(col_name, row1[col_name]) for col_name in max_source_validation_df.columns]
        max_source_validation_df1 = spark.createDataFrame(values, ["column_name", "max_values"])  

        #rounding the df
        min_source_validation_df = min_source_validation_df1.withColumn("min_values", round(col("min_values"), 1))
        max_source_validation_df = max_source_validation_df1.withColumn("max_values", round(col("max_values"), 1))

        max_source_validation_df.show()
        min_source_validation_df.show()      
        #subtracting two df to ensure there is no unmatching values
        ############consider using subtract its performing faster
        min_sub_df = min_source_validation_df.subtract(min_result_df)
        max_sub_df = max_source_validation_df.subtract(max_result_df)

        #validation starts
        row_count_validation = True if table_count == write_validation_count else False
        ###Need to change this fails since full/hashload thre will be extra column in write_table_column_count
        column_count_validation = True if table_column_count == write_table_column_count else False
        min_validation = True if min_sub_df.isEmpty() else False
        max_validation = True if max_sub_df.isEmpty() else False
        data_val = 1
        print("source_table_row_count:", table_count, "target_blob_row_count:", write_validation_count)        

    #if string columns only available means, then we are only checking counts
    except Py4JJavaError as pj:
        print('Will perform Only count validation, since no numerical column present We cant be able to perform min/max validations')
        row_count_validation = True if table_count == write_validation_count else False
        column_count_validation = True if table_column_count == write_table_column_count else False
        print("source_table_row_count:", table_count, "target_blob_row_count:", write_validation_count) 
        data_val = 2

    #getting results based on masking present or not & string/integer validation    
    if data_val == 1 and mask_val == 1:
        count_validation_variable=(row_count_validation and column_count_validation and masked_row_count_validation and masked_column_count_validation)
        min_validation_variable=(min_validation and masked_min_validation)
        max_validation_variable=(max_validation and masked_max_validation)
        validation_json_data={"client_source_id":source_id,"database_name":read_db_name,"schema_name":read_schema_name,"table_name":read_table_name,"count_validation":count_validation_variable,"min_validation":min_validation_variable,"max_validation":max_validation_variable}

        if (row_count_validation and column_count_validation and min_validation and max_validation and masked_row_count_validation and masked_column_count_validation and masked_min_validation and masked_max_validation):
            validation_key,validation_value = results_validation_pass()
        else:
            validation_key,validation_value = results_validation_fail()

        
    elif data_val == 1 and mask_val == 2:
        count_validation_variable=(row_count_validation and column_count_validation and masked_row_count_validation and masked_column_count_validation)
        min_validation_variable = min_validation
        max_validation_variable = max_validation            
        validation_json_data={"client_source_id":source_id,"database_name":read_db_name,"schema_name":read_schema_name,"table_name":read_table_name,"count_validation":count_validation_variable,"min_validation":min_validation_variable,"max_validation":max_validation_variable}
   
        if (row_count_validation and column_count_validation and min_validation and max_validation and masked_row_count_validation and masked_column_count_validation):
            validation_key,validation_value = results_validation_pass()
        else:
            validation_key,validation_value = results_validation_fail()

        
    elif data_val == 2 and mask_val == 1:
        count_validation_variable=(row_count_validation and column_count_validation and masked_row_count_validation and masked_column_count_validation)
        min_validation_variable = masked_min_validation
        max_validation_variable = masked_max_validation            
        validation_json_data={"client_source_id":source_id,"database_name":read_db_name,"schema_name":read_schema_name,"table_name":read_table_name,"count_validation":count_validation_variable,"min_validation":min_validation_variable,"max_validation":max_validation_variable}

        if (row_count_validation and column_count_validation and masked_row_count_validation and masked_column_count_validation and masked_min_validation and masked_max_validation):
            validation_key,validation_value = results_validation_partial_pass()
        else:
            validation_key,validation_value = results_validation_fail()


    elif data_val == 2 and mask_val == 2:
        count_validation_variable=(row_count_validation and column_count_validation and masked_row_count_validation and masked_column_count_validation)
        min_validation_variable = True
        max_validation_variable = True            
        validation_json_data={"client_source_id":source_id,"database_name":read_db_name,"schema_name":read_schema_name,"table_name":read_table_name,"count_validation":count_validation_variable,"min_validation":min_validation_variable,"max_validation":max_validation_variable}

        if (row_count_validation and column_count_validation and masked_row_count_validation and masked_column_count_validation):
            validation_key,validation_value = results_validation_partial_pass()
        else:
            validation_key,validation_value = results_validation_fail()


    elif data_val == 1 and mask_val == 0:
        count_validation_variable=(row_count_validation and column_count_validation)
        min_validation_variable = min_validation
        max_validation_variable = max_validation            
        validation_json_data={"client_source_id":source_id,"database_name":read_db_name,"schema_name":read_schema_name,"table_name":read_table_name,"count_validation":count_validation_variable,"min_validation":min_validation_variable,"max_validation":max_validation_variable}

        if (row_count_validation and column_count_validation and min_validation and max_validation):
            validation_key,validation_value = results_validation_pass()
        else:
            validation_key,validation_value = results_validation_fail()


    elif data_val == 2 and mask_val == 0:
        count_validation_variable=(row_count_validation and column_count_validation)
        min_validation_variable = True
        max_validation_variable = True            
        validation_json_data={"client_source_id":source_id,"database_name":read_db_name,"schema_name":read_schema_name,"table_name":read_table_name,"count_validation":count_validation_variable,"min_validation":min_validation_variable,"max_validation":max_validation_variable}

        if (row_count_validation and column_count_validation):
            validation_key,validation_value = results_validation_partial_pass()
        else:
            validation_key,validation_value = results_validation_fail()
    
    put_api(validation_url,validation_json_data)
    
    return validation_key,validation_value

        

#To check if partition there or not for a table
def partition_query_func(read_rdbms_name,read_db_name,read_schema_name,read_table_name):
    partition_dct={
        'mysql': f"SELECT max(PARTITION_NAME) as partition_name FROM information_schema.PARTITIONS WHERE TABLE_NAME = '{read_table_name}' AND TABLE_SCHEMA = '{read_db_name}' having COUNT(PARTITION_NAME) > 0",
        'mssql': f"SELECT partition_number FROM sys.partitions WHERE object_id = OBJECT_ID('{read_schema_name}.{read_table_name}')",
        'maria': f"SELECT max(PARTITION_NAME) as partition_name FROM information_schema.PARTITIONS WHERE TABLE_NAME = '{read_table_name}' AND TABLE_SCHEMA = '{read_db_name}' having COUNT(PARTITION_NAME) > 0",
        'postgres':f"SELECT child.relname AS partition_name FROM pg_class parent JOIN pg_inherits ON inhparent = parent.oid JOIN pg_class child ON inhrelid = child.oid WHERE parent.relname = '{read_table_name}'",
        'ibm':f"SELECT distinct(DATAPARTITIONNAME) FROM SYSCAT.DATAPARTITIONEXPRESSION a JOIN SYSCAT.DATAPARTITIONS b ON a.TABNAME = b.TABNAME AND a.TABSCHEMA=b.TABSCHEMA WHERE a.TABNAME = '{read_table_name}' AND a.TABSCHEMA  = '{read_schema_name}'"
    }
    return partition_dct[read_rdbms_name]


#To extract the partition names of the table
def partition_query_func_new(read_rdbms_name,read_db_name,read_schema_name,read_table_name):
    partition_dct={
        'mysql': f"SELECT distinct PARTITION_NAME as partition_name FROM information_schema.PARTITIONS WHERE TABLE_NAME = '{read_table_name}' AND TABLE_SCHEMA = '{read_db_name}'",
        'mssql': f"SELECT distinct partition_number FROM sys.partitions WHERE object_id = OBJECT_ID('{read_schema_name}.{read_table_name}')",
        'maria': f"SELECT distinct PARTITION_NAME as partition_name FROM information_schema.PARTITIONS WHERE TABLE_NAME = '{read_table_name}' AND TABLE_SCHEMA = '{read_db_name}'",
        'postgres':f"SELECT distinct child.relname AS partition_name FROM pg_class parent JOIN pg_inherits ON inhparent = parent.oid JOIN pg_class child ON inhrelid = child.oid WHERE parent.relname = '{read_table_name}'",
        'ibm':f"SELECT distinct DATAPARTITIONNAME FROM SYSCAT.DATAPARTITIONEXPRESSION a JOIN SYSCAT.DATAPARTITIONS b ON a.TABNAME = b.TABNAME AND a.TABSCHEMA=b.TABSCHEMA WHERE a.TABNAME = '{read_table_name}' AND a.TABSCHEMA  = '{read_schema_name}'"
    }
    return partition_dct[read_rdbms_name]



#To extract data based on partitions
def partition_get_data_func(read_rdbms_name,read_db_name,read_schema_name,read_table_name,partition,cluster_threshold,offset_value,table_columns,partition_function_name,partition_column_name):
    if read_rdbms_name.lower()=='mssql':
        quoted_columns = [f'[{col}]' if ' ' in col else col for col in table_columns]
        order_by_clause = ', '.join(quoted_columns)
    elif read_rdbms_name.lower()=='mysql' or read_rdbms_name.lower()=='maria':
        quoted_columns = [f'`{col}`' if ' ' in col else col for col in table_columns]
        order_by_clause = ', '.join(quoted_columns)
    elif read_rdbms_name not in ('postgres','ibm'): 
        order_by_clause = ', '.join(table_columns)
    else:
        quoted_columns = [f'"{col}"' for col in table_columns]
        order_by_clause = ', '.join(quoted_columns)
    partition_get_data_dct={
        'mysql':f"select * from `{read_table_name}` partition ({partition}) order by {order_by_clause} limit {cluster_threshold} offset {offset_value}",
        'mssql':f"SELECT * FROM [{read_schema_name}].[{read_table_name}] WHERE $PARTITION.{partition_function_name}([{partition_column_name}]) = {partition} order by {order_by_clause} offset {offset_value} rows fetch next {cluster_threshold} rows only",
        'maria':f"select * from `{read_table_name}` partition ({partition}) order by {order_by_clause} limit {cluster_threshold} offset {offset_value}",
        'postgres':f"select * from \"{partition}\" order by {order_by_clause} limit {cluster_threshold} offset {offset_value}",
        'ibm':f"SELECT * FROM \"{read_schema_name}\".\"{read_table_name}\" ORDER BY {order_by_clause} limit {cluster_threshold} offset {offset_value}"
    }
    return partition_get_data_dct[read_rdbms_name]



#To get extra details for the partitions if source rdbms is mssql
def partition_get_details_func(read_rdbms_name,read_db_name,read_schema_name,read_table_name):
    partition_get_details_dct={
        'mssql':f"SELECT pf.name function_name,c.name column_name FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id JOIN sys.indexes i ON t.object_id = i.object_id JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id JOIN sys.partition_functions pf ON ps.function_id = pf.function_id JOIN sys.index_columns ic ON i.index_id = ic.index_id AND i.object_id = ic.object_id JOIN sys.columns c ON t.object_id = c.object_id AND ic.column_id = c.column_id WHERE s.name = '{read_schema_name}' AND t.name = '{read_table_name}'"
    }
    return partition_get_details_dct[read_rdbms_name]


#To extract column list
def table_column_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name):
    partition_column_dct={
        'mysql':f"select * from `{read_table_name}` limit 0",
        'maria':f"select * from `{read_table_name}` limit 0",
        'mssql':f"SELECT TOP 0 * FROM [{read_schema_name}].[{read_table_name}]",
        'postgres':f"select * from \"{read_table_name}\" limit 0",
        'ibm':f"SELECT * FROM \"{read_schema_name}\".\"{read_table_name}\" FETCH FIRST 0 ROWS ONLY"}
    
    return partition_column_dct[read_rdbms_name]


#Non cluster limit approach
def non_cluster_limit_func(read_rdbms_name,read_table_name,cluster_threshold,offset_number,table_columns,read_schema_name,index_column_count,index_column_list):
    order_by_clause = order_by_clause_func(read_rdbms_name,table_columns)
    order_by_clause = ',' + order_by_clause if len(order_by_clause) !=0 else order_by_clause
    index_column = composite_non_clustered_query_func(read_rdbms_name,index_column_count,index_column_list)
        
    lmt = {'mysql': f"select * from `{read_table_name}` order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_number}",
          'maria': f"select * from `{read_table_name}` order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_number}",
          'postgres': f'select * from "{read_table_name}" order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_number}',
          'ibm': f'select * from "{read_schema_name}"."{read_table_name}" order by {index_column}{order_by_clause} limit {cluster_threshold} offset {offset_number}',
          'mssql': f"SELECT * FROM [{read_schema_name}].[{read_table_name}] ORDER BY {index_column}{order_by_clause} OFFSET {offset_number} ROWS FETCH NEXT {cluster_threshold} ROWS ONLY"}
    
    return lmt[read_rdbms_name]


#decision function to go for non cluster approach or non cluster limit approach
def non_cluster_decision_func(read_rdbms_name,read_schema_name,read_table_name,table_count,main_index_column,spark,driver,url,username,password,cluster_threshold):
    distinct_table_count = {'mysql': f'select count(distinct(`{main_index_column}`)) from `{read_table_name}`',
                           'maria': f'select count(distinct(`{main_index_column}`)) from `{read_table_name}`',
                           'postgres': f'select count(distinct("{main_index_column}")) from "{read_table_name}"',
                           'ibm': f'select count(distinct("{main_index_column}")) as cnt from "{read_schema_name}"."{read_table_name}"',
                           'mssql': f'select count(distinct([{main_index_column}])) as cnt from [{read_schema_name}].[{read_table_name}]'}

    distinct_table_count_value = read_func(spark,driver,url,distinct_table_count[read_rdbms_name],username,password).rdd.flatMap(lambda x:x).collect()[0]
    avg_distinct_count = (table_count/distinct_table_count_value)
    result = 'yes' if avg_distinct_count > (cluster_threshold/4) else 'no'

    return result



#used in composite clustered approach
def composite_non_clustered_query_func(read_rdbms_name,index_column_count,index_column_list):
    for elements in range(index_column_count):
        if read_rdbms_name not in ('postgres','ibm','mssql'):
            index_column = f'`{index_column_list[elements]}`' if elements == 0 else index_column + f',`{index_column_list[elements]}`'
        elif read_rdbms_name in ('postgres','ibm'):
            index_column = f'"{index_column_list[elements]}"' if elements == 0 else index_column + f',"{index_column_list[elements]}"'
        else:
            index_column = f'[{index_column_list[elements]}]' if elements == 0 else index_column + f',[{index_column_list[elements]}]'

    return index_column


#used in composite clustered approach
def composite_new_clustered_query_func(read_rdbms_name,read_table_name,main_key,symbol,maxval,table_type,read_schema_name):

    column_data_type = column_datatype_func(main_key,table_type)
    
    new_clustered_query = {
        'int': {
        'mysql': f'select * from `{read_table_name}` where `{main_key}` {symbol} {maxval}',
        'maria': f'select * from `{read_table_name}` where `{main_key}` {symbol} {maxval}',
        'postgres': f'select * from "{read_table_name}" where "{main_key}" {symbol} {maxval}',
        'ibm': f'select * from "{read_schema_name}"."{read_table_name}" where "{main_key}" {symbol} {maxval}',
        'mssql': f'select * from [{read_schema_name}].[{read_table_name}] where [{main_key}] {symbol} {maxval}'
    },
        'string': {
        'mysql': f'select * from `{read_table_name}` where `{main_key}` {symbol} \'{maxval}\'',
        'maria': f'select * from `{read_table_name}` where `{main_key}` {symbol} \'{maxval}\'',
        'postgres': f'select * from "{read_table_name}" where "{main_key}" {symbol} \'{maxval}\'',
        'ibm': f'select * from "{read_schema_name}"."{read_table_name}" where "{main_key}" {symbol} \'{maxval}\'',
        'mssql': f'select * from [{read_schema_name}].[{read_table_name}] where [{main_key}] {symbol} \'{maxval}\''
    }
}
    
    return new_clustered_query[column_data_type][read_rdbms_name]


#used in composite clustered approach
def composite_temp_query_func(read_rdbms_name,column_name,current_symbol,maxval,table_type):

    column_data_type = column_datatype_func(column_name,table_type)

    
    temp_query_dict = {
        'int': {
        'mysql': f'AND `{column_name}` {current_symbol} {maxval}',
        'maria': f'AND `{column_name}` {current_symbol} {maxval}',
        'postgres': f'AND "{column_name}" {current_symbol} {maxval}',
        'ibm': f'AND "{column_name}" {current_symbol} {maxval}',
        'mssql': f'AND [{column_name}] {current_symbol} {maxval}'
    },
        'string': {
        'mysql': f'AND `{column_name}` {current_symbol} \'{maxval}\'',
        'maria': f'AND `{column_name}` {current_symbol} \'{maxval}\'',
        'postgres': f'AND "{column_name}" {current_symbol} \'{maxval}\'',
        'ibm': f'AND "{column_name}" {current_symbol} \'{maxval}\'',
        'mssql': f'AND [{column_name}] {current_symbol} \'{maxval}\''
    }
}

    return temp_query_dict[column_data_type][read_rdbms_name]



#to get first 10 integer columns from the table
def integer_list_validation_func(table_columns,table_type,masked_columns,type=None):
    integer_list = []
    while True:
            for item in range(len(table_columns)):
                column_type = column_datatype_func(table_columns[item],table_type)
                if type is None:
                    if column_type == 'int' and (table_columns[item] not in masked_columns):
                        integer_list.append(table_columns[item])
                else:
                    if column_type == 'int':
                        integer_list.append(table_columns[item])

                if len(integer_list) >= 10:
                    break
            break

    return integer_list


#Converting json into dataframe 
def table_json_to_df(spark,json_data,main_key_list,other_key_list):
    
        itr = -1
        new_itr = -1
        data = []
        sources_variable,sources_id_variable,databasetype_variable,databases_variable,schemas_variable = main_key_list
        full_list = main_key_list + other_key_list

        
        if len(other_key_list) == 3:
            table_name, volume_gb, table_desc = other_key_list
            for i in range(len(json_data[sources_variable])):
                source_name = json_data[sources_variable][i]
                for j in range(len(json_data[databases_variable][i])):
                    database_name = json_data[databases_variable][i][j]
                    itr += 1
                    schemas = json_data[schemas_variable][itr]
                    for k in range(len(schemas)):
                        schema_name = schemas[k]
                        new_itr += 1
                        tables = json_data[table_name][new_itr]
                        volume_gb_list = json_data[volume_gb][new_itr]
                        table_desc_list = json_data[table_desc][new_itr]
                        for table_ind,volgb_ind,table_desc_ind in zip(tables,volume_gb_list,table_desc_list):
                            data.append((source_name,database_name, schema_name, table_ind,volgb_ind,table_desc_ind))

        
        elif len(other_key_list) == 2:
            table_name, entity = other_key_list
            full_list = ["sources","client_source_ids","db_types","databases", "schemas","tables","load_status"]
            for i in range(len(json_data[sources_variable])):
                source_name = json_data[sources_variable][i]
                source_id = json_data[sources_id_variable][i]
                database_type = json_data[databasetype_variable][i]
                for j in range(len(json_data[databases_variable][i])):
                    database_name = json_data[databases_variable][i][j]
                    itr += 1
                    schemas = json_data[schemas_variable][itr]
                    for k in range(len(schemas)):
                        schema_name = schemas[k]
                        new_itr += 1
                        tables = json_data[table_name][new_itr]
                        entity_list = json_data[entity][new_itr]
                        for table_ind,entity_ind in zip(tables,entity_list):
                            data.append((source_name,source_id,database_type,database_name, schema_name, table_ind,entity_ind))

            
        elif len(other_key_list) == 1:
            table_name = other_key_list[0]
            for i in range(len(json_data[sources_variable])):
                source_name = json_data[sources_variable][i]
                source_id = json_data[sources_id_variable][i]
                database_type = json_data[databasetype_variable][i]
                for j in range(len(json_data[databases_variable][i])):
                    database_name = json_data[databases_variable][i][j]
                    itr += 1
                    schemas = json_data[schemas_variable][itr]
                    for k in range(len(schemas)):
                        schema_name = schemas[k]
                        new_itr += 1
                        tables = json_data[table_name][new_itr]
                        for table_ind in tables:
                            data.append((source_name,source_id,database_type,database_name, schema_name, table_ind))


        # Create DataFrame
        df = spark.createDataFrame(data, full_list)
        return df


#Converting masking columns and their statuses into dataframe
def column_masking_json_to_df(spark,json_data,main_key_list,other_key_list):
    
    itr = -1
    new_itr = -1
    othr_itr = -1
    data = []
    sources_variable,databasetype_variable,databases_variable,schemas_variable,table_variable = main_key_list
    full_list = main_key_list + other_key_list

    column_name,sensitive_classification = other_key_list
    for i in range(len(json_data[sources_variable])):
        source_name = json_data[sources_variable][i]
        database_type = json_data[databasetype_variable][i]
        for j in range(len(json_data[databases_variable][i])):
            database_name = json_data[databases_variable][i][j]
            itr += 1
            schemas = json_data[schemas_variable][itr]
            for k in range(len(schemas)):
                schema_name = schemas[k]
                new_itr += 1
                tables = json_data[table_variable][new_itr]
                for l in range(len(tables)):
                    table_name = tables[l]
                    othr_itr += 1
                    column_list = json_data[column_name][othr_itr]
                    sensitive_classification_list = json_data[sensitive_classification][othr_itr]
                    for column_ind,sensitive_classification_ind in zip(column_list,sensitive_classification_list):
                        data.append((source_name,database_type,database_name, schema_name, table_name, column_ind,sensitive_classification_ind))

    # Create DataFrame
    df = spark.createDataFrame(data, full_list)
    return df

#function to get maximum value of watermark column
def lastmodified_source_query_func(read_rdbms_name,read_table_name,column_name,symbol,maxval,cluster_threshold,table_type,read_schema_name,watermark_column):
    modified_dt = {'mysql': f"select max(`{watermark_column}`) as max_modified_date from `{read_table_name}`",
          'maria': f"select max(`{watermark_column}`) from `{read_table_name}`",
          'postgres': f'select max("{watermark_column}") from "{read_table_name}"',
          'ibm': f'select max("{watermark_column}") from "{read_schema_name}"."{read_table_name}"',
          'mssql': f"SELECT max([{watermark_column}]) FROM [{read_schema_name}].[{read_table_name}]"
        }
    
    return modified_dt[read_rdbms_name]
    

#custom class to get double in json which is usefull for posting data via api
class doubleQuoteDict(dict):
    def __str__(self):
        import json
        return json.dumps(self)
    

#Function to append data to endpoint
def post_api(url, data):
    import requests
    import json
    data = doubleQuoteDict(data)
    json_data = json.dumps(data)
    try:
        response = requests.post(url=url, data=json_data)
        response.raise_for_status()
        return response.json()["data"]
       
    except requests.exceptions.HTTPError as e:
        print("HTTP error occurred:", e)
        print(response.json()["message"])
        raise e
       
    except Exception as e:
        print("An error occurred:", e)
        print(response.json()["message"])
        raise e

#Function to overwrite data to endpoint
def put_api(url, data):
    import requests
    import json
    data = doubleQuoteDict(data)
    json_data = json.dumps(data)
    
    try:
        response = requests.put(url, data=json_data)
        response.raise_for_status()
        return response.json()["data"]
    
    except requests.exceptions.HTTPError as e:
        print("HTTP error occurred:", e)
        print(response.json()["message"])
        raise e
    
    except Exception as e:
        print("An error occurred:", e)
        print(response.json()["message"])
        raise e


#Function to get data from endpoint
def get_api(url):
    import requests
    import json
    try:
        response = requests.get(url=url)
        response.raise_for_status()
        return response.json()["data"]
       
    except requests.exceptions.HTTPError as e:
        raise e
       
    except Exception as e:
        raise e
    
   
#Function to partially overwrite data to endpoint
def patch_api(url, data):
    import requests
    import json
    json_data = json.dumps(data)
    try:
        response = requests.patch(url=url, data=json_data)
        response.raise_for_status()
        return response.json()["data"]
       
    except requests.exceptions.HTTPError as e:
        print("HTTP error occurred:", e)
        print(response.json()["message"])
        raise e
    
    except Exception as e:
        print("An error occurred:", e)
        print(response.json()["message"])
        raise e
    

#Function to append database driver based on selected rdbms_type
def config_json_to_df(config_json):
    import pandas as pd
    config_df = pd.DataFrame.from_dict(config_json, orient='index').T

    driver_dict = {"mysql" : "com.mysql.jdbc.Driver",
                   "maria" : "com.mysql.jdbc.Driver",
                   "postgres" : "org.postgresql.Driver",
                   "ibm" : "com.ibm.db2.jcc.DB2Driver",
                   "mssql": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }

    config_df['db_driver'] = config_df['db_type'].map(driver_dict)

    return config_df


'''

########  These functions are not used for now
########  But Dont delete since it may needed in future 

def cnt_metadata_extrct_func(read_rdbms_name,table_name,read_db_name,read_schema_name=None):
    dct = {'mysql': f"SELECT SUM(TABLE_ROWS) AS Total_Rows FROM information_schema.tables WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{read_db_name}'",
           'maria': f"SELECT SUM(TABLE_ROWS) AS Total_Rows FROM information_schema.tables WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{read_db_name}'",
           'postgres': f"SELECT n_live_tup AS row_count FROM pg_stat_user_tables WHERE schemaname = 'public' and relname = '{table_name}'",
           'ibm': f"SELECT CARD FROM SYSCAT.TABLES WHERE TABSCHEMA = '{read_schema_name}' AND TABNAME = '{table_name}'",
          'mssql': f"SELECT SUM(row_count) as total_row_count FROM sys.dm_db_partition_stats WHERE object_id = OBJECT_ID('{read_schema_name}.{table_name}') AND (index_id = 0 or index_id = 1)"}

    return dct[read_rdbms_name]



def auto_inc_query_func(read_rdbms_name,read_db_name,read_table_name,column_name,read_schema_name):
    auto_inc = {'mysql': f"SELECT column_name, extra FROM information_schema.columns WHERE table_schema = '{read_db_name}' AND table_name = '{read_table_name}' AND column_name = '{column_name}' AND extra = 'auto_increment'",
                'maria': f"SELECT column_name, extra FROM information_schema.columns WHERE table_schema = '{read_db_name}' AND table_name = '{read_table_name}' AND column_name = '{column_name}' AND extra = 'auto_increment'",
                'postgres': f"SELECT * FROM information_schema.sequences WHERE sequence_name = '{read_table_name}_{column_name}_seq'",
                'ibm': f"SELECT COLNAME FROM SYSCAT.COLUMNS WHERE TABSCHEMA = '{read_schema_name}' AND TABNAME = '{read_table_name}' AND GENERATED = 'A'",
                'mssql': f"SELECT CASE WHEN COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 THEN 'YES' END AS IsAutoIncrement FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{read_schema_name}' AND TABLE_NAME = '{read_table_name}' AND COLUMN_NAME = '{column_name}' AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1"
               }
    return auto_inc[read_rdbms_name]


def partition_type_query_func(read_rdbms_name,read_db_name,read_table_name):
    partition_type_dct={
        'mysql':f"SELECT PARTITION_METHOD FROM information_schema.PARTITIONS WHERE TABLE_NAME = '{read_table_name}' AND TABLE_SCHEMA = '{read_db_name}'",
        'mssql':f"SELECT pf.type_desc FROM sys.tables t JOIN sys.indexes i ON t.object_id = i.object_id JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id JOIN sys.partition_functions pf ON ps.function_id = pf.function_id WHERE t.name = '{read_table_name}'",
        'maria':f"SELECT PARTITION_METHOD FROM information_schema.PARTITIONS WHERE TABLE_NAME = '{read_table_name}' AND TABLE_SCHEMA = '{read_db_name}'",
        'postgres':f"SELECT CASE WHEN partstrat = 'l' THEN 'List' WHEN partstrat = 'r' THEN 'Range' WHEN partstrat = 'h' THEN 'Hash' ELSE 'Unknown' END AS partition_type FROM pg_partitioned_table WHERE partrelid = '\"{read_table_name}\"'::regclass",
        'ibm':f"SELECT PARTITION_TYPE FROM SYSCAT.TABLES WHERE TABSCHEMA = {'default_schema'} AND TABNAME = '{read_table_name}'"
    }
    return partition_type_dct[read_rdbms_name]

'''

    
        
    

#############################################################################################################################################################
#############################################################################################################################################################




#This class is common and used for all tables
class CommonDataProcessing:

    #Creating spark sessions, converting input files to dictionaries & starting tracker table
    @staticmethod
    def logging_mechanism_function(config_spark_url,db_config_spark_url,target_details,masking_confirmation_url,masked_columns_url,master_tracker_get_url,exec_file_path,connection_id):
    
        #importing libraries
        import requests
        import json
        import pandas as pd
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import max
        from IPython.display import display

        
        #using items in pandas 
        pd.DataFrame.iteritems = pd.DataFrame.items
        #Getting db_config details from the endpoint
        db_config_spark_url = db_config_spark_url+f"{connection_id}" 
        masking_confirmation_url = masking_confirmation_url+f"{connection_id}"
        config_json=get_api(db_config_spark_url)
        db_config_df=config_json_to_df(config_json)

        #getting source_id
        source_id = connection_id


        #getting jar values
        db_config_jars_df=db_config_df['jdbc_jar'].drop_duplicates().dropna()
        jars=''
        for i,value in enumerate(db_config_jars_df):
            if i < len(db_config_jars_df) - 1:
                value = value + ';'
            jars += value.strip()


        #Importing
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col,lower
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    

        #Spark session starting
        spark = SparkSession.builder.appName('data_extraction').config("spark.driver.extraClassPath",jars) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

        
        #getting selected tables of user from the endpoints
        table_json={"client_source_details": ["id","db_type"],
                       "is_client_included_tables": 'true',
                       "table_details": ["name"]}
        table_json_data = post_api(config_spark_url,table_json)
        master_tracker_get_url = master_tracker_get_url.replace("client_source_id",str(source_id))
        master_tracker_json = get_api(master_tracker_get_url)
        #Reading files
        main_key_list = ["sources","client_source_ids","db_types","databases", "schemas"]
        other_key_list=["tables"]
        config_spark = table_json_to_df(spark,table_json_data,main_key_list,other_key_list)   
        config_spark = config_spark.filter(col('client_source_ids')==source_id)
        #getting already extracted tables from tracker endpoint
        if len(master_tracker_json):
            schema = StructType([
            StructField("log_id", IntegerType(), True),
            StructField("source_id", IntegerType(), True),
            StructField("source_name", StringType(), True),
            StructField("rdbms_type", StringType(), True),
            StructField("database_name", StringType(), True),
            StructField("schema_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("load_status", StringType(), True),
            StructField("start_timestamp", StringType(), True),
            StructField("end_timestamp", StringType(), True)
        ])            
            tracker_df =  spark.createDataFrame(master_tracker_json,schema)
            rename_map = {
                "source_name": "sources",
                "source_id": "client_source_ids",
                "rdbms_type": "db_types",
                "database_name": "databases",
                "schema_name": "schemas",
                "table_name": "tables"
                }
            for old_name, new_name in rename_map.items():
                tracker_df = tracker_df.withColumnRenamed(old_name, new_name)

        else:
            schema = StructType([
            StructField("databases", StringType(), True),
            StructField("end_timestamp", StringType(), True),
            StructField("load_status", StringType(), True),
            StructField("log_id", IntegerType(), True),
            StructField("db_types", StringType(), True),
            StructField("schemas", StringType(), True),
            StructField("client_source_ids", IntegerType(), True),
            StructField("sources", StringType(), True),
            StructField("start_timestamp", StringType(), True),
            StructField("tables", StringType(), True)
        ])
            tracker_df = spark.createDataFrame([], schema)


        #filtering tables based on source_id and load_status
        tracker_df = tracker_df.filter((col('client_source_ids')==source_id) & (lower(col('load_status')) != 'inprogress'))

        
        #To Only Extract tables which are not extracted previously
        from pyspark.sql.functions import row_number,lit
        from pyspark.sql.window import Window
        join_columns = ["sources","db_types","databases", "schemas", "tables"]
        w = Window().orderBy(lit('A'))
        config_spark = config_spark.withColumn("row_num", row_number().over(w))
        non_matching_df1 = config_spark.join(tracker_df, join_columns, "left_anti")
        config_spark = non_matching_df1.orderBy("row_num").drop("row_num")
        
        #Reading files
        db_config_df = db_config_df.drop('db_name', axis=1)
        db_config_spark = spark.createDataFrame(db_config_df)
        target_details = spark.read.csv(target_details,header = True)
        exec_details = spark.read.option("inferschema",True).csv(exec_file_path,header = True)

        
        
        #cluster threshold calculation
        exec_memory = int(exec_details.select('exec_memory').collect()[0][0])
        exec_instances = int(exec_details.select('exec_instances').collect()[0][0])
        tmp_ovr = exec_details.select('overhead_memory').collect()[0][0]
        memory_fraction = exec_details.select('memory_fraction').collect()[0][0]
        memory_storagefraction = exec_details.select('memory_storagefraction').collect()[0][0]
        overhead_memory = int(tmp_ovr) if tmp_ovr is not None else int(0)
        reserved_memory = 300
        general_memory = 0.5 if exec_memory < 8 else 1 if exec_memory < 16 else 2
        exec_memory_temp = int(exec_memory - general_memory)
        usable_memory = (exec_memory_temp*1024) - reserved_memory
        spark_memory = round(usable_memory * memory_fraction)
        execution_memory = round(spark_memory * memory_storagefraction)
        cluster_threshold_mb = (round(execution_memory * 0.85)) * exec_instances
        
        is_masking = False
        masking_confirmation_json_response = get_api(masking_confirmation_url)
        if masking_confirmation_json_response['is_mask_needed']:
            is_masking = True       
       
        #getting masked columns and their PII statuses in PII,Sensitive,None
        print("Masking Selected:",is_masking)
        if is_masking:
            sensitive_json={"client_source_details": ["db_type"],
              "is_client_included_tables": 'true',
              "table_details": ["name"],
              "column_details":["name","pii_status"]
            }
            sensitive_json_data = post_api(masked_columns_url,sensitive_json)
            main_key_list = ["sources","db_types","databases", "schemas","tables"]
            other_key_list = ["columns","pii_statuses"]
            masked_columns = column_masking_json_to_df(spark,sensitive_json_data,main_key_list,other_key_list).filter((lower(col('pii_statuses'))=='pii') | (lower(col('pii_statuses'))=='sensitive information'))
            masked_columns_dict = con_to_dictionary(masked_columns, 'tables','db_types','databases', 'columns','schemas')
        else:
            masked_columns_dict = None
            
        
        #converting df to dictionary
        db_dict = con_to_dictionary(db_config_spark,'db_type')
        table_dict = con_to_dictionary(config_spark, 'tables','db_types','databases','schemas')
        target_dict = con_to_dictionary(target_details, 'target_kind')
        
    
        return spark,db_dict,table_dict,target_dict,is_masking,masked_columns_dict,cluster_threshold_mb,source_id,master_tracker_get_url

        
###################################################################################################################################

    #to extract count of the table
    @staticmethod
    def count_extraction_function(spark,read_rdbms_name,read_table_name,read_db_name,read_driver,read_url,read_username,read_password,read_schema_name=None):
        
        #To derive the original count of the tables
        count_query = f"select count(*) from `{read_table_name}`" if read_rdbms_name not in ('postgres','ibm','mssql') else f'select count(*) as count_value from "{read_schema_name}"."{read_table_name}"' if read_rdbms_name in ('postgres','ibm') else f"select count(*) as count from [{read_schema_name}].[{read_table_name}]"
        count = read_func(spark,read_driver,read_url,count_query,read_username,read_password).rdd.flatMap(lambda x:x).collect()[0]
    
        #logs
        print(f'Count of the table: {count}')
    
        return count



###################################################################################################################################



#This class is method specific and only one method will be choosed for a particular table
class SpecificMethodProcessing:


    #class intialization
    def __init__(self, spark, read_rdbms_name, write_rdbms_name, read_table_name, write_table_name, read_db_name, write_db_name, read_driver, write_driver, read_url, write_url, read_username, write_username, read_password, write_password, cluster_threshold, read_schema_name, account_name, container_name, folder_structure, write_format, target_kind, masked_values, masked_folder_structure,table_count,read_source_name,source_id,master_tracker_get_url,master_tracker_post_url,chunk_tracker_post_url):
        self.spark = spark
        self.read_rdbms_name = read_rdbms_name
        self.write_rdbms_name = write_rdbms_name
        self.read_table_name = read_table_name
        self.write_table_name = write_table_name
        self.read_db_name = read_db_name
        self.write_db_name = write_db_name
        self.read_driver = read_driver
        self.write_driver = write_driver
        self.read_url = read_url
        self.write_url = write_url
        self.read_username = read_username
        self.write_username = write_username
        self.read_password = read_password
        self.write_password = write_password
        self.cluster_threshold = cluster_threshold
        self.read_schema_name = read_schema_name
        self.account_name = account_name
        self.container_name = container_name
        self.folder_structure = folder_structure
        self.write_format = write_format
        self.target_kind = target_kind
        self.masked_values = masked_values
        self.masked_folder_structure = masked_folder_structure
        self.table_count = table_count
        self.read_source_name = read_source_name
        self.source_id = source_id
        self.master_tracker_get_url = master_tracker_get_url
        self.master_tracker_post_url = master_tracker_post_url
        self.chunk_tracker_post_url = chunk_tracker_post_url




##############################################################################################################################################
    
        

    #Limit & Offset Approach - If table has no indexing and partitioning
    def limit_offset_func(self):
        spark,read_rdbms_name,write_rdbms_name,read_table_name,write_table_name,read_db_name,write_db_name,read_driver,write_driver,read_url,write_url,read_username,write_username,read_password,write_password,cluster_threshold,account_name,container_name,folder_structure,write_format,target_kind,read_schema_name,masked_values,masked_folder_structure,table_count,read_source_name,source_id,master_tracker_get_url,master_tracker_post_url,chunk_tracker_post_url = self.spark,self.read_rdbms_name,self.write_rdbms_name,self.read_table_name,self.write_table_name,self.read_db_name,self.write_db_name,self.read_driver,self.write_driver,self.read_url,self.write_url,self.read_username,self.write_username,self.read_password,self.write_password,self.cluster_threshold,self.account_name,self.container_name,self.folder_structure,self.write_format,self.target_kind,self.read_schema_name,self.masked_values,self.masked_folder_structure,self.table_count,self.read_source_name,self.source_id,self.master_tracker_get_url,self.master_tracker_post_url,self.chunk_tracker_post_url

        print('Extracting Data Chunks in Limit and Offset Approach\n')

     
        #Importing
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        #from functions import read_func,limit_func,write_func,write_func_blob,validation_preparation_first,validation_preparation_second,table_column_func,integer_list_validation_func,masking_encrypt_data,task_logid,write_task_log,table_log,tracker_table_prep_func,cur_timestamp_func
        from datetime import datetime
        from pyspark.sql.functions import col
        from pyspark.storagelevel import StorageLevel
        from tqdm import tqdm


        #tracker table initialization
        success_log = {}
        mask_success_log = {}
        master_tracker_get_json = get_api(master_tracker_get_url)
        k = tracker_table_prep_func(spark,master_tracker_get_json)
        

        #deriving number of batches of extraction
        number_of_iterations = (table_count // cluster_threshold)
        remaining = (table_count % cluster_threshold)
        if remaining > 0:
            number_of_iterations += 1
            
    
        #To get column list and type from the table
        table_query=table_column_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name)
        read_table = read_func(spark,read_driver,read_url,table_query,read_username,read_password)
        table_columns = read_table.columns
        table_type = read_table.dtypes

        #defining variables
        table_column_count = len(table_columns)
        itr,offset,wrtcnt,tmp,write_validation_count,masked_write_validation_count=0,0,0,0,0,0
        masked_write_table_column_count,masked_full_column_list = None,None
        
        
        #only picking first 10 integer columns from all columns
        integer_list = integer_list_validation_func(table_columns,table_type,masked_values)
        masked_integer_list = integer_list_validation_func(masked_values,table_type,None,'mask') if len(masked_values) > 0 else None
        
    
        #Extraction starts
        print('-----Data Extraction Progress-----')
        progress_bar = tqdm(total=number_of_iterations, desc="Processing", unit="iteration")
        for tqdm_range in range(number_of_iterations):

            #Intializing tracker table variables
            read_status,write_status = 'FAIL','FAIL'
            mask_read_status = 'FAIL' if len(masked_values) > 0 else 'NA'
            mask_write_status = 'FAIL' if len(masked_values) > 0 else 'NA'
            
            #Master tracker table started
            if tqdm_range == 0:
                table_status = 'Inprogress'
                formatted_timestamp = cur_timestamp_func()
                task_log_num = task_logid()
                log_list = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status,formatted_timestamp]
                #table_log(spark,log_list,log_details,"append",blob_log_details)
                table_json_append = table_log(spark,log_list,"append")
                put_api(master_tracker_post_url,table_json_append)
            

            #This tmp variable will be used in validation
            tmp += 1


            try:
                #reading data based on limit function
                limit_query = limit_func(read_rdbms_name,read_table_name,cluster_threshold,offset,table_columns,read_schema_name)
                limit_extract_df = read_func(spark,read_driver,read_url,limit_query,read_username,read_password)
                
                #iterating variables
                offset += cluster_threshold
                    
                
                #persisting data
                limit_extract_df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                read_status='PASS'
            
    
                #check if its empty, so that it means we extracted completely
                #we can break the loop once we extarcted completely
                if limit_extract_df.isEmpty():
                    progress_bar.update(1)
                    break
                
                #chunk table append
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,write_db_name,cluster_threshold,"limit_offset"
                        ,itr,limit_query,formatted_timestamp]
                    #write_task_log(spark, log, log_details,blob_log_details,'append')
                    chunk_json_append = write_task_log(spark, log,'append')
                    put_api(chunk_tracker_post_url,chunk_json_append)
                else:
                    log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,account_name+'_'+container_name+'_'+folder_structure,cluster_threshold,"limit_offset"
                        ,itr,limit_query,formatted_timestamp]
                    #write_task_log(spark, log, log_details,blob_log_details,'append')
                    chunk_json_append = write_task_log(spark, log,'append')
                    put_api(chunk_tracker_post_url,chunk_json_append)
                    

                #if masking available
                if len(masked_values) > 0:
                    encrypt_udf = udf(masking_encrypt_data, StringType())
                    limit_masked = limit_extract_df.select(*masked_values)
                    limit_masked.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                    for masked_df_column in limit_masked.columns:
                        limit_masked = limit_masked.withColumn(f"masked_{masked_df_column}",encrypt_udf(limit_masked[masked_df_column])) 
                    for mask_column in masked_values:
                        limit_extract_df = limit_extract_df.withColumn(mask_column, encrypt_udf(limit_extract_df[mask_column]))
                    mask_read_status = 'PASS'


                #write mode definition
                mode = 'overwrite' if itr == 0 else 'append'
                #writing to target
                if target_kind == 'rdbms':
                    write_func(limit_extract_df,mode,write_driver,write_url,write_table_name,write_username,write_password)
                    write_status='PASS'
                    if len(masked_values) > 0:
                        write_func(limit_masked,mode,write_driver,write_url,f"masked_{write_table_name}",write_username,write_password)
                        mask_write_status = 'PASS'
                elif target_kind == 'blob':
                    
                    formatted_timestamp = cur_timestamp_func()
                    write_func_blob(limit_extract_df,mode,account_name,container_name,folder_structure,write_format)
                    formatted_timestamp = cur_timestamp_func()                   
                    write_status='PASS'
                    if len(masked_values) > 0:
                        write_func_blob(limit_masked,mode,account_name,container_name,masked_folder_structure,write_format)
                        mask_write_status = 'PASS'
                            
                #chunk table overwrite
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log ,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                else:
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log ,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                    
                success_log[itr] = write_status
                mask_success_log[itr] = mask_write_status
    
    
                #iterating variables
                wrtcnt += cluster_threshold
                itr += 1
                import time
                time.sleep(5) if write_rdbms_name == 'ibm' else None


            except Exception as error:
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                else:
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite) 
                success_log[itr] = write_status
                mask_success_log[itr] = mask_write_status           
               
            
            
            #Validation first part
            if tmp == 1:
                tmp_write_validation_count,max_df,min_df,full_column_list,write_table_column_count = validation_preparation_first(spark,integer_list,limit_extract_df,tmp)
                if len(masked_values) > 0:
                    tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list,masked_write_table_column_count = validation_preparation_first(spark,masked_integer_list,limit_masked,tmp)
            else:
                tmp_write_validation_count,max_df,min_df,full_column_list = validation_preparation_first(spark,integer_list,limit_extract_df,tmp,max_df,min_df)
                if len(masked_values) > 0:
                    tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list = validation_preparation_first(spark,masked_integer_list,limit_masked,tmp,masked_max_df,masked_min_df)

            
            #unpersisting data and calculating count of the target
            limit_extract_df.unpersist()
            if len(masked_values) > 0:
                limit_masked.unpersist()
                masked_write_validation_count += tmp_masked_write_validation_count
                
            write_validation_count += tmp_write_validation_count

            progress_bar.update(1)

        
        #validation second part
        progress_bar.close()
        print('-----Validation Preparation Progress-----')
        max_result_df,min_result_df = validation_preparation_second(spark,min_df,max_df)
        masked_max_result_df, masked_min_result_df = validation_preparation_second(spark,masked_min_df,masked_max_df) if len(masked_values) > 0 else (None,None)


        #master tracker table ending
        if ('FAIL' not in success_log.values()) and ('FAIL' not in mask_success_log.values()):
            table_status = 'Completed'
        else:
            table_status = 'Failed'
        log_list = [task_log_num,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status]
        #table_log(spark,log_list,log_details,"overwrite",blob_log_details)
        table_json_overwrite = table_log(spark,log_list,"overwrite", table_json_append)
        #put_api(master_tracker_post_url,table_json_overwrite)


        return write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite


##############################################################################################################################################


    #If Single Primary key is available
    def clustered_index_function(self,column_name):


        print('Extracting Data Chunks in Clustered Index Approach\n')

        spark,read_rdbms_name,write_rdbms_name,read_table_name,write_table_name,read_db_name,write_db_name,read_driver,write_driver,read_url,write_url,read_username,write_username,read_password,write_password,cluster_threshold,account_name,container_name,folder_structure,write_format,target_kind,read_schema_name,masked_values,masked_folder_structure,table_count,read_source_name,source_id,master_tracker_get_url,master_tracker_post_url,chunk_tracker_post_url = self.spark,self.read_rdbms_name,self.write_rdbms_name,self.read_table_name,self.write_table_name,self.read_db_name,self.write_db_name,self.read_driver,self.write_driver,self.read_url,self.write_url,self.read_username,self.write_username,self.read_password,self.write_password,self.cluster_threshold,self.account_name,self.container_name,self.folder_structure,self.write_format,self.target_kind,self.read_schema_name,self.masked_values,self.masked_folder_structure,self.table_count,self.read_source_name,self.source_id,self.master_tracker_get_url,self.master_tracker_post_url,self.chunk_tracker_post_url
        
        #Importing
        from pyspark.sql.functions import col,udf,concat_ws,sha2
        #from functions import read_func,write_func,write_func_blob,composite_clustered_query_func,validation_preparation_first,validation_preparation_second,table_column_func,integer_list_validation_func,masking_encrypt_data,task_logid,write_task_log,table_log,tracker_table_prep_func,cur_timestamp_func
        from datetime import datetime
        #from delta.tables import DeltaTable
        from pyspark.storagelevel import StorageLevel
        from pyspark.sql.types import StringType
        from tqdm import tqdm

        
        #tracker table intialization
        success_log = {}
        mask_success_log = {}
        master_tracker_get_json = get_api(master_tracker_get_url)
        k = tracker_table_prep_func(spark,master_tracker_get_json)

        
        #deriving no of batches
        number_of_iterations = (table_count // cluster_threshold) 
        remaining = (table_count % cluster_threshold)
        if remaining > 0:
            number_of_iterations += 1
      
    
        #variables defining
        maxval,itr,write_validation_count,tmp,masked_write_validation_count = 0,0,0,0,0
        wrtcnt = cluster_threshold
        masked_write_table_column_count,masked_full_column_list = None,None
        #hardcoded for now
        is_incremental_load = False
        has_watermark_column = False



        #To get column list from the table
        table_query=table_column_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name)
        read_table = read_func(spark,read_driver,read_url,table_query,read_username,read_password)
        table_type = read_table.dtypes
        table_columns = read_table.columns
        new_table_columns = table_columns.copy()
        table_column_count = len(table_columns)
        
        

        #removing the clustered index column from the list
        table_columns.remove(column_name)

        #only picking first 10 integer columns from all columns
        integer_list = integer_list_validation_func(new_table_columns,table_type,masked_values)
        masked_integer_list = integer_list_validation_func(masked_values,table_type,None,'mask') if len(masked_values) > 0 else None

    
        #Getting first value since it is needed to start the process
        maxval_query = f'select `{column_name}` from `{read_table_name}` order by `{column_name}` limit 1' if read_rdbms_name not in ('postgres','ibm','mssql') else f'select "{column_name}" from "{read_table_name}" order by "{column_name}" limit 1' if read_rdbms_name in ('postgres','ibm') else f'SELECT TOP 1 [{column_name}] FROM [{read_schema_name}].[{read_table_name}] order by [{column_name}]'
        maxval_df = read_func(spark,read_driver,read_url,maxval_query,read_username,read_password)
        maxval = maxval_df.rdd.flatMap(lambda x:x).collect()[0]
      
        
    
        #Extraction starts
        print('-----Data Extraction Progress-----')
        progress_bar = tqdm(total=number_of_iterations, desc="Processing", unit="iteration")
        for tqdm_range in range(number_of_iterations):
            #intializing tracker table varaiables
            read_status,write_status = 'FAIL','FAIL'
            mask_read_status = 'FAIL' if len(masked_values) > 0 else 'NA'
            mask_write_status = 'FAIL' if len(masked_values) > 0 else 'NA'
            #master table started
            if tqdm_range == 0:
                table_status = 'Inprogress'
                formatted_timestamp = cur_timestamp_func()
                task_log_num = task_logid()
                log_list = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status,formatted_timestamp]
                #table_log(spark,log_list,log_details,"append",blob_log_details)
                table_json_append = table_log(spark,log_list,"append")
                put_api(master_tracker_post_url,table_json_append)
                

            #This tmp variable will be used in validation
            tmp += 1
          


            try:
    
                #Defining query and extracting data in chunks
                symbol = '>=' if itr == 0 else '>'
                #If its incremental load and has watermark column
                if is_incremental_load and has_watermark_column:
                    print("***** Incremental load with watermark approach *****")
                    # watermark_column, last_inserted_date = get_api(some_url)
                    watermark_column, last_inserted_date = "transaction_date", "2024-06-27 12:02:00"
                    source_max_date_query = lastmodified_source_query_func(read_rdbms_name,read_table_name,column_name,symbol,maxval,cluster_threshold,table_type,read_schema_name,watermark_column)
                    source_max_date_df = read_func(spark,read_driver,read_url,source_max_date_query,read_username,read_password)
                    source_max_date = source_max_date_df.collect()[0][0]
                    clustered_query = composite_clustered_query_func(read_rdbms_name,read_table_name,column_name,symbol,maxval,cluster_threshold,table_type,read_schema_name,None,watermark_column,last_inserted_date,source_max_date)
                else:
                    clustered_query = composite_clustered_query_func(read_rdbms_name,read_table_name,column_name,symbol,maxval,cluster_threshold,table_type,read_schema_name,None,has_watermark_column)
                clustered = read_func(spark,read_driver,read_url,clustered_query,read_username,read_password)
                #adding hash column for full load
                if is_incremental_load is False and has_watermark_column is False:
                    print("***** Full load with adding Hash Column *****")
                    clustered = clustered.withColumn("Hash_Column", sha2(concat_ws("~", *table_columns), 256))   
                #adding hash column to compare with target 
                if is_incremental_load and has_watermark_column is False: 
                    print("***** Incremental load with hashing *****") 
                    clustered = clustered.withColumn("Hash_Column", sha2(concat_ws("~", *table_columns), 256))                 
                clustered.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                read_status='PASS'
    
        
                #check if its empty, so that it means we extracted completely
                #we can break the loop once we extarcted completely
                if clustered.isEmpty():
                    progress_bar.update(1)
                    break
                
                #chunk table append
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,write_db_name,cluster_threshold,"clustered"
                        ,itr,clustered_query,formatted_timestamp]
                    #write_task_log(spark, log, log_details,blob_log_details,'append')
                    chunk_json_append = write_task_log(spark, log,'append')
                    put_api(chunk_tracker_post_url,chunk_json_append)
                else:
                    log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,account_name+'_'+container_name+'_'+folder_structure,cluster_threshold,"clustered"
                        ,itr,clustered_query,formatted_timestamp]
                    #write_task_log(spark, log, log_details,blob_log_details,'append')
                    chunk_json_append = write_task_log(spark, log,'append')
             
                    put_api(chunk_tracker_post_url,chunk_json_append)
          

                #if masking available
                if len(masked_values) > 0:
                    msk_var = 0
                    encrypt_udf = udf(masking_encrypt_data, StringType())
                    cluster_masked = clustered.select(*masked_values)
                    cluster_masked.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                    for masked_df_column in cluster_masked.columns:
                        cluster_masked = cluster_masked.withColumn(f"masked_{masked_df_column}",encrypt_udf(cluster_masked[masked_df_column]))
                    for mask_column in masked_values:
                        if msk_var == 0:
                            clustered_new = clustered.withColumn(mask_column, encrypt_udf(clustered[mask_column]))
                        else:
                            clustered_new = clustered_new.withColumn(mask_column, encrypt_udf(clustered_new[mask_column]))
                        msk_var += 1
                    mask_read_status = 'PASS'
       
                #write mode definition
                mode = 'overwrite' if itr == 0 else 'append'
                #writing to target
                if target_kind == 'rdbms':
                    if len(masked_values) > 0:
                        clustered_new.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                        write_func(cluster_masked,mode,write_driver,write_url,f"masked_{write_table_name}",write_username,write_password) 
                        write_func(clustered_new,mode,write_driver,write_url,write_table_name,write_username,write_password)
                        mask_write_status = 'PASS'
                        write_status = 'PASS'
                    else:
                        write_func(clustered,mode,write_driver,write_url,write_table_name,write_username,write_password)
                        write_status = 'PASS'
                elif target_kind == 'blob':
                    if len(masked_values) > 0:
                        clustered_new.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                        write_func_blob(cluster_masked,mode,account_name,container_name,masked_folder_structure,write_format) if is_incremental_load is False else write_func_blob_incremental(cluster_masked,account_name,container_name,masked_folder_structure,column_name) if has_watermark_column else write_func_blob_incremental_hashing(cluster_masked,account_name,container_name,masked_folder_structure,column_name,"Hash_Column") 
                        write_func_blob(clustered_new,mode,account_name,container_name,folder_structure,write_format) if is_incremental_load is False else write_func_blob_incremental(clustered_new,account_name,container_name,folder_structure,column_name)  if has_watermark_column else write_func_blob_incremental_hashing(clustered_new,account_name,container_name,masked_folder_structure,column_name,"Hash_Column") 
                        mask_write_status = 'PASS'
                        write_status = 'PASS'
                    else:
                        write_func_blob(clustered,mode,account_name,container_name,folder_structure,write_format) if is_incremental_load is False else write_func_blob_incremental(clustered,account_name,container_name,folder_structure,column_name)  if has_watermark_column else write_func_blob_incremental_hashing(clustered,account_name,container_name,folder_structure,column_name,"Hash_Column") 
                        write_status = 'PASS'
                        
                        
                #chunk table overwrite
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                else:
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                    
                success_log[itr] = write_status
                mask_success_log[itr] = mask_write_status
    
    
                #iterating variables
                itr += 1
                import time
                time.sleep(5) if write_rdbms_name == 'ibm' else None


            except Exception as error:
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                else:
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append) 
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)    
                success_log[itr] = write_status
                mask_success_log[itr] = mask_write_status
            

            #since performing max on a datetime format column yiedling results in diff format, so we are casting to stringtype
            timestamp_columns = [col_name for col_name, col_type in clustered.dtypes if col_type == "timestamp"]
            for col_name in timestamp_columns:
                clustered = clustered.withColumn(col_name, col(col_name).cast(StringType()))
    
            #collecting max value from the current df, to use in next iteration
            #handled single quotes in data, since single quotes in data causing issues when giving in query
            try:
                maxval = clustered.agg({column_name: "max"}).collect()[0][0].strip().replace("'", "\\'")
            except AttributeError:
                maxval = clustered.agg({column_name: "max"}).collect()[0][0]

            #validation first part
            if len(masked_values) > 0: 
                if tmp == 1:
                    tmp_write_validation_count,max_df,min_df,full_column_list,write_table_column_count = validation_preparation_first(spark,integer_list,clustered_new,tmp)
                    tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list,masked_write_table_column_count = validation_preparation_first(spark,masked_integer_list,cluster_masked,tmp)

                else:
                    tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list = validation_preparation_first(spark,masked_integer_list,cluster_masked,tmp,masked_max_df,masked_min_df)
                    tmp_write_validation_count,max_df,min_df,full_column_list = validation_preparation_first(spark,integer_list,clustered_new,tmp,max_df,min_df)

                clustered_new.unpersist()


            else:
                if tmp == 1:
                    tmp_write_validation_count,max_df,min_df,full_column_list,write_table_column_count = validation_preparation_first(spark,integer_list,clustered,tmp)
                else:
                    tmp_write_validation_count,max_df,min_df,full_column_list = validation_preparation_first(spark,integer_list,clustered,tmp,max_df,min_df)
                    

         
            #unpersisting data and calculating target count
            clustered.unpersist()
            write_validation_count += tmp_write_validation_count
            if len(masked_values) > 0:
                cluster_masked.unpersist()
                masked_write_validation_count += tmp_masked_write_validation_count

            progress_bar.update(1)

        #validation second part
        progress_bar.close()
        print('-----Validation Preparation Progress-----')
        max_result_df,min_result_df = validation_preparation_second(spark,min_df,max_df)
        masked_max_result_df, masked_min_result_df = validation_preparation_second(spark,masked_min_df,masked_max_df) if len(masked_values) > 0 else (None,None)

        
        #master level tracker table ending
        if ('FAIL' not in success_log.values()) and ('FAIL' not in mask_success_log.values()):
            table_status = 'Completed'
        else:
            table_status = 'Failed'
        log_list = [task_log_num,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status]
        #table_log(spark,log_list,log_details,"overwrite",blob_log_details)
        table_json_overwrite = table_log(spark,log_list,"overwrite", table_json_append)
        #put_api(master_tracker_post_url,table_json_overwrite)

        

        return write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite
        

        '''
        
        #This below code is commented and not in use for now(but may needed in future)
        Why: First we planned to have two approaches for auto incremneted table and non auto incremented table. In auto incremented approach we dont want to get the max value for primary key for next iteration and thus efficient compared to latter. But we cant rely on this approach since, if some auto incremented rows deleted, this will extract duplicate values, and also however we planned to derive max for validations anyway, so its better use the other approach for all the single clustered index tables... But we dont want to delete this approach code either since it may be modified and can be used for other purposes
    
        
        #To check if the clustered table is auto incremented or not
        #auto_inc_query =  auto_inc_query_func(read_rdbms_name,read_db_name,read_table_name,column_name,read_schema_name)
        #auto_inc = read_func(spark,read_driver,read_url,auto_inc_query,read_username,read_password)
        
        
        #If auto increment
        else:
            print('auto increment')
    
            #Extraction starts
            while True:
    
                #Extracting data in batches
                clustered_query = clustered_query_func(read_rdbms_name,read_table_name,column_name,maxval,cluster_threshold,read_schema_name)
                clustered = read_func(spark,read_driver,read_url,clustered_query,read_username,read_password)
    
                #check if its empty, so that it means we extracted completely
                #we can break the loop once we extarcted completely
                if clustered.isEmpty():
                    break
    
                #write mode definition
                mode = 'overwrite' if itr == 0 else 'append'
    
    
                #writing to target rdbms
                if target_kind == 'rdbms':
                    write_validation_count += clustered.count()
                    write_func(clustered,mode,write_driver,write_url,write_table_name,write_username,write_password)
                elif target_kind == 'blob':
                    write_func_blob(clustered,mode,account_name,container_name,folder_structure,write_format)

                current_timestamp = datetime.now()
                formatted_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")
                print(f"iteration - {itr} is done, write table count is {wrtcnt}.... Timestamp - {formatted_timestamp}")
                wrtcnt += cluster_threshold
                maxval += cluster_threshold
                itr += 1
                import time
                time.sleep(5) if write_rdbms_name == 'ibm' else None

########## This is metadata count extraction steps, since for validation we need actual row count so we ignored this below

        Not used for now, but maybe needed in future, so dont delete it
        
                ##from functions import cnt_metadata_extrct_func
                #import sys
        
                #Extracting count from table metadata
                #count_metadata_query = cnt_metadata_extrct_func(read_rdbms_name,read_table_name,read_db_name) if read_rdbms_name not in ('ibm','mssql') else cnt_metadata_extrct_func(read_rdbms_name,read_table_name,read_db_name,read_schema_name)
                #print(count_metadata_query)
        
                #handling errors
                #try:
                    #count = read_func(spark,read_driver,read_url,count_metadata_query,read_username,read_password).collect()[0][0]
                    
                #except IndexError:
                    #print("IndexError: list index out of range. Please check the table name again.")
                    #sys.exit(1)

'''

###################################################################################################################################################

    #If Non clustered index available
    def non_clustered_function(self,index_column_list):

        print('Extracting Data Chunks in Non clustered Index Approach\n')

        spark,read_rdbms_name,write_rdbms_name,read_table_name,write_table_name,read_db_name,write_db_name,read_driver,write_driver,read_url,write_url,read_username,write_username,read_password,write_password,cluster_threshold,account_name,container_name,folder_structure,write_format,target_kind,read_schema_name,masked_values,masked_folder_structure,read_source_name,source_id,master_tracker_get_url,master_tracker_post_url,chunk_tracker_post_url = self.spark,self.read_rdbms_name,self.write_rdbms_name,self.read_table_name,self.write_table_name,self.read_db_name,self.write_db_name,self.read_driver,self.write_driver,self.read_url,self.write_url,self.read_username,self.write_username,self.read_password,self.write_password,self.cluster_threshold,self.account_name,self.container_name,self.folder_structure,self.write_format,self.target_kind,self.read_schema_name,self.masked_values,self.masked_folder_structure,self.read_source_name,self.source_id,self.master_tracker_get_url,self.master_tracker_post_url,self.chunk_tracker_post_url

        #Importing
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        #from functions import distinct_index_query_func,non_clustered_query_func,read_func,write_func,write_func_blob,column_datatype_func,order_by_clause_func,non_clustered_null_query_func,validation_preparation_first,validation_preparation_second,table_column_func,integer_list_validation_func,masking_encrypt_data,task_logid,write_task_log,table_log,tracker_table_prep_func,cur_timestamp_func
        from datetime import datetime
        from pyspark.storagelevel import StorageLevel
        from tqdm import tqdm

        #tracker table intialization
        success_log = {}
        mask_success_log = {}
        master_tracker_get_json = get_api(master_tracker_get_url)
        k = tracker_table_prep_func(spark,master_tracker_get_json)

    
        #To get column list from the table
        table_query=table_column_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name)
        read_table = read_func(spark,read_driver,read_url,table_query,read_username,read_password)
        table_type = read_table.dtypes
        table_columns = read_table.columns
        table_column_count = len(table_columns)
        new_table_columns = table_columns.copy()
        index_column_count = len(index_column_list)

        #to find the index key data type like int or string
        column_data_type = column_datatype_func(index_column_list[0],table_type,masked_values)
        
    
        #collecting distinct of index values and storing it in a list
        distinct_query = distinct_index_query_func(read_rdbms_name,read_db_name,read_table_name,index_column_list[0],read_schema_name)
        distinct_records = read_func(spark,read_driver,read_url,distinct_query,read_username,read_password).collect()
        distinct_records_count = len(distinct_records)

        #defining varaiables
        itr,tmp,write_validation_count,masked_write_validation_count = 0,0,0,0
        masked_write_table_column_count,masked_full_column_list = None,None
        

        #removing the index columns from the list
        for elements in range(index_column_count):
            table_columns.remove(index_column_list[elements])

        #only picking first 10 integer columns from all columns
        integer_list = integer_list_validation_func(new_table_columns,table_type)
        masked_integer_list = integer_list_validation_func(masked_values,table_type,None,'mask') if len(masked_values) > 0 else None


        #To dynamically create order by columns based on the indexing
        order_by_clause = order_by_clause_func(read_rdbms_name,table_columns)
        order_by_clause = ',' + order_by_clause if len(order_by_clause) !=0 else order_by_clause
    
        #Iterating through that list to extract data in batches
        print('-----Data Extraction Progress-----')
        progress_bar = tqdm(total=distinct_records_count, desc="Processing", unit="iteration")
        for item in range(distinct_records_count):
            
            #master level tracker table started
            if itr == 0:
                table_status = 'Inprogress'
                formatted_timestamp = cur_timestamp_func()
                task_log_num = task_logid()
                log_list = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status,formatted_timestamp]
                #table_log(spark,log_list,log_details,"append",blob_log_details)
                table_json_append = table_log(spark,log_list,"append")
                put_api(master_tracker_post_url,table_json_append)


            
            dist = distinct_records[item][0]
            offset_value = 0
    
            #Making sure that we are extracting data less than the cluster threshold using limit and offset
            #Extraction starts
            while True:

                #tracker table varaiables intialization
                read_status,write_status = 'FAIL','FAIL'
                mask_read_status = 'FAIL' if len(masked_values) > 0 else 'NA'
                mask_write_status = 'FAIL' if len(masked_values) > 0 else 'NA'

            

                #This tmp variable will be used in validation
                tmp += 1


                try:


                    #if my index distinct value is null
                    if dist is None:
                        non_cluster_query = non_clustered_null_query_func(read_rdbms_name,read_table_name,index_column_list,index_column_count,dist,cluster_threshold,offset_value,table_columns,read_schema_name)
    
                    #not null
                    else:      
                        non_cluster_query = non_clustered_query_func(read_rdbms_name,read_table_name,index_column_list,index_column_count,dist,cluster_threshold,offset_value,table_columns,read_schema_name,column_data_type)
                    
                    #reading data and persisting it
                    non_cluster_records = read_func(spark,read_driver,read_url,non_cluster_query,read_username,read_password)
                    
                    #iterating variables
                    offset_value += cluster_threshold
                    
                    #persisting data
                    non_cluster_records.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                    read_status='PASS'
                    
                    
                    #chunk table append
                    formatted_timestamp = cur_timestamp_func()
                    if target_kind == 'rdbms':
                        log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,write_db_name,cluster_threshold,"non_clustered"
                            ,itr,non_cluster_query,formatted_timestamp]
                        #write_task_log(spark, log, log_details,blob_log_details,'append')
                        chunk_json_append = write_task_log(spark, log,'append')
                        put_api(chunk_tracker_post_url,chunk_json_append)
                    else:
                        log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,account_name+'_'+container_name+'_'+folder_structure,cluster_threshold,"non_clustered"
                            ,itr,non_cluster_query,formatted_timestamp]
                        #write_task_log(spark, log, log_details,blob_log_details,'append')
                        chunk_json_append = write_task_log(spark, log,'append')
                        put_api(chunk_tracker_post_url,chunk_json_append)
                    
        
                    #check if its empty, so that it means we extracted completely
                    #we can break the loop for that index value once we extarcted completely
                    if non_cluster_records.isEmpty():
                        progress_bar.update(1)
                        break
    
                    #if masking available
                    if len(masked_values) > 0:
                        encrypt_udf = udf(masking_encrypt_data, StringType())
                        non_cluster_masked = non_cluster_records.select(*masked_values)
                        non_cluster_masked.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                        for masked_df_column in non_cluster_masked.columns:
                            non_cluster_masked = non_cluster_masked.withColumn(f"masked_{masked_df_column}",encrypt_udf(non_cluster_masked[masked_df_column]))
                        for mask_column in masked_values:
                            non_cluster_records = non_cluster_records.withColumn(mask_column, encrypt_udf(non_cluster_records[mask_column]))
                        mask_read_status = 'PASS'
        
                    #write mode definition
                    mode = 'overwrite' if itr == 0 else 'append'
        
                    #writing to target
                    if target_kind == 'rdbms':
                        write_func(non_cluster_records,mode,write_driver,write_url,write_table_name,write_username,write_password)
                        write_status = 'PASS'
                        if len(masked_values) > 0:
                            write_func(non_cluster_masked,mode,write_driver,write_url,f"masked_{write_table_name}",write_username,write_password)
                            mask_write_status = 'PASS'
                    elif target_kind == 'blob':
                        write_func_blob(non_cluster_records,mode,account_name,container_name,folder_structure,write_format)
                        write_status = 'PASS'
                        if len(masked_values) > 0:
                            write_func_blob(non_cluster_masked,mode,account_name,container_name,masked_folder_structure,write_format)
                            mask_write_status = 'PASS'
                            
                            
                            
                    #chunk table overwrite
                    formatted_timestamp = cur_timestamp_func()
                    if target_kind == 'rdbms':
                        log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                        #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                        chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                        put_api(chunk_tracker_post_url,chunk_json_overwrite)
                    else:
                        log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                        #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                        chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                        put_api(chunk_tracker_post_url,chunk_json_overwrite)
                        
                    success_log[itr] = write_status
                    mask_success_log[itr] = mask_write_status
                
    
                    #iterating varaiables
                    itr += 1

                except Exception as error:
                    formatted_timestamp = cur_timestamp_func()
                    if target_kind == 'rdbms':
                        log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                        #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                        chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                        put_api(chunk_tracker_post_url,chunk_json_overwrite)
                    else:
                        log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                        #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                        chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)  
                        put_api(chunk_tracker_post_url,chunk_json_overwrite)   
                    success_log[itr] = write_status
                    mask_success_log[itr] = mask_write_status




                #validation first part
                if tmp == 1:
                    tmp_write_validation_count,max_df,min_df,full_column_list,write_table_column_count = validation_preparation_first(spark,integer_list,non_cluster_records,tmp)
                    if len(masked_values) > 0:
                        tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list,masked_write_table_column_count = validation_preparation_first(spark,masked_integer_list,non_cluster_masked,tmp)
                
                else:
                    tmp_write_validation_count,max_df,min_df,full_column_list = validation_preparation_first(spark,integer_list,non_cluster_records,tmp,'index',max_df,min_df)
                    if len(masked_values) > 0:
                        tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list = validation_preparation_first(spark,masked_integer_list,non_cluster_masked,tmp,masked_max_df,masked_min_df)


                #unpersisting data and calculating target count
                non_cluster_records.unpersist()
                write_validation_count += tmp_write_validation_count

                if len(masked_values) > 0:
                    non_cluster_masked.unpersist()
                    masked_write_validation_count += tmp_masked_write_validation_count

            progress_bar.update(1)

        #validation second step
        progress_bar.close()
        print('-----Validation Preparation Progress-----')
        max_result_df,min_result_df = validation_preparation_second(spark,min_df,max_df)
        masked_max_result_df, masked_min_result_df = validation_preparation_second(spark,masked_min_df,masked_max_df) if len(masked_values) > 0 else (None,None)

        
        #master level tracker table ended
        if ('FAIL' not in success_log.values()) and ('FAIL' not in mask_success_log.values()):
            table_status = 'Completed'
        else:
            table_status = 'Failed'
        log_list = [task_log_num,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status]
        #table_log(spark,log_list,log_details,"overwrite",blob_log_details)
        table_json_overwrite = table_log(spark,log_list,"overwrite", table_json_append)
        #put_api(master_tracker_post_url,table_json_overwrite)
        

        return write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite
        




##########################################################################################################################################################

    #If partition is available
    def partition_func(self):

        spark,read_rdbms_name,write_rdbms_name,read_table_name,write_table_name,read_db_name,write_db_name,read_driver,write_driver,read_url,write_url,read_username,write_username,read_password,write_password,cluster_threshold,account_name,container_name,folder_structure,write_format,target_kind,read_schema_name,masked_values,masked_folder_structure,read_source_name,source_id,master_tracker_get_url,master_tracker_post_url,chunk_tracker_post_url = self.spark,self.read_rdbms_name,self.write_rdbms_name,self.read_table_name,self.write_table_name,self.read_db_name,self.write_db_name,self.read_driver,self.write_driver,self.read_url,self.write_url,self.read_username,self.write_username,self.read_password,self.write_password,self.cluster_threshold,self.account_name,self.container_name,self.folder_structure,self.write_format,self.target_kind,self.read_schema_name,self.masked_values,self.masked_folder_structure,self.read_source_name,self.source_id,self.master_tracker_get_url,self.master_tracker_post_url,self.chunk_tracker_post_url


        print('Extracting Data chunks in Partition Based Approach\n')

        #Importing
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        from pyspark.storagelevel import StorageLevel
        from datetime import datetime
        #from functions import read_func,write_func,partition_get_data_func,partition_get_details_func,table_column_func,validation_preparation_first,validation_preparation_second,partition_query_func_new,integer_list_validation_func,masking_encrypt_data,task_logid,write_task_log,table_log,tracker_table_prep_func,cur_timestamp_func
        from tqdm import tqdm


        #tracker table intialization
        success_log = {}
        mask_success_log = {}
        master_tracker_get_json = get_api(master_tracker_get_url)
        k = tracker_table_prep_func(spark,master_tracker_get_json)
        

        #To get the partition names
        partition_query=partition_query_func_new(read_rdbms_name,read_db_name,read_schema_name,read_table_name)
        partition_df=read_func(spark,read_driver,read_url,partition_query,read_username,read_password)

        #To get the table columns
        table_query=table_column_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name)
        read_table = read_func(spark,read_driver,read_url,table_query,read_username,read_password)
        table_columns = read_table.columns
        table_type = read_table.dtypes
        table_column_count = len(table_columns)
        new_table_columns = table_columns.copy()

        #only picking first 10 integer columns from all columns
        integer_list = integer_list_validation_func(new_table_columns,table_type,masked_values)
        masked_integer_list = integer_list_validation_func(masked_values,table_type,None,'mask') if len(masked_values) > 0 else None

        #Converting the partition name df to a list
        unique_values_list = partition_df.rdd.map(lambda x: x[0]).collect()

        #Importing and intializing variable
        tmp,write_validation_count,itr,masked_write_validation_count = 0,0,0,0
        partition_function_name,partition_column_name='',''
        masked_write_table_column_count,masked_full_column_list = None,None


        #for microsoft sql server we are extracting some more details
        if read_rdbms_name.lower()=='mssql':
            partition_get_details_query=partition_get_details_func(read_rdbms_name,read_db_name,read_schema_name,read_table_name)
            partition_get_details_df=read_func(spark,read_driver,read_url,partition_get_details_query,read_username,read_password)
            partition_function_name=partition_get_details_df.first()[0]
            partition_column_name=partition_get_details_df.first()[1]
            

        #Extraction starts
        print('-----Data Extraction Progress-----')
        progress_bar = tqdm(total=len(unique_values_list), desc="Processing", unit="iteration")
        for partition in unique_values_list:

            #master level tracker table started
            if itr == 0:
                table_status = 'Inprogress'
                formatted_timestamp = cur_timestamp_func()
                task_log_num = task_logid()
                log_list = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status,formatted_timestamp]
                #table_log(spark,log_list,log_details,"append",blob_log_details)
                table_json_append = table_log(spark,log_list,"append")
                put_api(master_tracker_post_url,table_json_append)
            
            offset_value = 0
            
            while True:

                #tracker table varaiables intialization
                read_status,write_status = 'FAIL','FAIL'
                mask_read_status = 'FAIL' if len(masked_values) > 0 else 'NA'
                mask_write_status = 'FAIL' if len(masked_values) > 0 else 'NA'
                    

                #This tmp variable will be used in validation
                tmp += 1


                try:
                    partition_get_data_query=partition_get_data_func(read_rdbms_name,read_db_name,read_schema_name,read_table_name,partition,cluster_threshold,offset_value,table_columns,partition_function_name,partition_column_name)
                    partition_get_data_df=read_func(spark,read_driver,read_url,partition_get_data_query,read_username,read_password)
                    
                    #iterating variables
                    offset_value += cluster_threshold
                    
                    #persisting data
                    partition_get_data_df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                    read_status='PASS'
                    
    
                    #check if its empty, so that it means we extracted completely
                    #we can break the loop for that index value once we extarcted completely        
                    if partition_get_data_df.isEmpty():
                        progress_bar.update(1)
                        break
                    
                    
                    #chunk table append
                    formatted_timestamp = cur_timestamp_func()
                    if target_kind == 'rdbms':
                        log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,write_db_name,cluster_threshold,"partition"
                            ,itr,partition_get_data_query,formatted_timestamp]
                        #write_task_log(spark, log, log_details,blob_log_details,'append')
                        chunk_json_append = write_task_log(spark, log,'append')
                        put_api(chunk_tracker_post_url,chunk_json_append)
                    else:
                        log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,account_name+'_'+container_name+'_'+folder_structure,cluster_threshold,"partition"
                            ,itr,partition_get_data_query,formatted_timestamp]
                        #write_task_log(spark, log, log_details,blob_log_details,'append')
                        chunk_json_append = write_task_log(spark, log,'append')
                        put_api(chunk_tracker_post_url,chunk_json_append)
                        
    
                    #if masking available
                    if len(masked_values) > 0:
                        encrypt_udf = udf(masking_encrypt_data, StringType())
                        partition_masked = partition_get_data_df.select(*masked_values)
                        partition_masked.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                        for masked_df_column in partition_masked.columns:
                            partition_masked = partition_masked.withColumn(f"masked_{masked_df_column}",encrypt_udf(partition_masked[masked_df_column]))
                        for mask_column in masked_values:
                            partition_get_data_df = partition_get_data_df.withColumn(mask_column, encrypt_udf(partition_get_data_df[mask_column]))
                        mask_read_status='PASS'
    
                    #write mode definition
                    mode = 'overwrite' if itr == 0 else 'append'
    
                    
                    #writing to target
                    if target_kind == 'rdbms':
                        write_func(partition_get_data_df,mode,write_driver,write_url,write_table_name,write_username,write_password)
                        write_status='PASS'
                        if len(masked_values) > 0:
                            write_func(partition_masked,mode,write_driver,write_url,f"masked_{write_table_name}",write_username,write_password)
                            mask_write_status='PASS'
                    elif target_kind == 'blob':
                        write_func_blob(partition_get_data_df,mode,account_name,container_name,folder_structure,write_format)
                        write_status='PASS'
                        if len(masked_values) > 0:
                            write_func_blob(partition_masked,mode,account_name,container_name,masked_folder_structure,write_format)
                            mask_write_status='PASS'
                            
                            
                    #chunk table overwrite
                    formatted_timestamp = cur_timestamp_func()
                    if target_kind == 'rdbms':
                        log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                        #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                        chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                        put_api(chunk_tracker_post_url,chunk_json_overwrite)
                    else:
                        log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                        #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                        chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                        put_api(chunk_tracker_post_url,chunk_json_overwrite)
                        
                    success_log[itr] = write_status
                    mask_success_log[itr] = mask_write_status
                        
    
                    #iterating variables
                    itr += 1



                except Exception as error:
                    formatted_timestamp = cur_timestamp_func()
                    if target_kind == 'rdbms':
                        log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                        #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                        chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                        put_api(chunk_tracker_post_url,chunk_json_overwrite)
                    else:
                        log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                        #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                        chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                        put_api(chunk_tracker_post_url,chunk_json_overwrite)     
                    success_log[itr] = write_status
                    mask_success_log[itr] = mask_write_status


                #validation first part
                if tmp == 1:
                    tmp_write_validation_count,max_df,min_df,full_column_list,write_table_column_count = validation_preparation_first(spark,integer_list,partition_get_data_df,tmp)
                    if len(masked_values) > 0:
                        tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list,masked_write_table_column_count = validation_preparation_first(spark,masked_integer_list,partition_masked,tmp)
                
                else:
                    tmp_write_validation_count,max_df,min_df,full_column_list = validation_preparation_first(spark,integer_list,partition_get_data_df,tmp,max_df,min_df)
                    if len(masked_values) > 0:
                        tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list = validation_preparation_first(spark,masked_integer_list,partition_masked,tmp,masked_max_df,masked_min_df)

                #unpersisting data and calculating target count
                partition_get_data_df.unpersist()
                write_validation_count += tmp_write_validation_count

                if len(masked_values) > 0:
                    partition_masked.unpersist()
                    masked_write_validation_count += tmp_masked_write_validation_count

            progress_bar.update(1)
        

        #validation second part
        progress_bar.close()
        print('-----Validation Preparation Progress-----')
        max_result_df,min_result_df = validation_preparation_second(spark,min_df,max_df)
        masked_max_result_df, masked_min_result_df = validation_preparation_second(spark,masked_min_df,masked_max_df) if len(masked_values) > 0 else (None,None)
        

        #master tracker table end
        if ('FAIL' not in success_log.values()) and ('FAIL' not in mask_success_log.values()):
            table_status = 'Completed'
        else:
            table_status = 'Failed'
        log_list = [task_log_num,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status]
        #table_log(spark,log_list,log_details,"overwrite",blob_log_details)
        table_json_overwrite = table_log(spark,log_list,"overwrite", table_json_append)
        #put_api(master_tracker_post_url,table_json_overwrite)
        

        return write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite



##########################################################################################################################################################

    #if more distinct values present in non clustered index this approach will be followed
    #it is very similar to limit and offset approach but here the table columns will be ordered in indexed query for faster retreival od data
    def non_cluster_limit_offset_func(self,index_column_list):

        print('Extracting Data Chunks in Non Cluster Limit and Offset Approach\n')

        spark,read_rdbms_name,write_rdbms_name,read_table_name,write_table_name,read_db_name,write_db_name,read_driver,write_driver,read_url,write_url,read_username,write_username,read_password,write_password,cluster_threshold,account_name,container_name,folder_structure,write_format,target_kind,read_schema_name,masked_values,masked_folder_structure,table_count,read_source_name,source_id,master_tracker_get_url,master_tracker_post_url,chunk_tracker_post_url = self.spark,self.read_rdbms_name,self.write_rdbms_name,self.read_table_name,self.write_table_name,self.read_db_name,self.write_db_name,self.read_driver,self.write_driver,self.read_url,self.write_url,self.read_username,self.write_username,self.read_password,self.write_password,self.cluster_threshold,self.account_name,self.container_name,self.folder_structure,self.write_format,self.target_kind,self.read_schema_name,self.masked_values,self.masked_folder_structure,self.table_count,self.read_source_name,self.source_id,self.master_tracker_get_url,self.master_tracker_post_url,self.chunk_tracker_post_url

        
        #Importing
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        #from functions import read_func,non_cluster_limit_func,write_func,write_func_blob,validation_preparation_first,validation_preparation_second,table_column_func,integer_list_validation_func,masking_encrypt_data,task_logid,write_task_log,table_log,tracker_table_prep_func,cur_timestamp_func
        from datetime import datetime
        from pyspark.storagelevel import StorageLevel
        from tqdm import tqdm


        #tracker table intialization
        success_log = {}
        mask_success_log = {}
        master_tracker_get_json = get_api(master_tracker_get_url)
        k = tracker_table_prep_func(spark,master_tracker_get_json)

        
        #calculating no of batches
        number_of_iterations = (table_count // cluster_threshold) 
        remaining = (table_count % cluster_threshold)
        if remaining > 0:
            number_of_iterations += 1
    
        #To get column list from the table and intializing variables
        table_query=table_column_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name)
        read_table = read_func(spark,read_driver,read_url,table_query,read_username,read_password)
        table_columns = read_table.columns
        table_type = read_table.dtypes
        new_table_columns = table_columns.copy()
        table_column_count = len(table_columns)
        index_column_count = len(index_column_list)
        itr,offset,wrtcnt,tmp,write_validation_count,masked_write_validation_count=0,0,0,0,0,0
        masked_write_table_column_count,masked_full_column_list = None,None

        #removing indexed columns from the list
        for elements in range(index_column_count):
            table_columns.remove(index_column_list[elements])

        #only picking first 10 integer columns from all columns
        integer_list = integer_list_validation_func(new_table_columns,table_type,masked_values)
        masked_integer_list = integer_list_validation_func(masked_values,table_type,None,'mask') if len(masked_values) > 0 else None
    
        #Extraction starts
        print('-----Data Extraction Progress-----')
        progress_bar = tqdm(total=number_of_iterations , desc="Processing", unit="iteration")
        for tqdm_range in range(number_of_iterations):

            #intializing tracker level table varaiables
            read_status,write_status = 'FAIL','FAIL'
            mask_read_status = 'FAIL' if len(masked_values) > 0 else 'NA'
            mask_write_status = 'FAIL' if len(masked_values) > 0 else 'NA'

            #master level tracker table started
            if tqdm_range == 0:
                table_status = 'Inprogress'
                formatted_timestamp = cur_timestamp_func()
                task_log_num = task_logid()
                log_list = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status,formatted_timestamp]
                #table_log(spark,log_list,log_details,"append",blob_log_details)
                table_json_append = table_log(spark,log_list,"append")
                put_api(master_tracker_post_url,table_json_append)

            
            
            #This tmp variable will be used in validation
            tmp += 1


            try:

                #reading data and persisting
                non_limit_query = non_cluster_limit_func(read_rdbms_name,read_table_name,cluster_threshold,offset,table_columns,read_schema_name,index_column_count,index_column_list)
                non_limit_extract_df = read_func(spark,read_driver,read_url,non_limit_query,read_username,read_password)
                
                #iterating variables
                offset += cluster_threshold
                
                #persisting data
                non_limit_extract_df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                read_status='PASS'
        
                #check if its empty, so that it means we extracted completely
                #we can break the loop once we extarcted completely
                if non_limit_extract_df.isEmpty():
                    progress_bar.update(1)
                    break
                
                
                #chunk table append
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,write_db_name,cluster_threshold,"non_clustered_limit"
                        ,itr,non_limit_query,formatted_timestamp]
                    #write_task_log(spark, log, log_details,blob_log_details,'append')
                    chunk_json_append = write_task_log(spark, log,'append')
                    put_api(chunk_tracker_post_url,chunk_json_append)
                else:
                    log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,account_name+'_'+container_name+'_'+folder_structure,cluster_threshold,"non_clustered_limit"
                        ,itr,non_limit_query,formatted_timestamp]
                    #write_task_log(spark, log, log_details,blob_log_details,'append')
                    chunk_json_append = write_task_log(spark, log,'append')
                    put_api(chunk_tracker_post_url,chunk_json_append)
                    
                    
    
                #if masking available
                if len(masked_values) > 0:
                    encrypt_udf = udf(masking_encrypt_data, StringType())
                    non_clustered_masked = non_limit_extract_df.select(*masked_values)
                    non_clustered_masked.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                    for masked_df_column in non_clustered_masked.columns:
                        non_clustered_masked = non_clustered_masked.withColumn(f"masked_{masked_df_column}",encrypt_udf(non_clustered_masked[masked_df_column]))
                    for mask_column in masked_values:
                        non_limit_extract_df = non_limit_extract_df.withColumn(mask_column, encrypt_udf(non_limit_extract_df[mask_column]))
                    mask_read_status='PASS'
        
                #write mode definition
                mode = 'overwrite' if itr == 0 else 'append'
        
                #writing to target rdbms
                if target_kind == 'rdbms':
                    write_func(non_limit_extract_df,mode,write_driver,write_url,write_table_name,write_username,write_password)
                    write_status='PASS'
                    if len(masked_values) > 0:
                        write_func(non_clustered_masked,mode,write_driver,write_url,f"masked_{write_table_name}",write_username,write_password)
                        mask_write_status='PASS'
                elif target_kind == 'blob':
                    write_func_blob(non_limit_extract_df,mode,account_name,container_name,folder_structure,write_format)
                    write_status='PASS'
                    if len(masked_values) > 0:
                        write_func_blob(non_clustered_masked,mode,account_name,container_name,masked_folder_structure,write_format)
                        mask_write_status='PASS'
                        
                        
                #chunk table overwrite
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                else:
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                    
                success_log[itr] = write_status
                mask_success_log[itr] = mask_write_status
    
    
                #iterating variables
                wrtcnt += cluster_threshold
                itr += 1

            

            except Exception as error:
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                else:
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append) 
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)    
                success_log[itr] = write_status
                mask_success_log[itr] = mask_write_status


            #validation first part
            if tmp == 1:
                tmp_write_validation_count,max_df,min_df,full_column_list,write_table_column_count = validation_preparation_first(spark,integer_list,non_limit_extract_df,tmp)
                if len(masked_values) > 0:
                    tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list,masked_write_table_column_count = validation_preparation_first(spark,masked_integer_list,non_clustered_masked,tmp)
                
            else:
                tmp_write_validation_count,max_df,min_df,full_column_list = validation_preparation_first(spark,integer_list,non_limit_extract_df,tmp,max_df,min_df)
                if len(masked_values) > 0:
                    tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list = validation_preparation_first(spark,masked_integer_list,non_clustered_masked,tmp,masked_max_df,masked_min_df)

            #unpersisting data and calculating target count
            non_limit_extract_df.unpersist()
            write_validation_count += tmp_write_validation_count

            if len(masked_values) > 0:
                    non_clustered_masked.unpersist()
                    masked_write_validation_count += tmp_masked_write_validation_count

            progress_bar.update(1)

        #validation second part
        progress_bar.close()
        print('-----Validation Preparation Progress-----')
        max_result_df,min_result_df = validation_preparation_second(spark,min_df,max_df)
        masked_max_result_df, masked_min_result_df = validation_preparation_second(spark,masked_min_df,masked_max_df) if len(masked_values) > 0 else (None,None)

        #master level tracker table end
        if ('FAIL' not in success_log.values()) and ('FAIL' not in mask_success_log.values()):
            table_status = 'Completed'
        else:
            table_status = 'Failed'
        log_list = [task_log_num,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status]
        #table_log(spark,log_list,log_details,"overwrite",blob_log_details)
        table_json_overwrite = table_log(spark,log_list,"overwrite", table_json_append)
        #put_api(master_tracker_post_url,table_json_overwrite)
        

        return write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite


##############################################################################################################################################

    #This is to handle composite primary keys
    #this logic is complex compared to the other methods
    #kindly contact the owner of the code before making any changes in it
    def composite_clustered_function(self,column_list):
    
        print('Multi Column Clustered Index Approach')

        spark,read_rdbms_name,write_rdbms_name,read_table_name,write_table_name,read_db_name,write_db_name,read_driver,write_driver,read_url,write_url,read_username,write_username,read_password,write_password,cluster_threshold,account_name,container_name,folder_structure,write_format,target_kind,read_schema_name,masked_values,masked_folder_structure,read_source_name,source_id,master_tracker_get_url,master_tracker_post_url,chunk_tracker_post_url = self.spark,self.read_rdbms_name,self.write_rdbms_name,self.read_table_name,self.write_table_name,self.read_db_name,self.write_db_name,self.read_driver,self.write_driver,self.read_url,self.write_url,self.read_username,self.write_username,self.read_password,self.write_password,self.cluster_threshold,self.account_name,self.container_name,self.folder_structure,self.write_format,self.target_kind,self.read_schema_name,self.masked_values,self.masked_folder_structure,self.read_source_name,self.source_id,self.master_tracker_get_url,self.master_tracker_post_url,self.chunk_tracker_post_url
    
        #Importing
        from pyspark.sql.functions import col,udf,concat_ws,sha2
        from datetime import datetime
        #from functions import read_func,write_func,write_func_blob,order_by_clause_func,composite_clustered_query_func,composite_new_clustered_query_func,composite_temp_query_func,table_column_func,validation_preparation_first,validation_preparation_second,column_datatype_func,integer_list_validation_func,masking_encrypt_data,task_logid,write_task_log,table_log,tracker_table_prep_func,cur_timestamp_func
        from pyspark.storagelevel import StorageLevel
        from pyspark.sql.types import StringType

        #tracker table intialization
        success_log = {}
        mask_success_log = {}
        master_tracker_get_json = get_api(master_tracker_get_url)
        k = tracker_table_prep_func(spark,master_tracker_get_json)
    
    
        #To get column list from the table and variables intialization
        column_count = len(column_list)
        main_key = column_list[0]
        table_query=table_column_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name)
        read_table = read_func(spark,read_driver,read_url,table_query,read_username,read_password)
        table_type = read_table.dtypes
        table_columns = read_table.columns
        new_table_columns = table_columns.copy()
        table_column_count = len(table_columns)

        #variables declaration
        #hardcoded for now
        is_incremental_load = True
        has_watermark_column = False        


        #removing indexed columns from the list
        for item in range(column_count):
            table_columns.remove(column_list[item])

        #only picking first 10 integer columns from all columns
        integer_list = integer_list_validation_func(new_table_columns,table_type,masked_values)
        masked_integer_list = integer_list_validation_func(masked_values,table_type,None,'mask') if len(masked_values) > 0 else None
    
    
        #getting first value since we dont know the first value of the table
        maxval_query = f'select `{main_key}` from `{read_table_name}` order by `{main_key}` limit 1' if read_rdbms_name not in ('postgres','ibm','mssql') else f'select "{main_key}" from "{read_table_name}" order by "{main_key}" limit 1' if read_rdbms_name in ('postgres','ibm') else f'SELECT TOP 1 [{main_key}] FROM [{read_schema_name}].[{read_table_name}] order by [{main_key}]'
        maxval_df = read_func(spark,read_driver,read_url,maxval_query,read_username,read_password)
        
        
        #getting max value od the clustered main key for next iteration
        #handled single quotes in data, since single quotes in data causing issues when giving in query
        try:
            maxval = maxval_df.rdd.flatMap(lambda x:x).collect()[0].strip().replace("'", "\\'")
        except AttributeError:
            maxval = maxval_df.rdd.flatMap(lambda x:x).collect()[0]

        
        #creating order by clause based on the primary key list
        order_by_clause = order_by_clause_func(read_rdbms_name,column_list)

        #intializing variables
        maxval_0 = maxval
        itr,tmp,write_validation_count,masked_write_validation_count,offset = 0,0,0,0,0
        masked_write_table_column_count,masked_full_column_list = None,None


        #Extraction Starts
        while True:
            print("######################    Main Iteration       ########################")
            
            #tracker table variables intialization
            read_status,write_status = 'FAIL','FAIL'
            mask_read_status = 'FAIL' if len(masked_values) > 0 else 'NA'
            mask_write_status = 'FAIL' if len(masked_values) > 0 else 'NA'

            #master level tracker table started
            if itr == 0:
                table_status = 'Inprogress'
                formatted_timestamp = cur_timestamp_func()
                task_log_num = task_logid()
                log_list = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status,formatted_timestamp]
                #table_log(spark,log_list,log_details,"append",blob_log_details)
                table_json_append = table_log(spark,log_list,"append")
                put_api(master_tracker_post_url,table_json_append)

            

            #this variable is for validation
            tmp += 1


            try:

                #getting data with main primary key
                symbol = '>=' if itr == 0 else '>'
                if is_incremental_load and has_watermark_column:
                    print("***** Incremental load with watermark approach *****")
                    # watermark_column, last_inserted_date = get_api(some_url)
                    watermark_column, last_inserted_date = "transaction_date", "2024-06-27 12:00:00"
                    source_max_date_query = lastmodified_source_query_func(read_rdbms_name,read_table_name,main_key,symbol,maxval,cluster_threshold,table_type,read_schema_name,watermark_column)
                    source_max_date_df = read_func(spark,read_driver,read_url,source_max_date_query,read_username,read_password)
                    source_max_date = source_max_date_df.collect()[0][0]
                    clustered_query = composite_clustered_query_incremental_func(read_rdbms_name,read_table_name,offset,cluster_threshold,read_schema_name,order_by_clause,watermark_column,last_inserted_date,source_max_date)
                else:
                    clustered_query = composite_clustered_query_func(read_rdbms_name,read_table_name,main_key,symbol,maxval,cluster_threshold,table_type,read_schema_name,order_by_clause,has_watermark_column)
                clustered = read_func(spark,read_driver,read_url,clustered_query,read_username,read_password)
                if is_incremental_load is False and has_watermark_column is False:
                    print("***** Full load with adding Hash Column *****")
                    clustered = clustered.withColumn("Hash_Column", sha2(concat_ws("~", *table_columns), 256))    
                if is_incremental_load and has_watermark_column is False: 
                    print("***** Incremental load with hashing *****") 
                    clustered = clustered.withColumn("Hash_Column", sha2(concat_ws("~", *table_columns), 256))         
                if is_incremental_load and has_watermark_column:
                    offset+=cluster_threshold
                clustered.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                read_status='PASS'
        
        
                #check if its empty, so that it means we extracted completely
                #we can break the loop once we extarcted completely
                if clustered.isEmpty():
                    break
                
                
                #chunk table append
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,write_db_name,cluster_threshold,"comp_clustered"
                        ,itr,clustered_query,formatted_timestamp]
                    #write_task_log(spark, log, log_details,blob_log_details,'append')
                    chunk_json_append = write_task_log(spark, log,'append')
                    put_api(chunk_tracker_post_url,chunk_json_append)
                else:
                    log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,account_name+'_'+container_name+'_'+folder_structure,cluster_threshold,"comp_clustered"
                        ,itr,clustered_query,formatted_timestamp]
                    #write_task_log(spark, log, log_details,blob_log_details,'append')
                    chunk_json_append = write_task_log(spark, log,'append')
                    put_api(chunk_tracker_post_url,chunk_json_append)
    
                #if masking available
                if len(masked_values) > 0:
                    msk_var = 0
                    encrypt_udf = udf(masking_encrypt_data, StringType())
                    clustered_masked = clustered.select(*masked_values)
                    clustered_masked.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                    for masked_df_column in clustered_masked.columns:
                        clustered_masked = clustered_masked.withColumn(f"masked_{masked_df_column}",encrypt_udf(clustered_masked[masked_df_column]))
                    for mask_column in masked_values:
                        if msk_var == 0:
                            clustered_masked_new = clustered.withColumn(mask_column, encrypt_udf(clustered[mask_column]))
                        else:
                            clustered_masked_new = clustered_masked_new.withColumn(mask_column, encrypt_udf(clustered_masked_new[mask_column]))
                        msk_var += 1
                    mask_read_status='PASS'
              
                #write mode definition
                mode = 'overwrite' if itr == 0 else 'append'
            
                #writing to target rdbms
                if target_kind == 'rdbms':
                    if len(masked_values) > 0:
                        clustered_masked_new.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                        write_func(clustered_masked,mode,write_driver,write_url,f"masked_{write_table_name}",write_username,write_password)
                        write_func(clustered_masked_new,mode,write_driver,write_url,write_table_name,write_username,write_password)
                        write_status='PASS'
                        mask_write_status='PASS'
                    else:
                        write_func(clustered,mode,write_driver,write_url,write_table_name,write_username,write_password)
                        write_status='PASS'
                elif target_kind == 'blob':
                    if len(masked_values) > 0:
                        clustered_masked_new.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                        write_func_blob(clustered_masked_new,mode,account_name,container_name,folder_structure,write_format) if is_incremental_load is False else write_func_blob_incremental(clustered_masked_new,account_name,container_name,masked_folder_structure,column_list) if has_watermark_column else write_func_blob_incremental_hashing(clustered_masked_new,account_name,container_name,masked_folder_structure,column_list,"Hash_Column")  
                        write_func_blob(clustered_masked,mode,account_name,container_name,masked_folder_structure,write_format) if is_incremental_load is False else write_func_blob_incremental(clustered_masked,account_name,container_name,masked_folder_structure,column_list) if has_watermark_column else write_func_blob_incremental_hashing(clustered_masked,account_name,container_name,masked_folder_structure,column_list,"Hash_Column") 
                        write_status='PASS'
                        mask_write_status='PASS'
                    else:
                        write_func_blob(clustered,mode,account_name,container_name,folder_structure,write_format) if is_incremental_load is False else write_func_blob_incremental(clustered,account_name,container_name,folder_structure,column_list) if has_watermark_column else write_func_blob_incremental_hashing(clustered,account_name,container_name,folder_structure,column_list,"Hash_Column") 
                        write_status='PASS'
                        
                        
                        
                #chunk table overwrite
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                else:
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                    
                success_log[itr] = write_status
                mask_success_log[itr] = mask_write_status
                
    
                #iterating variables
                itr += 1
                import time
                time.sleep(5) if write_rdbms_name == 'ibm' else None


            except Exception as error:
                formatted_timestamp = cur_timestamp_func()
                if target_kind == 'rdbms':
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)
                else:
                    log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                    #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                    chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append) 
                    put_api(chunk_tracker_post_url,chunk_json_overwrite)    
                success_log[itr] = write_status
                mask_success_log[itr] = mask_write_status
            
            
            print(f"Main iteration - {itr} is done, write table count is {cluster_threshold}.... Timestamp - {formatted_timestamp}")
            
            

            
            #since performing max on a datetime format column yiedling results in diff format, so we are casting to stringtype
            timestamp_columns = [col_name for col_name, col_type in clustered.dtypes if col_type == "timestamp"]
            for col_name in timestamp_columns:
                clustered = clustered.withColumn(col_name, col(col_name).cast(StringType()))
    
    
            #collecting max value from the current df, to use in next iteration
            try:
                maxval = clustered.agg({main_key: "max"}).collect()[0][0].strip()
            except AttributeError:
                maxval = clustered.agg({main_key: "max"}).collect()[0][0]


            #validation first part
            
            if len(masked_values) > 0: 
                if tmp == 1:
                    tmp_write_validation_count,max_df,min_df,full_column_list,write_table_column_count = validation_preparation_first(spark,integer_list,clustered_masked_new,tmp)
                    tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list,masked_write_table_column_count = validation_preparation_first(spark,masked_integer_list,clustered_masked,tmp)

                else:
                    tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list = validation_preparation_first(spark,masked_integer_list,clustered_masked,tmp,masked_max_df,masked_min_df)
                    tmp_write_validation_count,max_df,min_df,full_column_list = validation_preparation_first(spark,integer_list,clustered_masked_new,tmp,max_df,min_df)

                clustered_masked_new.unpersist()


            else:
                if tmp == 1:
                    tmp_write_validation_count,max_df,min_df,full_column_list,write_table_column_count = validation_preparation_first(spark,integer_list,clustered,tmp)
                else:
                    tmp_write_validation_count,max_df,min_df,full_column_list = validation_preparation_first(spark,integer_list,clustered,tmp,max_df,min_df)


            #unpersisting data and calculating target count
            write_validation_count += tmp_write_validation_count
            if len(masked_values) > 0:
                clustered_masked.unpersist()
                masked_write_validation_count += tmp_masked_write_validation_count

            if is_incremental_load:
                print("Exiting since incremental load in watermark/hashing done..")
                break

            #intializing variables
            query_dict = {}
            maxval_dict = {}
    
            # Sort the DataFrame in descending order based on the defined columns
            #this step is done to extract the last value of the df
            #to ensure we are extracting all other records for the last value of the main key
            sorted_df = clustered.orderBy(*column_list, ascending=False).limit(1)
            clustered.unpersist()
    

            #this is to ensure we are assigning varaibles for the max values of clustered indexes dynamically
            for elements in range(column_count-1):
                
                variable_name = f"maxval_{elements+1}"
                try:
                    temp_maxval = sorted_df.select(col(column_list[elements+1])).collect()[0][0].strip().replace("'", "\\'")
                except AttributeError:
                    temp_maxval = sorted_df.select(col(column_list[elements+1])).collect()[0][0]
                        
    
                #logics to create queries dynamically to get the remaining data of the main key
                maxval_dict[variable_name] = temp_maxval
                current_symbol = '>'
                query_var_name = f"add_query_{elements+1}"
                temp_query = composite_temp_query_func(read_rdbms_name,column_list[elements+1],current_symbol,maxval_dict[variable_name],table_type)
                new_clustered_query = composite_new_clustered_query_func(read_rdbms_name,read_table_name,main_key,symbol,maxval,table_type,read_schema_name)
                split_first_half = new_clustered_query.strip().replace(">=", "=").replace(">", "=") if elements == 0 else temp_clustered_query.split("ORDER BY")[0].strip().replace(">=", "=").replace(">", "=")
                split_second_half = f"ORDER BY {order_by_clause}"
                temp_clustered_query = split_first_half + ' ' + temp_query + ' ' + split_second_half
                query_dict[query_var_name] = temp_clustered_query + f' LIMIT {cluster_threshold}' if read_rdbms_name in ('mysql','maria','postgres','ibm') else temp_clustered_query

            

            #passing the dictionary reversed to get data dynamically
            for dict_key,dict_value in reversed(query_dict.items()):
                print("#####    Remaining Iteration   ######")
                offset_value = 0
                while True:

                    #tracker table varaiables intialization
                    read_status,write_status = 'FAIL','FAIL'
                    mask_read_status = 'FAIL' if len(masked_values) > 0 else 'NA'
                    mask_write_status = 'FAIL' if len(masked_values) > 0 else 'NA'
                    

                    #this variable is for validation
                    tmp += 1



                    try:

                        #getting remaining data and persisting
                        new_dict_value = f"{dict_value} OFFSET {offset_value}" if read_rdbms_name in ('mysql','maria','postgres','ibm') else f"{dict_value} OFFSET {offset_value} ROWS FETCH NEXT {cluster_threshold} ROWS ONLY"
                        remaining_clustered = read_func(spark,read_driver,read_url,new_dict_value,read_username,read_password)
                        remaining_clustered.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                        read_status = 'PASS'
                        
                            
                        #check if its empty, so that it means we extracted completely
                        #we can break the loop once we extarcted completely
                        if remaining_clustered.isEmpty():
                            break
                        
                        
                        #chunk table append
                        formatted_timestamp = cur_timestamp_func()
                        if target_kind == 'rdbms':
                            log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,write_db_name,cluster_threshold,"comp_clustered"
                                ,itr,new_dict_value,formatted_timestamp]
                            #write_task_log(spark, log, log_details,blob_log_details,'append')
                            chunk_json_append = write_task_log(spark, log,'append')
                            put_api(chunk_tracker_post_url,chunk_json_append)
                        else:
                            log = [task_log_num,source_id,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,target_kind,account_name+'_'+container_name+'_'+folder_structure,cluster_threshold,"comp_clustered"
                                ,itr,new_dict_value,formatted_timestamp]
                            #write_task_log(spark, log, log_details,blob_log_details,'append')
                            chunk_json_append = write_task_log(spark, log,'append')
                            put_api(chunk_tracker_post_url,chunk_json_append)
                    
    
                        #if masking available
                        if len(masked_values) > 0:
                            encrypt_udf = udf(masking_encrypt_data, StringType())
                            remaining_clustered_masked = clustered.select(*masked_values)
                            remaining_clustered_masked.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                            for masked_df_column in remaining_clustered_masked.columns:
                                remaining_clustered_masked = remaining_clustered_masked.withColumn(f"masked_{masked_df_column}",encrypt_udf(remaining_clustered_masked[masked_df_column]))
                            for mask_column in masked_values:
                                remaining_clustered = remaining_clustered.withColumn(mask_column, encrypt_udf(remaining_clustered[mask_column]))
                            mask_read_status = 'PASS'
                    
                        #write mode definition
                        mode = 'append'
                    
                        #writing to target rdbms
                        if target_kind == 'rdbms':
                            write_func(remaining_clustered,mode,write_driver,write_url,write_table_name,write_username,write_password)
                            write_status = 'PASS'
                            if len(masked_values) > 0:
                                write_func(remaining_clustered_masked,mode,write_driver,write_url,f"masked_{write_table_name}",write_username,write_password)
                                mask_write_status = 'PASS'
                        elif target_kind == 'blob':
                            write_func_blob(remaining_clustered,mode,account_name,container_name,folder_structure,write_format)
                            write_status = 'PASS'
                            if len(masked_values) > 0:
                                write_func_blob(remaining_clustered_masked,mode,account_name,container_name,masked_folder_structure,write_format)
                                mask_write_status = 'PASS'
    
    
                        #chunk table overwrite
                        formatted_timestamp = cur_timestamp_func()
                        if target_kind == 'rdbms':
                            log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                            #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                            chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                            put_api(chunk_tracker_post_url,chunk_json_overwrite)
                        else:
                            log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status}
                            #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                            chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                            put_api(chunk_tracker_post_url,chunk_json_overwrite)
                            
                        success_log[itr] = write_status
                        mask_success_log[itr] = mask_write_status
                        
                        
    
                        #iterating variables
                        offset_value += cluster_threshold
                        itr += 1
                        import time
                        time.sleep(5) if write_rdbms_name == 'ibm' else None



                    except Exception as error:
                        formatted_timestamp = cur_timestamp_func()
                        if target_kind == 'rdbms':
                            log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                            #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                            chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                            put_api(chunk_tracker_post_url,chunk_json_overwrite)
                        else:
                            log = {'end_timestamp': formatted_timestamp, 'read_status': read_status, 'write_status': write_status, 'mask_read_status': mask_read_status, 'mask_write_status': mask_write_status,'failure_reason':error}
                            #write_task_log(spark, log, log_details,blob_log_details,'overwrite')
                            chunk_json_overwrite = write_task_log(spark, log,'overwrite', chunk_json_append)
                            put_api(chunk_tracker_post_url,chunk_json_overwrite)     
                        success_log[itr] = write_status
                        mask_success_log[itr] = mask_write_status
                    
                    print(f"Remaining iteration - {itr} is done, write table count is {cluster_threshold}.... Timestamp - {formatted_timestamp}")


                    #validation first part
                    tmp_write_validation_count,max_df,min_df,full_column_list = validation_preparation_first(spark,integer_list,remaining_clustered,tmp,max_df,min_df)
                    if len(masked_values) > 0:
                        tmp_masked_write_validation_count,masked_max_df,masked_min_df,masked_full_column_list = validation_preparation_first(spark,masked_integer_list,remaining_clustered_masked,tmp,masked_max_df,masked_min_df)

                    #iterating row count and unpersisting
                    write_validation_count += tmp_write_validation_count
                    remaining_clustered.unpersist()
                    if len(masked_values) > 0:
                        remaining_clustered_masked.unpersist()
                        masked_write_validation_count += tmp_masked_write_validation_count
                    

        #validation second part
        max_result_df,min_result_df = validation_preparation_second(spark,min_df,max_df)
        masked_max_result_df, masked_min_result_df = validation_preparation_second(spark,masked_min_df,masked_max_df) if len(masked_values) > 0 else (None,None)


        #master level tracker table end
        if ('FAIL' not in success_log.values()) and ('FAIL' not in mask_success_log.values()):
            table_status = 'Completed'
        else:
            table_status = 'Failed'
        log_list = [task_log_num,read_source_name,read_rdbms_name,read_db_name,read_schema_name,read_table_name,table_status]
        #table_log(spark,log_list,log_details,"overwrite",blob_log_details)
        table_json_overwrite = table_log(spark,log_list,"overwrite", table_json_append)
        #put_api(master_tracker_post_url,table_json_overwrite)

        
    
        return write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite


############################################################################################################################################################
############################################################################################################################################################


#Importing
#from main_functions import CommonDataProcessing,SpecificMethodProcessing
from pyspark.sql.functions import split,explode
from datetime import datetime

if __name__ == "__main__":
    

    #assigning the class
    data_processor = CommonDataProcessing()
    

    
    #Import Statements
    import sys
    import ast
    #from functions import table_size_func,read_func,primary_key_func,index_query_func,validations,partition_query_func,non_cluster_decision_func


    #Input Files
    target_details = sys.argv[1]
    rawzone_db = sys.argv[2]
    maskedzone_db = sys.argv[3]
    exec_file_path = sys.argv[4]
 

    connection_id = int(sys.argv[5])
    api_endpoint = sys.argv[6]
    api_endpoint_dict = ast.literal_eval(api_endpoint)
	

    db_config_spark_url = api_endpoint_dict['config_url']
    config_spark_url = api_endpoint_dict['get_api']
    masking_confirmation_url = api_endpoint_dict['masking_confirmation_url']
    masked_columns_url = api_endpoint_dict['get_api']
    master_tracker_get_url = api_endpoint_dict['master_tracker_get_url']
    master_tracker_post_url = api_endpoint_dict['master_tracker_post_url']
    chunk_tracker_post_url = api_endpoint_dict['chunk_tracker_post_url']
    validation_url = api_endpoint_dict['validation_url']
  

    #starting spark session and input dictionary return
    spark,db_dict,table_dict,target_dict,is_masking,masked_columns_dict,cluster_threshold_mb,source_id,master_tracker_get_url = data_processor.logging_mechanism_function(config_spark_url,db_config_spark_url,target_details,masking_confirmation_url,masked_columns_url,master_tracker_get_url,exec_file_path,connection_id)

    #variables setting
    validation_dict = {}
    unsuccesful_extraction = {}


    #Loop for data extraction for all tables in the input list
    for key in table_dict:
         

        current_timestamp = datetime.now()
        formatted_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        #logs
        print('-------------------------------------------------------------------------------------------------------------------------------------------------------')
        
        #source parameters
        read_source_name = table_dict[key]['sources']
        read_rdbms_name = table_dict[key]['db_types']
        read_table_name = table_dict[key]['tables']
        read_db_name = table_dict[key]['databases']
        source_connection_details = db_dict[read_rdbms_name]
        read_url = source_connection_details['jdbc_url'] + read_db_name if read_rdbms_name != 'mssql' else source_connection_details['jdbc_url'].rstrip('/') + f';databaseName={read_db_name};encrypt=true;trustServerCertificate=true'
        read_driver = source_connection_details['db_driver']
        read_username = source_connection_details['user_name']
        read_password = source_connection_details['password']
        read_schema_name = table_dict[key]['schemas']
    
        #target parameters
        target_kind = next(iter(target_dict)).lower()

        #rdbms related inputs
        write_format = target_dict[target_kind]['write_format']
        target_info = target_dict[target_kind]['rdbms_type'] 
        write_rdbms_name = target_info
        write_db_name = target_dict[target_kind]['database_name']
        write_table_name = read_table_name
        if target_kind == 'rdbms':
            write_url = target_dict[target_kind]['db_url'] + write_db_name if write_rdbms_name != 'mssql' else target_dict[target_kind]['db_url'].rstrip('/') + f';databaseName={write_db_name};encrypt=true;trustServerCertificate=true'
        else:
            write_url = target_dict[target_kind]['db_url']
        write_driver = target_dict[target_kind]['db_driver']
        write_username = target_dict[target_kind]['db_user']
        write_password = target_dict[target_kind]['db_password']
        write_schema_name = target_dict[target_kind]['schema_name']
   
    
        #blob related inputs
        account_name = target_dict[target_kind]['account_name']
        container_name = target_dict[target_kind]['container_name']
        access_key = target_dict[target_kind]['access_key']
        folder_structure = target_dict[target_kind]['folder_structure'] + write_table_name + '/' if target_kind == 'blob' else target_dict[target_kind]['folder_structure']
        masked_folder_structure = target_dict[target_kind]['masked_folder_structure'] + write_table_name + '/' if target_kind == 'blob' else target_dict[target_kind]['folder_structure']

        #if masking available
        if is_masking:
            masked_keys = [mask_key for mask_key in masked_columns_dict.keys() if mask_key.startswith(key)]
            masked_values = [masked_columns_dict[mask_key]['columns'] for mask_key in masked_keys]
            print(masked_values)
            if len(masked_values) > 0:
                print(f'Masking Included For this table - {read_table_name}')

        else:
            masked_values = []
        

        #logs
        (print(f'####Source Details####\nRDBMS Type: {read_rdbms_name}\nDatabase Name: {read_db_name}\nTable Name: {read_table_name}\n\n####Target Details####\nRDBMS Type: {write_rdbms_name}\nDatabase Name: {write_db_name}\nTable Name: {write_table_name}')
         if target_kind == 'rdbms'
        else
        print(f'####Source Details####\nRDBMS Type: {read_rdbms_name}\nDatabase Name: {read_db_name}\nTable Name: {read_table_name}\n\n####Target Details####\nBLOB Account: {account_name}\nContainer Name: {container_name}\nFolder Structure: {folder_structure}'))
    
    
        #setting the container access key if target is blob
        spark.conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", access_key) if target_kind == 'blob' else None
        
    
        try:

            print('\n******** Data Extraction Started *********')
        
            #extracting table metadata count or original count
            table_count = data_processor.count_extraction_function(spark,read_rdbms_name,read_table_name,read_db_name,read_driver,read_url,read_username,read_password) if read_rdbms_name not in ('ibm','mssql','postgres') else data_processor.count_extraction_function(spark,read_rdbms_name,read_table_name,read_db_name,read_driver,read_url,read_username,read_password,read_schema_name)
            
            #extracting average row size for the table
            avg_row_size_query = table_size_func(read_rdbms_name,read_db_name,read_table_name) if read_rdbms_name not in ('ibm','mssql','postgres') else table_size_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name)
            table_row_size_mb_df = read_func(spark,read_driver,read_url,avg_row_size_query,read_username,read_password)
            if table_row_size_mb_df.isEmpty():
                table_row_size_mb = 0.1
            else:
                table_row_size_mb = table_row_size_mb_df.rdd.collect()[0][0]
            
            #Decision for chunks/full load
            if table_count < 1:
                print('use select full table approach, not chunks.. it is hardcoded for less than 1 now, it needs to be configured accordingly')

            else:
                avg_row_size_mb = table_row_size_mb/table_count
                
                #for some small tables the avg_row_size is returning as zero, so this is handled temporavarily here
                if avg_row_size_mb == 0:
                    avg_row_size_mb = 0.00000001
                    
                cluster_threshold = round((cluster_threshold_mb/avg_row_size_mb))
                print("Table_count", table_count)
                print("Table_size_mb", table_row_size_mb)
                print("avg_row_size_mb", avg_row_size_mb)
                print("Cluster_threshold_mb", cluster_threshold_mb)
                print("Chunk_Size", cluster_threshold)
                #cluster_threshold = 2000


                #general validation steps
                def validation_table_creation_func():
                    validation_key, validation_value = validations(read_rdbms_name, read_db_name, read_table_name, table_count,
                                                                   write_validation_count, max_result_df, min_result_df,
                                                                   table_column_count, write_table_column_count, spark,
                                                                   full_column_list, read_driver, read_url, read_username,
                                                                   read_password, masked_values, masked_integer_list,
                                                                   masked_write_validation_count, masked_write_table_column_count,
                                                                   masked_full_column_list, masked_min_result_df,
                                                                   masked_max_result_df,read_schema_name,source_id,validation_url)
                    validation_dict[validation_key] = validation_value

                    print(f'Validation Result - {validation_value}') 
                
                    if (validation_value in ('Success', 'Partial Success')) and target_kind == 'blob':
                        spark.sql(f"""
                            DROP TABLE IF EXISTS {rawzone_db}.{read_table_name}
                        """)
                        spark.sql(f"""
                            CREATE TABLE IF NOT EXISTS {rawzone_db}.{read_table_name}
                            USING DELTA
                            LOCATION 'abfss://{container_name}@{account_name}.dfs.core.windows.net/{folder_structure}'
                        """)
                
                    if (validation_value in ('Success', 'Partial Success')) and target_kind == 'blob' and len(masked_values) > 0:
                        spark.sql(f"""
                            DROP TABLE IF EXISTS {maskedzone_db}.{read_table_name}_masked
                        """)
                        spark.sql(f"""
                            CREATE TABLE IF NOT EXISTS {maskedzone_db}.{read_table_name}_masked
                            USING DELTA
                            LOCATION 'abfss://{container_name}@{account_name}.dfs.core.windows.net/{masked_folder_structure}'
                        """)
                    
                    put_api(master_tracker_post_url,table_json_overwrite)
                            
        
                #As priority given to clustered index, first we are checking whether the table has primary key or not here
                prim_query = primary_key_func(read_rdbms_name,read_db_name,read_table_name) if read_rdbms_name not in ('ibm','mssql') else primary_key_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name)
                prim_df = read_func(spark,read_driver,read_url,prim_query,read_username,read_password)
                
                #For mssql multi clustered below steps will be needed
                if read_rdbms_name == 'mssql':
                    prim_df = prim_df.orderBy("ORDINAL_POSITION").select('COLUMN_NAME')
                    

                #To check if table has partitions
                partition=True
                partition_query=partition_query_func(read_rdbms_name,read_db_name,read_schema_name,read_table_name)
                partition_df=read_func(spark,read_driver,read_url,partition_query,read_username,read_password)
                
                
                #for some rdbms, even indexing column is showing as partition so to handle it below steps are needed
                part_yes = len(partition_df.collect())
                if partition_df.isEmpty() or part_yes == 1:
                    partition=False


                #intializing the method class
                method_processor = SpecificMethodProcessing(spark, read_rdbms_name, write_rdbms_name, read_table_name, write_table_name, read_db_name, 
                                                            write_db_name, read_driver, write_driver, read_url, write_url, read_username, write_username, 
                                                            read_password, write_password, cluster_threshold, read_schema_name, account_name, container_name, 
                                                            folder_structure, write_format, target_kind, masked_values, masked_folder_structure,table_count,read_source_name,source_id,master_tracker_get_url,master_tracker_post_url,chunk_tracker_post_url)
                    
        
                #if primary key not there in table
                if prim_df.isEmpty():
        
                    #we are checking for non clustered index possibility
                    idx_query = index_query_func(read_rdbms_name,read_db_name,read_table_name) if read_rdbms_name not in ('ibm','mssql','postgres') else index_query_func(read_rdbms_name,read_db_name,read_table_name,read_schema_name)
                    index_df = read_func(spark,read_driver,read_url,idx_query,read_username,read_password)


                    #error handling for ibm db2
                    if read_rdbms_name == 'ibm':
                        split_df = index_df.withColumn("split_values", split(index_df.INDEXED_COLUMN, "\\+"))
                        index_df = split_df.select(explode(split_df.split_values).alias("INDEXED_COLUMN"))
   
        
                    #if both clustered and non clustered index not present
                    if index_df.isEmpty():
                        
                        #partition approach
                        if partition and read_rdbms_name != 'ibm':
                            write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite = method_processor.partition_func()
                            validation_table_creation_func()
                            
    
                        #if no indexing & partitioning available for the table
                        else:
                            write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite = method_processor.limit_offset_func()
                            validation_table_creation_func()


                            
                    #if non-clustered index available
                    else:
                        index_column_list = index_df.distinct().rdd.flatMap(lambda x:x).collect()
                        
                        #based on the ditinct value count, either non clustered approach or non clustered limit offset approached will be selected
                        non_cluster_decision = non_cluster_decision_func(read_rdbms_name,read_schema_name,read_table_name,table_count,index_column_list[0],spark,read_driver,read_url,read_username,read_password,cluster_threshold)
                        if non_cluster_decision == 'yes':
                            write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite = method_processor.non_clustered_function(index_column_list)
                        else:
                            write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite = method_processor.non_cluster_limit_offset_func(index_column_list)
                        validation_table_creation_func()
                
                
                #if clustered index available
                else:
                    prim_list = prim_df.rdd.flatMap(lambda x:x).collect()
                    prim_key_list = []

                    #To handle duplicates(in some cases primary keys duplicates are coming, when we perform distinct order is changing so handled like this)
                    for prim_key_item in prim_list:
                        if prim_key_item not in prim_key_list:
                            prim_key_list.append(prim_key_item)

                    #single primary key approach
                    if len(prim_key_list) == 1:
                        prim_key = prim_key_list[0]
                        write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite = method_processor.clustered_index_function(prim_key)
                        validation_table_creation_func()
    
                    
                    #composite primary key approach
                    else:
                        write_validation_count,table_column_count,write_table_column_count,full_column_list,min_result_df,max_result_df,masked_integer_list,masked_write_validation_count,masked_write_table_column_count,masked_full_column_list,masked_min_result_df,masked_max_result_df,table_json_overwrite = method_processor.composite_clustered_function(prim_key_list)
                        validation_table_creation_func()
                        

            #to derive time for every table extraction
            time_difference = datetime.now() - current_timestamp
            total_seconds = time_difference.total_seconds()

            # Calculate hours, minutes, and seconds
            hours = int(total_seconds // 3600)
            remaining_seconds = total_seconds % 3600
            minutes = int(remaining_seconds // 60)
            seconds = int(remaining_seconds % 60)
            
            # Print total hours, minutes, and seconds
            print(f'Time taken for data extraction and validation is  {hours}h:{minutes}m:{seconds}s')
                        

        #unsuccesful extraction will be caught in this dictionary with errors
        except Exception as e:
            unsuccesful_extraction[read_table_name] = e
            #raise e


    
    #Printing validation results at last for all tables
    print("Succesful Validation")
    for valid_key,valid_value in validation_dict.items():
        if valid_value == 'Success' or valid_value == 'Partial Success':
            print(valid_key)
    print("Failed Validation")
    for valid_key,valid_value in validation_dict.items():
        if valid_value == 'Failed':
            print(valid_key)
    print("Unsucessful Extraction")
    for unsc_key, unsc_valid in unsuccesful_extraction.items():
        print(unsc_key)