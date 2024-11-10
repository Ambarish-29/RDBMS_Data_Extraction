######################################################

#----------------------------------------------------


#           Prerequisistes for the code: pip install pycryptodome==3.20.0
#                                        pip install azure-storage-file-datalake
#                                        pip install pandas


#----------------------------------------------------


#                        Product: Data Studio
#                Code written by: Ambarish
#                    Reviewed By:
#                       Language: Pyspark
#                        Purpose: To delete the old files(if present) & rerun the failed data extraction chunks
#                Supported RDBMS: Mysql, Maria, Postgres, IBM DB2, Microsoft SQL server
#              Supported Targets: Azure Blob, Mysql, Maria, Postgres, IBM DB2, Microsoft SQL server
#  Code Documentation & Pre Read: https://crayon.atlassian.net/wiki/spaces/DO/pages/3081568293/Onboarding+-+RDBMS+Data+Extraction+in+batches



#######################################################



#Code Starts from here
#custom class to get double in json which is usefull for posting data via api
class doubleQuoteDict(dict):
    def __str__(self):
        import json
        return json.dumps(self)

#python class containing all the backend api like get,put,post,patch    
class App:
    @staticmethod
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
    
    @staticmethod
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
    
    @staticmethod
    def patch_api(url, data):
        import requests
        import json        
        json_data = json.dumps(data)
        try:
            response = requests.patch(url=url, data=json_data)
            response.raise_for_status()
            return response.json()["data"]
        
        except requests.exceptions.HTTPError as e:
            raise e
        
        except Exception as e:
            raise e
    
    #Function to overwrite data to endpoint
    @staticmethod
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



# def old_files_clearing_func(account_name,account_key,container_name,folder_str,start_timestamp,end_timestamp):
#     from azure.storage.blob import BlobServiceClient
#     from datetime import datetime
#     # parquet_file_path = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/"
#     #Define your storage account and access key
#     account_name = account_name
#     account_key = account_key
    
    
#     #Initialize the BlobServiceClient
#     blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
    
#     #Define the container and directory path
#     container_name = container_name
#     directory_name = folder_str
    
#     container_client = blob_service_client.get_container_client(container_name)
    
#     #List files in the specified directory
#     files_in_directory = container_client.walk_blobs(name_starts_with=directory_name)
    
#     for file in files_in_directory:
#         if hasattr(file, 'last_modified'):
#             #Extract the last modified timestamp
#             last_modified_datatime = file.last_modified.replace(tzinfo=None) #Remove timezone information
#             last_modified = last_modified_datatime.strftime("%Y-%m-%d %H:%M:%S")
#             print(start_timestamp,last_modified,end_timestamp)

            
#             #Check if the file's last modified timestamp is within the specified range
#             if start_timestamp <= last_modified <= end_timestamp:
#                 print("1111")
#                 # parquet_file_path = parquet_file_path+file.name
#                 # df = spark.read.parquet(parquet_file_path)
#                 # print(df.count())
#                 # df_schema = df.schema
#                 # empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), df_schema)
#                 # empty_df.write
#                 container_client.delete_blob(file.name)
#                 print(f"Deleted file: {file.name} (Last Modified: {last_modified})")
                
                
                
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


#function which return formatted timestamp
def cur_timestamp_func():
    from datetime import datetime
    current_timestamp = datetime.now()
    formatted_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")
      
    return formatted_timestamp


#To write a dataframe to azure blob
def write_func_blob(dff_name,mode,account_name,container_name,folder_structure,write_format):
    dff_name.write.mode(mode).option("header", "true")\
    .format(write_format)\
    .option("delta.columnMapping.mode", "name")\
    .save(f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{folder_structure}")
    
  
#masking function using cryptpo AES algorithm  
def masking_encrypt_data(data):
    
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


#Converting masking columns and their PII statuses into dataframe
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

   
    df = spark.createDataFrame(data, full_list)
    return df


#Handling mask write status fail
def mask_write_status_fail(masked_folder_structure,mask_chunk_itr_df,chunked_df,db_config_df,api_endpoints,lst,itr,write_format,chunk_tracker_post_url):

    from datetime import datetime
    app = App()
    table_name = mask_chunk_itr_df.select('table_name').rdd.flatMap(lambda x:x).collect()[0]
    database_name = mask_chunk_itr_df.select('database_name').rdd.flatMap(lambda x:x).collect()[0]
    masked_folder_structure = masked_folder_structure + table_name + '/'
    # old_files_clearing_func(account_name,access_key,container_name,masked_folder_structure_tbl,start_timestamp,end_timestamp)

    print("Table name:", table_name, "Iteration:", itr, "- Re appending MASK Write status failed chunk")              
    #reading corresponding data
    query = mask_chunk_itr_df.select('query').rdd.flatMap(lambda x:x).collect()[0]
    source_name = mask_chunk_itr_df.select('source_name').rdd.flatMap(lambda x:x).collect()[0]
    rdbms_type = mask_chunk_itr_df.select('rdbms_type').rdd.flatMap(lambda x:x).collect()[0]
    database_name = mask_chunk_itr_df.select('database_name').rdd.flatMap(lambda x:x).collect()[0]
    schema_name = mask_chunk_itr_df.select('schema_name').rdd.flatMap(lambda x:x).collect()[0]
    config_df = db_config_df.filter(col('db_type') == rdbms_type)
    url = config_df.select('jdbc_url').collect()[0][0] + database_name if rdbms_type != 'mssql' else config_df.select('jdbc_url').collect()[0][0].rstrip('/') + f';databaseName={database_name};encrypt=true;trustServerCertificate=true'
    driver = config_df.select('db_driver').collect()[0][0]
    username = config_df.select('user_name').collect()[0][0]
    password = config_df.select('password').collect()[0][0]
    recovery_df = read_func(spark,driver,url,query,username,password,read_rdbms_name=None)


    #to get columns and their PII statuses which will be passed to api 
    sensitive_json={"client_source_details": ["db_type"],
        "is_client_included_tables": 'true',
        "table_details": ["name"],
        "column_details":["name","pii_status"]
    }
    sensitive_json_data = app.post_api(api_endpoints["get_api"],sensitive_json)
    main_key_list = ["sources","db_types","databases", "schemas","tables"]
    other_key_list = ["columns","pii_statuses"]
    mask_columns = column_masking_json_to_df(spark,sensitive_json_data,main_key_list,other_key_list)\
                         .filter((col('sources') == source_name) & (col('db_types') == rdbms_type) & (col('databases') == database_name) & (col('schemas') == schema_name) & (col('tables') == table_name) & ((lower(col('pii_statuses'))=='pii') | (lower(col('pii_statuses'))=='sensitive information')))\
                         .select('columns').rdd.flatMap(lambda x:x).collect()
    encrypt_udf = udf(masking_encrypt_data, StringType())
    masked_recovery_df = recovery_df.select(*mask_columns)
    for masked_df_column in masked_recovery_df.columns:
        masked_recovery_df = masked_recovery_df.withColumn(f"masked_{masked_df_column}",encrypt_udf(masked_recovery_df[masked_df_column]))
                        
    #writing to blob
    start_timestamp = cur_timestamp_func()
    chunked_df = chunked_df.withColumn('start_timestamp', when(((col('log_id') == lst) & (col('iteration') == itr)), start_timestamp).otherwise(col('start_timestamp')))
    write_func_blob(masked_recovery_df,'append',account_name,container_name,masked_folder_structure,write_format)
    chunked_df = chunked_df.withColumn('mask_write_status', when(((col('log_id') == lst) & (col('iteration') == itr)), 'PASS').otherwise(col('mask_write_status')))
    end_timestamp = cur_timestamp_func()
    chunked_df = chunked_df.withColumn('end_timestamp', when(((col('log_id') == lst) & (col('iteration') == itr)), end_timestamp).otherwise(col('end_timestamp')))
    chunk_json = write_task_log(spark,chunked_df,lst,itr,end_timestamp,schema_name,False,True)
    app.put_api(chunk_tracker_post_url,chunk_json)


    return chunked_df,table_name,masked_folder_structure


#Handling write status fail
def write_status_fail(is_masking,folder_structure,chunk_itr_df,chunked_df,db_config_df,api_endpoints,lst,itr,write_format,chunk_tracker_post_url):
    
    from datetime import datetime

    table_name = chunk_itr_df.select('table_name').rdd.flatMap(lambda x:x).collect()[0]
    database_name = chunk_itr_df.select('database_name').rdd.flatMap(lambda x:x).collect()[0]
    folder_structure = folder_structure + table_name + '/'
    # old_files_clearing_func(account_name,access_key,container_name,folder_structure,start_timestamp,end_timestamp)
    
    print("Table name:", table_name, "Iteration:", itr, "- Re appending Write status failed chunk")
    #reading corresponding data
    query = chunk_itr_df.select('query').rdd.flatMap(lambda x:x).collect()[0]
    rdbms_type = chunk_itr_df.select('rdbms_type').rdd.flatMap(lambda x:x).collect()[0]
    config_df = db_config_df.filter(col('db_type') == rdbms_type)
    url = config_df.select('jdbc_url').collect()[0][0] + database_name if rdbms_type != 'mssql' else config_df.select('jdbc_url').collect()[0][0].rstrip('/') + f';databaseName={database_name};encrypt=true;trustServerCertificate=true'
    driver = config_df.select('db_driver').collect()[0][0]
    username = config_df.select('user_name').collect()[0][0]
    password = config_df.select('password').collect()[0][0]
    recovery_df = read_func(spark,driver,url,query,username,password,read_rdbms_name=None)

    #if masking present
    if is_masking:
        source_name = chunk_itr_df.select('source_name').rdd.flatMap(lambda x:x).collect()[0]
        database_name = chunk_itr_df.select('database_name').rdd.flatMap(lambda x:x).collect()[0]
        table_name = chunk_itr_df.select('table_name').rdd.flatMap(lambda x:x).collect()[0]
        schema_name = chunk_itr_df.select('schema_name').rdd.flatMap(lambda x:x).collect()[0]
        
        #to get columns and their PII statuses which will be passed to api 
        sensitive_json={"client_source_details": ["db_type"],
         "is_client_included_tables": 'true',
         "table_details": ["name"],
         "column_details":["name","pii_status"]
        }
        sensitive_json_data = app.post_api(api_endpoints["get_api"],sensitive_json)
        main_key_list = ["sources","db_types","databases", "schemas","tables"]
        other_key_list = ["columns","pii_statuses"]
        mask_columns = column_masking_json_to_df(spark,sensitive_json_data,main_key_list,other_key_list)\
                         .filter((col('sources') == source_name) & (col('db_types') == rdbms_type) & (col('databases') == database_name) & (col('schemas') == schema_name) & (col('tables') == table_name) & ((lower(col('pii_statuses'))=='pii') | (lower(col('pii_statuses'))=='sensitive information')))\
                         .select('columns').rdd.flatMap(lambda x:x).collect()
        encrypt_udf = udf(masking_encrypt_data, StringType())
        
        for mask_column in mask_columns:
            recovery_df = recovery_df.withColumn(mask_column, encrypt_udf(recovery_df[mask_column]))
                        
    #writing to blob
    start_timestamp = cur_timestamp_func()
    chunked_df = chunked_df.withColumn('start_timestamp', when(((col('log_id') == lst) & (col('iteration') == itr)), start_timestamp).otherwise(col('start_timestamp')))
    write_func_blob(recovery_df,'append',account_name,container_name,folder_structure,write_format)
    chunked_df = chunked_df.withColumn('write_status', when(((col('log_id') == lst) & (col('iteration') == itr)), 'PASS').otherwise(col('write_status')))
    end_timestamp = cur_timestamp_func()
    chunked_df = chunked_df.withColumn('end_timestamp', when(((col('log_id') == lst) & (col('iteration') == itr)), end_timestamp).otherwise(col('end_timestamp')))
    chunk_json = write_task_log(spark,chunked_df,lst,itr,end_timestamp,schema_name,True,False)
    app.put_api(chunk_tracker_post_url,chunk_json)    

    return chunked_df,table_name,folder_structure

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


#master level log tracker table
def table_log(spark,master_main_df,lst,end_timestamp):
    from datetime import datetime
    import json
    
    main_json_array = dict()
    filtered_df = master_main_df.filter(col("log_id") == lst)
    main_json_array['log_id'] = filtered_df.select('log_id').rdd.flatMap(lambda x:x).collect()[0]
    main_json_array['client_source_id'] = filtered_df.select('source_id').rdd.flatMap(lambda x:x).collect()[0]
    main_json_array['source_name'] = filtered_df.select('source_name').rdd.flatMap(lambda x:x).collect()[0]
    main_json_array['rdbms_type'] = filtered_df.select('rdbms_type').rdd.flatMap(lambda x:x).collect()[0]
    main_json_array['database_name'] = filtered_df.select('database_name').rdd.flatMap(lambda x:x).collect()[0]
    main_json_array['schema_name'] = filtered_df.select('schema_name').rdd.flatMap(lambda x:x).collect()[0]
    main_json_array['table_name'] = filtered_df.select('table_name').rdd.flatMap(lambda x:x).collect()[0]
    main_json_array['load_status'] = "Completed"
    main_json_array['start_timestamp'] = filtered_df.select('start_timestamp').rdd.flatMap(lambda x:x).collect()[0]
    main_json_array['end_timestamp'] = end_timestamp

    return main_json_array

#chunk level log tracker table
def write_task_log(spark,chunked_df,lst,itr,end_timestamp,read_schema_name,write_status_change=None,masK_write_status_change=None):
    chunk_json_array = dict()
    chunk_filtered_df = chunked_df.filter((col("log_id") == lst) & (col("iteration")==itr))       
    chunk_json_array=dict()
    chunk_json_array['log_id'] = chunk_filtered_df.select('log_id').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['client_source_id'] = chunk_filtered_df.select('source_id').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['source_name'] = chunk_filtered_df.select('source_name').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['rdbms_type'] = chunk_filtered_df.select('rdbms_type').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['database_name'] = chunk_filtered_df.select('database_name').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['schema_name'] = read_schema_name
    chunk_json_array['table_name'] = chunk_filtered_df.select('table_name').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['write_detail'] = chunk_filtered_df.select('write_detail').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['write_sub_detail'] = chunk_filtered_df.select('write_sub_detail').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['chunk_size'] = chunk_filtered_df.select('chunk_size').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['extraction_approach'] = chunk_filtered_df.select('extraction_approach').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['iteration'] = chunk_filtered_df.select('iteration').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['query'] = chunk_filtered_df.select('query').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['start_timestamp'] = chunk_filtered_df.select('start_timestamp').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['read_status'] = chunk_filtered_df.select('read_status').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['mask_read_status'] = chunk_filtered_df.select('mask_read_status').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['write_status'] = "PASS" if write_status_change else chunk_filtered_df.select('write_status').rdd.flatMap(lambda x:x).collect()[0] 
    chunk_json_array['mask_write_status'] = "PASS" if masK_write_status_change else chunk_filtered_df.select('mask_write_status').rdd.flatMap(lambda x:x).collect()[0]
    chunk_json_array['end_timestamp'] = end_timestamp
    
    return chunk_json_array

 
#Importing
import pandas as pd
import sys
import ast
from pyspark.sql import SparkSession


#Reading files
target_details_path = sys.argv[1]
rawzone_db = sys.argv[2]
maskedzone_db = sys.argv[3]
source_id = int(sys.argv[4])
api_endpoint = sys.argv[5]
api_endpoints = ast.literal_eval(api_endpoint)

master_tracker_post_url = api_endpoints["master_tracker_post_url"]
chunk_tracker_post_url = api_endpoints["chunk_tracker_post_url"]

app = App()
pd.DataFrame.iteritems = pd.DataFrame.items

config_url = api_endpoints["config_url"]+f"{source_id}"
config_json = app.get_api(config_url)

config_df = config_json_to_df(config_json)

#To assign jars dynamically
for i,value in enumerate(config_df):
    if i < len(config_df) - 1:
        value = value + ';'
    jars = value.strip()


#Spark session starting
spark = SparkSession.builder.appName('failure_recovery').config("spark.driver.extraClassPath"
                    ,jars).getOrCreate()

db_config_df = config_df.drop('db_name', axis=1)
db_config_df = spark.createDataFrame(db_config_df)



#Target details Parameters
target_details = spark.read.option("header",True).csv(target_details_path)
target_row = target_details.rdd.first()
target_dict = target_row.asDict()
target_kind = target_dict['target_kind']
account_name = target_dict['account_name']
container_name = target_dict['container_name']
folder_structure = target_dict['folder_structure']
masked_folder_structure = target_dict['masked_folder_structure']
write_format = target_dict['write_format']
access_key = target_dict['access_key']


#setting the container access key in the spark session
spark.conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", access_key)



#Failure Recovery starts
if (account_name is not None) and (container_name is not None) and (access_key is not None) and ((folder_structure is not None) or (masked_folder_structure is not None)) and (write_format is not None):

    #only handled for target blob for now
    if target_kind == 'blob':
        from pyspark.sql.functions import col,when,lower,udf
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        master_tracker_get_url = api_endpoints["master_tracker_get_url"].replace("client_source_id",str(source_id))
        master_table_json = app.get_api(master_tracker_get_url)
        master_main_df = spark.createDataFrame(master_table_json)
        master_main_df.show()
        master_df = master_main_df.filter(col('load_status') == 'Failed')

        #no failed extraction jobs
        if master_df.isEmpty():
            pass
        
        #if failed extraction
        else:
            
            #getting failed log id in a list
            master_list = master_df.select('log_id').rdd.flatMap(lambda x:x).collect()

            schema2 = StructType([
                StructField("log_id", IntegerType(), True),
                StructField("source_id", IntegerType(), True),
                StructField("query", StringType(), True),
                StructField("schema_name", StringType(), True),
                StructField("database_name", StringType(), True),
                StructField("rdbms_type", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("source_name", StringType(), True),
                StructField("write_detail", StringType(), True),
                StructField("write_sub_detail", StringType(), True),
                StructField("chunk_size", StringType(), True),
                StructField("extraction_approach", StringType(), True),
                StructField("start_timestamp", StringType(), True),
                StructField("end_timestamp", StringType(), True),
                StructField("read_status", StringType(), True),
                StructField("mask_read_status", StringType(), True),
                StructField("write_status", StringType(), True),
                StructField("mask_write_status", StringType(), True),
                StructField("iteration", IntegerType(), True)
            ])

            chunk_tracker_get_url = api_endpoints["chunk_tracker_get_url"].replace("client_source_id",str(source_id))
            chunk_table_json = app.get_api(chunk_tracker_get_url)
            chunked_df = spark.createDataFrame(chunk_table_json,schema=schema2)
            #looping through the log id
            for lst in master_list:

                write_status_flag = False
                mask_write_status_flag = False
                chunk_df = chunked_df.filter((col('log_id') == lst) & (col('write_status') == 'FAIL'))
                
                #handling mask extraction failure
                if chunk_df.isEmpty():
                    #collecting iterations list for that log id
                    mask_chunk_df = chunked_df.filter((col('log_id') == lst) & (col('mask_write_status') == 'FAIL'))
                    itr_lst = mask_chunk_df.select('iteration').rdd.flatMap(lambda x:x).collect()
                    
                    #looping through iterations
                    for itr in itr_lst:
                        mask_write_status_flag = True
                        mask_chunk_itr_df = mask_chunk_df.filter(col('iteration') == itr)
                        chunked_df,table_name,tmp_msk_folder_str = mask_write_status_fail(masked_folder_structure,mask_chunk_itr_df,chunked_df,db_config_df,api_endpoints,lst,itr,write_format,chunk_tracker_post_url)     
                
                #if write_status failed
                else:
                    write_status_flag = True
                    #collecting iterations list for that log id
                    chunk_df1 = chunked_df.filter((col('log_id') == lst) & ((col('write_status') == 'FAIL') | (col('mask_write_status') == 'FAIL')))
                    itr_lst = chunk_df1.select('iteration').rdd.flatMap(lambda x:x).collect()
                    
                    #looping through iterations
                    for itr in itr_lst:
                        chunk_itr_df = chunk_df1.filter(col('iteration') == itr)
                        identify_df = chunk_itr_df.filter((col('mask_write_status') == 'FAIL') | (col('mask_write_status') == 'PASS'))
                        identify_mask_df = chunk_itr_df.filter(col('mask_write_status') == 'FAIL')
                        identify_write_status_fail_df = chunk_itr_df.filter(col('write_status') == 'FAIL')
                        
                        #if masking not applicable
                        if identify_df.isEmpty():
                            print("-------------------Masking Not applicable for this chunk------------------------------")
                            chunked_df,table_name,tmp_folder_str = write_status_fail(False,folder_structure,chunk_itr_df,chunked_df,db_config_df,api_endpoints,lst,itr,write_format,chunk_tracker_post_url)
                            
                        #if applicable
                        else:
                            
                            #if masking iteration failed
                            if identify_mask_df.isEmpty():
                                pass
                            
                            #if normal iteration failed
                            else: 
                                print("-----------------------Masking Applicable for this chunk------------------------")
                                mask_write_status_flag = True
                                chunked_df,table_name,tmp_msk_folder_str = mask_write_status_fail(masked_folder_structure,chunk_itr_df,chunked_df,db_config_df,api_endpoints,lst,itr,write_format,chunk_tracker_post_url)

                        if identify_write_status_fail_df.isEmpty():
                            pass
                        else:
                            chunked_df,table_name,tmp_folder_str = write_status_fail(True,folder_structure,chunk_itr_df,chunked_df,db_config_df,api_endpoints,lst,itr,write_format,chunk_tracker_post_url)
                
                            
                
                #check data to be re created in rawzone database
                if write_status_flag:
                    spark.sql(f"""
                                DROP TABLE IF EXISTS {rawzone_db}.{table_name}
                            """)
                    spark.sql(f"""
                            CREATE TABLE IF NOT EXISTS {rawzone_db}.{table_name}
                            USING DELTA
                            LOCATION 'abfss://{container_name}@{account_name}.dfs.core.windows.net/{tmp_folder_str}'
                            """)
                
                #check data to be re created in masked zone database
                if mask_write_status_flag:
                    spark.sql(f"""
                            DROP TABLE IF EXISTS {maskedzone_db}.{table_name}_masked
                            """)
                    spark.sql(f"""
                            CREATE TABLE IF NOT EXISTS {maskedzone_db}.{table_name}_masked
                            USING DELTA
                            LOCATION 'abfss://{container_name}@{account_name}.dfs.core.windows.net/{tmp_msk_folder_str}'
                        """)
                
                end_timestamp = cur_timestamp_func()
                master_json = table_log(spark,master_main_df,lst,end_timestamp)
                app.put_api(master_tracker_post_url,master_json)                
                
        
            

