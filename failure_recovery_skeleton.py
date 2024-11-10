######################################################

#----------------------------------------------------


#           Prerequisistes for the code: pip install pycryptodome==3.20.0
#                                        pip install azure-storage-file-datalake
#                                        pip install pandas


#----------------------------------------------------


#                Code written by: Ambarish
#                    Reviewed By:
#                       Language: Pyspark
#                        Purpose: To delete the old files(if present) & rerun the failed data extraction chunks
#                Supported RDBMS: Mysql, Maria, Postgres, IBM DB2, Microsoft SQL server
#              Supported Targets: Azure Blob, Mysql, Maria, Postgres, IBM DB2, Microsoft SQL server



#######################################################



#Code Starts from here
#Original code with all functions and classes comprises of 650 lines
##All the function,Classes are deleted to protect privacy
#Only main function remains



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
                
        
            

