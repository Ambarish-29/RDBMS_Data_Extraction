######################################################

#----------------------------------------------------


#           Prerequisistes for the code: pip install pycryptodome==3.20.0
#                                        pip install tqdm==4.66.1
#                                        pip install pandas


#----------------------------------------------------


#                Code written by: Ambarish
#                    Reviewed By:
#                       Language: Pyspark
#                        Purpose: To extract data from RDBMS in batches based on our cluster properties
#                Supported RDBMS: Mysql, Maria, Postgres, IBM DB2, Microsoft SQL server
#              Supported Targets: Azure Blob, Mysql, Maria, Postgres, IBM DB2, Microsoft SQL server



#######################################################



#Code Starts from here

#Original code with all functions and classes comprises of 3000 lines
##All the function,Classes are deleted to protect privacy
#Only main function remains

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