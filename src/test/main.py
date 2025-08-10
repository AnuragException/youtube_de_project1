# Check if local directory has already a file
# if file is there then check if the same file is present in staging area
# with status A. If so then don't delete and try to re-run
# Else give an error and not process the next file
import datetime
import os
import shutil

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_sql_transform_write
from src.main.utility.my_sql_session import *
from src.main.utility.spark_session import *
from src.main.write.parquet_writer import ParquetWriter

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]  # List Comprehension

connection = get_mysql_connection()
cursor = connection.cursor()

# total_csv_files = ["abc.csv","xyz.csv"]
# {str(total_csv_files)[1:-1] = "abc.csv","xyz.csv"

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f""" 
                 select distinct file_name from {config.db_name}.{config.table_name}
                 where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A'
                 """

    logger.info(f"Dynamically statement created :{statement}")
    cursor.execute(statement)
    data = cursor.fetchall()

    if data:
        logger.info("Your last run was failed please check")
    else:
        logger.info("No record match")

else:
    logger.info("last run was successful")

# In this area code was there to download the files from S3 to local
# in D:\data_engg_test\youtube_de_project1\S3_and_Local_Files\Files_from_S3 path,
# but we haven't setup S3 yet so we will be manually creating the files in above folder
# from next step process code will be same as original project


local_directory = config.local_directory

# Get a list of all files in local directory

all_files = os.listdir(local_directory)

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))

    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")

else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

logger.info("*******************Listing the File********************")
logger.info("List of csv files that needs to be processed %s", csv_files)

logger.info("*******************Creating spark session********************")

spark = spark_session()

logger.info("*******************Spark session created*******************")

#check the required column in the schema of csv files
#if  not required columns keep it in a list or error_files
#else union all the data into one dataframe

logger.info("************Checking Schema for data loaded in S3************")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv").option("header", "true") \
        .load(data).columns

    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns schema is  {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing column for the {data}")
        correct_files.append(data)

logger.info(f"************List of correct files******{correct_files}")
logger.info(f"************List of correct files******{error_files}")

#Move the data to error directory on local
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path,file_name)

            shutil.move(file_path,destination_path)
            logger.info(f"Moved '{file_name}' from s3 file path to '{destination_path}' ")

        else:
            logger.error(f"'{file_path} does not exist'")
else:
    logger.info("***There is no error files at our dataset********")


# Additional Columns need to be taken care of
# Determine extra columns

# Before running the process
# stage table needs to be updated with status Active(A) or Inactive(A)

logger.info("****Updating the project_staging_table that we have started the process****")
insert_statements= []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)

        statements = f"INSERT INTO {db_name}.{config.product_staging_table} " \
                     f"(file_name, file_location, created_date, status) " \
                     f"VALUES ('{filename}', '{filename}', '{formatted_date}', 'A')"


        insert_statements.append(statements)

    logger.info(f"Insert Statements are {insert_statements}")
    logger.info(f"****Connecting with MySQL server****")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("***There is no files to process ********")
    raise Exception("***No data available with correct files ********")


logger.info("************Staging table updated successfully************")

logger.info("************Fixing extra columns coming from source ************")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

# Connecting with databaseReader
# database_client = DatabaseReader(config.url,config.properties)
# logger.info("************Creating empty dataframe************")
# final_df_to_process = database_client.create_dataframe(spark,"empty_df_create_table")




# final_df_to_process = database_client.create_dataframe([],schema=schema)
# Create a new column with concatenated values of extra columns

for data in correct_files:
    final_df_to_process = spark.createDataFrame([], schema)
    data_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data)
    data_schema = data_df.columns
    extra_columns = set(data_schema) - set(config.mandatory_columns)
    logger.info(f"Extra columns present {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_column",concat_ws(",", *extra_columns))\
        .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")

        logger.info(f"processed {data} and added 'additional_column' column to the dataframe")

    else:
        data_df = data_df.withColumn("additional_column",lit(None))\
        .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")
        logger.info(f"we are not having extra columns in {data}")


    logger.info(f"************Final dataframe from source which will be going to processed************")
    final_df_to_process = final_df_to_process.union(data_df)
    # final_df_to_process.show()

# Enrich the data from all dimension tables
# also create a datamart for sales_team and their incentive, address and all
# another department for the customer who bought how much each day of the month
# for every month there should a file and inside that
# there should be store_id segregation
# Read the data from parquet and generate a csv file
# in which there will be a sales_person_name, sales_person_store_id
# sales_person_total_billing_done_for_each_month, total_incentive

# Connecting with DatabaseReader
database_client = DatabaseReader(config.url, config.properties)

# Creating df for all tables

# customer table
logger.info("************Loading customer table into customer_table_df************")
customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)


# product table
logger.info("************Loading product table into product_table_df************")
product_table_df = database_client.create_dataframe(spark,config.product_table)

# product staging table
logger.info("************Loading customer table into product_staging_table_df************")
product_staging_table_df = database_client.create_dataframe(spark,config.product_staging_table)


# sales_team table
logger.info("************Loading product table into sales_team_df************")
sales_team_df = database_client.create_dataframe(spark,config.sales_team_table)

# sales_team_df.show()

# store table
logger.info("************Loading product table into store_table_df************")
store_table_df = database_client.create_dataframe(spark,config.store_table)

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                       customer_table_df,
                                                       store_table_df,
                                                       sales_team_df)

logger.info("************Final Enriched Dataframe************")
# s3_customer_store_sales_df_join.show()

# Write the customer data into customer data mart in a parquet format
# file will be written in local first
# move the raw data to S3 bucket for reporting tool
# Write reporting data in MySQL table also

logger.info("************Writing the data into Customer Data Mart************")

final_customer_data_mart_df = s3_customer_store_sales_df_join\
                               .select("ct.customer_id","ct.first_name","ct.last_name",
                                       "ct.address","ct.pincode","phone_number","sales_date","total_cost")

logger.info("************Final data for Customer Data Mart************")
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info(f"*****Customer Data written successfully to local disk at {config.customer_data_mart_local_file}*****")

# Move data to S3 bucket for customer data mart
# this code is not implemented as we don't have S3 infrastructure yet

# sales team data mart
logger.info("************Writing the data into Sales Team Data Mart************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
                               .select("store_id","sales_person_id","sales_person_first_name",
                                       "sales_person_last_name","store_manager_name",
                                       "manager_id","is_manager","sales_person_address",
                                       "sales_person_pincode","sales_date","total_cost",
                                       expr("SUBSTRING(sales_date,1,7) as sales_month"))


logger.info("************Final data for Sales Team Data Mart************")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)
logger.info(f"*****Sales Team Data written successfully to local disk at {config.sales_team_data_mart_local_file}*****")

# Move data to S3 bucket for sales data mart
# this code is not implemented as we don't have S3 infrastructure yet

# Also writing the data into partitions
final_sales_team_data_mart_df.write.format("parquet")\
                                    .option("header", "true")\
                                    .mode("overwrite")\
                                    .partitionBy("sales_month","store_id")\
                                    .option("path",config.sales_team_data_mart_partitioned_local_file)\
                                    .save()

# The same partitioned file will be moved to S3
# this code is not implemented as we don't have S3 infrastructure yet

# Calculation for customer mart
# Finds out the customer's total purchase every month
# Write the data in MySQL table

logger.info("************Calculating customer every month purchased amount************")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("************Calculation for customer mart done and written to MySQL table************")

# Calculation for sales team mart
# Finds out total sales done by each sales person every month
# Give the top performer 1% incentive of total sales of the month
# Rest sales person with get nothing
# Write the data in MySQL table

logger.info("************Calculating sales team every month purchased amount************")
sales_mart_sql_transform_write(final_sales_team_data_mart_df)
logger.info("************Calculation for sales team mart done and written to MySQL table************")

# Last Step
# Move the files in S3 into a processed folder and delete the local files
# this code is not implemented as we don't have S3 infrastructure yet

logger.info("************Deleting sales data from local disk************")
delete_local_file(config.local_directory)
logger.info("************Deleted sales data from local disk************")

logger.info("************Deleting customer_data_mart_local_file local disk************")
delete_local_file(config.customer_data_mart_local_file)
logger.info("************Deleted customer_data_mart_local_file local disk************")

logger.info("************Deleting sales data from local disk************")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("************Deleted sales data from local disk************")

logger.info("************Deleting sales_team_data_mart_partitioned_local_file from local disk************")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("************Deleted sales_team_data_mart_partitioned_local_file from local disk************")

# Update the status of staging table
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"UPDATE {db_name}.{config.product_staging_table} " \
                     f"SET status = 'I', updated_date = '{formatted_date}' " \
                     f"WHERE file_name = '{filename}'"

        update_statements.append(statements)

    logger.info(f"Update Statements created for staging table {update_statements}")
    logger.info(f"****Connecting with MySQL server****")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info(f"****Connected with MySQL server****")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("***There is some error in processing the files ********")
    sys.exit()

input("Press Enter to continue...")