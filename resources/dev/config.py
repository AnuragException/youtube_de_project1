import os

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"

#AWS Access And Secret key
aws_access_key = "your_encrypted_access_key"
aws_secret_key = "your_encrypted_secret_key"
bucket_name = "youtube-project-testing"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
database_name = "pyspark_project"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "D:\\data_engg_test\\youtube_de_project1\\S3_and_Local_Files\\Files_from_S3"
customer_data_mart_local_file = "D:\\data_engg_test\\youtube_de_project1\\S3_and_Local_Files\\customer_data_mart"
sales_team_data_mart_local_file = "D:\\data_engg_test\\youtube_de_project1\\S3_and_Local_Files\\sales_team_data_mart"
sales_team_data_mart_partitioned_local_file = "D:\\data_engg_test\\youtube_de_project1\\S3_and_Local_Files\\sales_team_data_mart_partitioned"
error_folder_path_local = "D:\\data_engg_test\\youtube_de_project1\\S3_and_Local_Files\\Error_Files"

# Database tables
db_name = "pyspark_project"
table_name = "product_staging_table"