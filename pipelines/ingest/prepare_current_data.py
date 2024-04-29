import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name
from awsglue.dynamicframe import DynamicFrame
import boto3
import psycopg2
from psycopg2 import Error
import yaml
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Get parameters from config file

with open("config.yml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

p_dbname = cfg["postgre"]["dbname"]
p_port = cfg["postgre"]["port"]
p_user= cfg["postgre"]["user"]
p_host = cfg["postgre"]["host"]
p_password = cfg["postgre"]["password"]

try:

    # Connect to an existing database
    connection = psycopg2.connect(
                                    dbname = p_dbname,
                                    port = p_port,
                                    user= p_user,
                                    host = p_host,
                                    password = p_password
                                )

    # Create a cursor to perform database operations
    cursor = connection.cursor()

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
    

CUSTOMER_BUCKET_NAME = "tenant-setur"
STAGING_BUCKET_NAME = "scm-staging-prod"
STAGING_INPUT_PATH = CUSTOMER_BUCKET_NAME + "/input/"
DATABASE = "tenant-setur-db"
TABLE_NAME_PREFIX = "input_"

'''
inventory
location
passenger
price
price_location
product
product_market_price
promotion
sales
supplier
employee_count
cash_register_count
location_area
'''

inventory_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "inventory", transformation_ctx = "inventory").toDF()
location_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "location", transformation_ctx = "location").toDF()
passenger_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "passenger", transformation_ctx = "passenger").toDF()
price_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "price", transformation_ctx ="price").toDF()
price_location_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "price_location", transformation_ctx ="price_location").toDF()
product_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "product", transformation_ctx = "product").toDF()
product_market_price_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "product_market_price", transformation_ctx = "product_market_price").toDF()
promotion_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "promotion", transformation_ctx = "promotion").toDF()
sales_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "sales", transformation_ctx ="sales").toDF()
supplier_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "supplier", transformation_ctx ="supplier").toDF()
employee_count_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "employee_count", transformation_ctx ="employee_count").toDF()
cash_register_count_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "cash_register_count", transformation_ctx ="cash_register_count").toDF()
location_area_df = glueContext.create_dynamic_frame.from_catalog(database = DATABASE, table_name = TABLE_NAME_PREFIX + "location_area", transformation_ctx ="location_area").toDF()

inventory_df.createOrReplaceTempView("vw_inventory")
location_df.createOrReplaceTempView("vw_location") 
passenger_df.createOrReplaceTempView("vw_passenger")
price_df.createOrReplaceTempView("vw_price")
price_location_df.createOrReplaceTempView("vw_price_location")
product_df.createOrReplaceTempView("vw_product")
product_market_price_df.createOrReplaceTempView("vw_product_market_price")
promotion_df.createOrReplaceTempView("vw_promotion")
sales_df.createOrReplaceTempView("vw_sales")
supplier_df.createOrReplaceTempView("vw_supplier")
employee_count_df.createOrReplaceTempView("vw_employee_count")
cash_register_count_df.createOrReplaceTempView("vw_cash_register_count")
location_area_df.createOrReplaceTempView("vw_location_area")

## MASTER TABLES

# Inventory

inventory_master_df = spark.sql("""
    with inventory_current as (
        select 
            location_code
            , product_code
            , date_id
            , stock_quantity
            , stock_quantity_unit
            , load_date
            , row_number() over (partition by location_code, product_code, date_id order by load_date desc) as load_date_order
        from vw_inventory
    )
    select 
        location_code
        , product_code
        , date_id
        , stock_quantity
        , stock_quantity_unit
    from inventory_current
    where load_date_order = 1
""")

# Location

location_master_df = spark.sql("""
    with location_current as (
        select 
            location_code
            , location_desc
            , location_category_name
            , city
            , county
            , setur_region_name
            , country
            , capacity
            , channel
            , load_date
            , row_number() over (partition by location_code order by load_date desc) as load_date_order
        from vw_location
    )
    select 
        location_code
        , location_desc
        , location_category_name
        , city
        , county
        , setur_region_name
        , country
        , capacity
        , channel
    from location_current
    where load_date_order = 1
""")

# Passenger

passenger_master_df = spark.sql("""
    with passenger_current as (
        select 
            tarihid
            , setur_region_name
            , giriscikis
            , yolcusayisi
            , load_date
            , row_number() over (partition by tarihid, setur_region_name, giriscikis order by load_date desc) as load_date_order
        from vw_passenger
    )
    select 
        tarihid
        , setur_region_name
        , giriscikis
        , yolcusayisi
    from passenger_current
    where load_date_order = 1
""")

# Price

price_master_df = spark.sql("""
    with price_current as (
        select 
            product_code
            , channel
            , location_code
            , price_type
            , price
            , currency
            , start_date_id
            , end_date_id
            , update_date_id
            , price_unit
            , load_date
            , row_number() over (partition by product_code, channel, location_code, start_date_id, end_date_id, update_date_id order by load_date desc) as load_date_order
        from vw_price
    )
    select 
        product_code
        , channel
        , location_code
        , price_type
        , price
        , currency
        , start_date_id
        , end_date_id
        , update_date_id
        , price_unit
    from price_current
    where load_date_order = 1
""")

# Product

product_master_df = spark.sql("""
    with product_current as (
        select 
            product_code
            , product_desc
            , status
            , product_brand
            , product_brand_type
            , create_date_id
            , activation_date_id
            , exit_date_id
            , hierarchy_1_code
            , hierarchy_2_code
            , hierarchy_3_code
            , hierarchy_4_code
            , hierarchy_1_desc
            , hierarchy_2_desc
            , hierarchy_3_desc
            , hierarchy_4_desc
            , year
            , limit_category_code
            , limit_category_desc
            , arrival_limit
            , arrival_unit_desc
            , departure_limit
            , departure_unit_desc
            , main_measure_desc
            , main_measure_value
            , category_code
            , category_name
            , is_delist
            , product_color
            , alcohol_degree
            , is_travelkit
            , load_date
            , row_number() over (partition by product_code order by load_date desc) as load_date_order
        from vw_product
    )
    select 
        product_code
        , product_desc
        , status
        , product_brand
        , product_brand_type
        , create_date_id
        , activation_date_id
        , exit_date_id
        , hierarchy_1_code
        , hierarchy_2_code
        , hierarchy_3_code
        , hierarchy_4_code
        , hierarchy_1_desc
        , hierarchy_2_desc
        , hierarchy_3_desc
        , hierarchy_4_desc
        , year
        , limit_category_code
        , limit_category_desc
        , arrival_limit
        , arrival_unit_desc
        , departure_limit
        , departure_unit_desc
        , main_measure_desc
        , main_measure_value
        , category_code
        , category_name
        , is_delist
        , product_color
        , alcohol_degree
        , is_travelkit
    from product_current
    where load_date_order = 1
""")

# Product Market Price

product_market_price_master_df = spark.sql("""
    with product_market_price_current as (
        select 
            TarihID
            , UrunKod
            , FiyatLokasyonID
            , PiyasaTLTutar
            , load_date
            , row_number() over (partition by TarihID, UrunKod, FiyatLokasyonID order by load_date desc) as load_date_order
        from vw_product_market_price
    )
    select 
        TarihID
        , UrunKod
        , FiyatLokasyonID
        , PiyasaTLTutar
    from product_market_price_current
    where load_date_order = 1
""")

# Promotion

promotion_master_df = spark.sql("""
    with promotion_current as (
        select 
            receipt_date
            , product_code
            , promotion_id
            , promotion_type_id
            , promotion_code
            , promotion_name
            , promotion_description
            , quantity
            , discount_amount_sales
            , currency_code
            , integration_type_id
            , load_date
            , row_number() over (partition by receipt_date, product_code, promotion_id, promotion_type_id, promotion_code order by load_date desc) as load_date_order
        from vw_promotion
    )
    select 
        receipt_date
        , product_code
        , promotion_id
        , promotion_type_id
        , promotion_code
        , promotion_name
        , promotion_description
        , quantity
        , discount_amount_sales
        , currency_code
        , integration_type_id
    from promotion_current
    where load_date_order = 1
""")

# Sales

sales_master_df = spark.sql("""
    with sales_current as (
        select 
            product_code
            , location_code
            , sales_date
            , sales_quantity
            , sales_quantity_unit
            , sales_revenue
            , purchase_amount
            , currency_code
            , load_date
            , row_number() over (partition by product_code, location_code, sales_date order by load_date desc) as load_date_order
        from vw_sales
    )
    select 
        product_code
        , location_code
        , sales_date
        , sales_quantity
        , sales_quantity_unit
        , sales_revenue
        , purchase_amount
        , currency_code
    from sales_current
    where load_date_order = 1
""")

# Supplier

supplier_master_df = spark.sql("""
    with supplier_current as (
        select 
            product_id
            , supplier_code
            , supplier_name
            , leadtime
            , load_date
            , row_number() over (partition by product_id, supplier_code order by load_date desc) as load_date_order
        from vw_supplier
    )
    select 
        product_id
        , supplier_code
        , supplier_name
        , leadtime
    from supplier_current
    where load_date_order = 1
""")

# Employee Count

employee_count_master_df = spark.sql("""
    with employee_count_current as (
        select 
            Setur_Region_Name
            , AyId
            , SatisKisiSayisi
            , load_date
            , row_number() over (partition by Setur_Region_Name, AyId order by load_date desc) as load_date_order
        from vw_employee_count
    )
    select 
        Setur_Region_Name
        , AyId
        , SatisKisiSayisi
    from employee_count_current
    where load_date_order = 1
""")

# Cash Register Count

cash_register_count_master_df = spark.sql("""
    with cash_register_count_current as (
        select 
            location_code
            , count_of_cash_register
            , load_date
            , row_number() over (partition by location_code order by load_date desc) as load_date_order
        from vw_cash_register_count
    )
    select 
        location_code
        , count_of_cash_register
    from cash_register_count_current
    where load_date_order = 1
""")

# Location Area

location_area_master_df = spark.sql("""
    with location_area_current as (
        select 
            Setur_Region_Name
            , GirisCikis
            , MagazaMetrekare
            , load_date
            , row_number() over (partition by Setur_Region_Name, GirisCikis order by load_date desc) as load_date_order
        from vw_location_area
    )
    select 
        Setur_Region_Name
        , GirisCikis
        , MagazaMetrekare
    from location_area_current
    where load_date_order = 1
""")

# SAVE FILES

PREFIX_INVENTORY = STAGING_INPUT_PATH + "inventory/inventory_current/"
TARGET_INVENTORY = STAGING_INPUT_PATH + "inventory/inventory_current.parquet"
PREFIX_LOCATION = STAGING_INPUT_PATH + "location/location_current/"
TARGET_LOCATION = STAGING_INPUT_PATH + "location/location_current.parquet"
PREFIX_PASSENGER = STAGING_INPUT_PATH + "passenger/passenger_current/"
TARGET_PASSENGER = STAGING_INPUT_PATH + "passenger/passenger_current.parquet"
PREFIX_PRICE = STAGING_INPUT_PATH + "price/price_current/"
TARGET_PRICE = STAGING_INPUT_PATH + "price/price_current.parquet"
PREFIX_PRODUCT = STAGING_INPUT_PATH + "product/product_current/"
TARGET_PRODUCT = STAGING_INPUT_PATH + "product/product_current.parquet"
PREFIX_PRODUCT_MARKET_PRICE = STAGING_INPUT_PATH + "product_market_price/product_market_price_current/"
TARGET_PRODUCT_MARKET_PRICE = STAGING_INPUT_PATH + "product_market_price/product_market_price_current.parquet"
PREFIX_PROMOTION = STAGING_INPUT_PATH + "promotion/promotion_current/"
TARGET_PROMOTION = STAGING_INPUT_PATH + "promotion/promotion_current.parquet"
PREFIX_SALES = STAGING_INPUT_PATH + "sales/sales_current/"
TARGET_SALES = STAGING_INPUT_PATH + "sales/sales_current.parquet"
PREFIX_SUPPLIER = STAGING_INPUT_PATH + "supplier/supplier_current/"
TARGET_SUPPLIER = STAGING_INPUT_PATH + "supplier/supplier_current.parquet"
PREFIX_EMPLOYEE_COUNT = STAGING_INPUT_PATH + "employee_count/employee_count_current/"
TARGET_EMPLOYEE_COUNT = STAGING_INPUT_PATH + "employee_count/employee_count_current.parquet"
PREFIX_CASH_REGISTER_COUNT = STAGING_INPUT_PATH + "cash_register_count/cash_register_count_current/"
TARGET_CASH_REGISTER_COUNT = STAGING_INPUT_PATH + "cash_register_count/cash_register_count_current.parquet"
PREFIX_LOCATION_AREA = STAGING_INPUT_PATH + "location_area/location_area_current/"
TARGET_LOCATION_AREA = STAGING_INPUT_PATH + "location_area/location_area_current.parquet"

inventory_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_INVENTORY)
location_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_LOCATION)
passenger_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_PASSENGER)
price_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_PRICE)
product_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_PRODUCT)
product_market_price_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_PRODUCT_MARKET_PRICE)
promotion_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_PROMOTION)
sales_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_SALES)
supplier_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_SUPPLIER)
employee_count_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_EMPLOYEE_COUNT)
cash_register_count_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_CASH_REGISTER_COUNT)
location_area_master_df.coalesce(1).write.format("parquet").mode('overwrite').option("header", "true").save("s3a://" + STAGING_BUCKET_NAME + "/" + PREFIX_LOCATION_AREA)

# Rename Files

client = boto3.client('s3')

lst_prefix_and_targets = { 
    PREFIX_INVENTORY: TARGET_INVENTORY 
    , PREFIX_LOCATION: TARGET_LOCATION
    , PREFIX_PASSENGER: TARGET_PASSENGER
    , PREFIX_PRICE: TARGET_PRICE
    , PREFIX_PRODUCT: TARGET_PRODUCT
    , PREFIX_PRODUCT_MARKET_PRICE: TARGET_PRODUCT_MARKET_PRICE
    , PREFIX_PROMOTION: TARGET_PROMOTION
    , PREFIX_SALES: TARGET_SALES
    , PREFIX_SUPPLIER: TARGET_SUPPLIER
    , PREFIX_EMPLOYEE_COUNT: TARGET_EMPLOYEE_COUNT
    , PREFIX_CASH_REGISTER_COUNT: TARGET_CASH_REGISTER_COUNT
    , PREFIX_LOCATION_AREA: TARGET_LOCATION_AREA
}

# Iterate through items

for prefix, target in lst_prefix_and_targets.items():
    print("Processing: ", prefix, " - ", target, "...")

    response = client.list_objects(Bucket=STAGING_BUCKET_NAME, Prefix=prefix,)
    name = response["Contents"][0]["Key"]
    copy_source = {'Bucket': STAGING_BUCKET_NAME, 'Key': name}
    client.copy(CopySource=copy_source, Bucket=STAGING_BUCKET_NAME, Key=target)
    client.delete_object(Bucket=STAGING_BUCKET_NAME, Key=name)


job.commit()