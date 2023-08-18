from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from datetime import datetime
from pathlib import Path
import argparse
from time import time

from tpcds_schema import *
# import all queries in __init__.py in queries folder
from queries import *


def conf_setup():
    conf = SparkConf()
    conf.setAppName("tpcds")
    conf.set("spark.driver.memory", "8g")
    spark_session = SparkSession.builder.config(conf=conf).getOrCreate()

    spark_context = spark_session.sparkContext

    return spark_session, spark_context


def read_table(spark_session, spark_context):
    call_center_split = spark_context.textFile(data_path + "/call_center.dat").map(lambda l: l.split('|'))
    call_center = call_center_split.map(lambda l: [int(l[0].strip() or 0),
                                                   str(l[1]),
                                                   datetime.strptime(l[2].strip() or '1970-01-01', '%Y-%m-%d'),
                                                   datetime.strptime(l[3].strip() or '1970-01-01', '%Y-%m-%d'),
                                                   int(l[4].strip() or 0),
                                                   int(l[5].strip() or 0),
                                                   str(l[6]),
                                                   str(l[7]),
                                                   int(l[8].strip() or 0),
                                                   int(l[9].strip() or 0),
                                                   str(l[10]),
                                                   str(l[11]),
                                                   int(l[12].strip() or 0),
                                                   str(l[13]),
                                                   str(l[14]),
                                                   str(l[15]),
                                                   int(l[16].strip() or 0),
                                                   str(l[17]),
                                                   int(l[18].strip() or 0),
                                                   str(l[19]),
                                                   str(l[20]),
                                                   str(l[21]),
                                                   str(l[22]),
                                                   str(l[23]),
                                                   str(l[24]),
                                                   str(l[25]),
                                                   str(l[26]),
                                                   str(l[27]),
                                                   str(l[28]),
                                                   float(l[29].strip() or 0),
                                                   float(l[30].strip() or 0)])
    df_call_center = spark_session.createDataFrame(call_center, call_center_schema)
    df_call_center.registerTempTable("call_center")
    df_call_center.cache().count()

    catalog_page_split = spark_context.textFile(data_path+"/catalog_page.dat").map(lambda l: l.split('|'))
    catalog_page = catalog_page_split.map(lambda l: [int(l[0].strip() or 0),
                                                     str(l[1]),
                                                     int(l[2].strip() or 0),
                                                     int(l[3].strip() or 0),
                                                     str(l[4]),
                                                     int(l[5].strip() or 0),
                                                     int(l[6].strip() or 0),
                                                     str(l[7].strip() or 0),
                                                     str(l[8])])
    df_catalog_page = spark_session.createDataFrame(catalog_page, catalog_page_schema)
    df_catalog_page.registerTempTable("catalog_page")
    df_catalog_page.cache().count()

    catalog_returns_split = spark_context.textFile(data_path + "/catalog_returns.dat").map(lambda l: l.split('|'))
    catalog_returns = catalog_returns_split.map(lambda l: [int(l[0].strip() or 0),
                                                           int(l[1].strip() or 0),
                                                           int(l[2].strip() or 0),
                                                           int(l[3].strip() or 0),
                                                           int(l[4].strip() or 0),
                                                           int(l[5].strip() or 0),
                                                           int(l[6].strip() or 0),
                                                           int(l[7].strip() or 0),
                                                           int(l[8].strip() or 0),
                                                           int(l[9].strip() or 0),
                                                           int(l[10].strip() or 0),
                                                           int(l[11].strip() or 0),
                                                           int(l[12].strip() or 0),
                                                           int(l[13].strip() or 0),
                                                           int(l[14].strip() or 0),
                                                           int(l[15].strip() or 0),
                                                           int(l[16].strip() or 0),
                                                           int(l[17].strip() or 0),
                                                           float(l[18].strip() or 0.0),
                                                           float(l[19].strip() or 0.0),
                                                           float(l[20].strip() or 0.0),
                                                           float(l[21].strip() or 0.0),
                                                           float(l[22].strip() or 0.0),
                                                           float(l[23].strip() or 0.0),
                                                           float(l[24].strip() or 0.0),
                                                           float(l[25].strip() or 0.0),
                                                           float(l[26].strip() or 0.0)])
    df_catalog_returns = spark_session.createDataFrame(catalog_returns, catalog_returns_schema)
    df_catalog_returns.registerTempTable("catalog_returns")
    df_catalog_returns.cache().count()

    catalog_sales_split = spark_context.textFile(data_path + "/catalog_sales.dat").map(lambda l: l.split('|'))
    catalog_sales = catalog_sales_split.map(lambda l: [int(l[0].strip() or 0),
                                                       int(l[1].strip() or 0),
                                                       int(l[2].strip() or 0),
                                                       int(l[3].strip() or 0),
                                                       int(l[4].strip() or 0),
                                                       int(l[5].strip() or 0),
                                                       int(l[6].strip() or 0),
                                                       int(l[7].strip() or 0),
                                                       int(l[8].strip() or 0),
                                                       int(l[9].strip() or 0),
                                                       int(l[10].strip() or 0),
                                                       int(l[11].strip() or 0),
                                                       int(l[12].strip() or 0),
                                                       int(l[13].strip() or 0),
                                                       int(l[14].strip() or 0),
                                                       int(l[15].strip() or 0),
                                                       int(l[16].strip() or 0),
                                                       int(l[17].strip() or 0),
                                                       int(l[18].strip() or 0),
                                                       float(l[19].strip() or 0.0),
                                                       float(l[20].strip() or 0.0),
                                                       float(l[21].strip() or 0.0),
                                                       float(l[22].strip() or 0.0),
                                                       float(l[23].strip() or 0.0),
                                                       float(l[24].strip() or 0.0),
                                                       float(l[25].strip() or 0.0),
                                                       float(l[26].strip() or 0.0),
                                                       float(l[27].strip() or 0.0),
                                                       float(l[28].strip() or 0.0),
                                                       float(l[29].strip() or 0.0),
                                                       float(l[30].strip() or 0.0),
                                                       float(l[31].strip() or 0.0),
                                                       float(l[32].strip() or 0.0),
                                                       float(l[33].strip() or 0.0)])
    df_catalog_sales = spark_session.createDataFrame(catalog_sales, catalog_sales_schema)
    df_catalog_sales.registerTempTable("catalog_sales")
    df_catalog_sales.cache().count()

    customer_split = spark_context.textFile(data_path + "/customer.dat").map(lambda l: l.split('|'))
    customer = customer_split.map(lambda l: [int(l[0].strip() or 0),
                                             str(l[1]),
                                             int(l[2].strip() or 0),
                                             int(l[3].strip() or 0),
                                             int(l[4].strip() or 0),
                                             int(l[5].strip() or 0),
                                             int(l[6].strip() or 0),
                                             str(l[7]),
                                             str(l[8]),
                                             str(l[9]),
                                             str(l[10]),
                                             int(l[11].strip() or 0),
                                             int(l[12].strip() or 0),
                                             int(l[13].strip() or 0),
                                             str(l[14]),
                                             str(l[15]),
                                             str(l[16]),
                                             str(l[17])])
    df_customer = spark_session.createDataFrame(customer, customer_schema)
    df_customer.registerTempTable("customer")
    df_customer.cache().count()

    customer_address_split = spark_context.textFile(data_path + "/customer_address.dat").map(lambda l: l.split('|'))
    customer_address = customer_address_split.map(lambda l: [int(l[0].strip() or 0),
                                                             str(l[1]),
                                                             str(l[2]),
                                                             str(l[3]),
                                                             str(l[4]),
                                                             str(l[5]),
                                                             str(l[6]),
                                                             str(l[7]),
                                                             str(l[8]),
                                                             str(l[9]),
                                                             str(l[10]),
                                                             float(l[11].strip() or 0.0),
                                                             str(l[12])])
    df_customer_address = spark_session.createDataFrame(customer_address, customer_address_schema)
    df_customer_address.registerTempTable("customer_address")
    df_customer_address.cache().count()

    customer_demographics_split = spark_context.textFile(data_path + "/customer_demographics.dat").map(lambda l: l.split('|'))
    customer_demographics = customer_demographics_split.map(lambda l: [int(l[0].strip() or 0),
                                                                       str(l[1]),
                                                                       str(l[2]),
                                                                       str(l[3]),
                                                                       int(l[4].strip() or 0),
                                                                       str(l[5]),
                                                                       int(l[6].strip() or 0),
                                                                       int(l[7].strip() or 0),
                                                                       int(l[8].strip() or 0)])
    df_customer_demographics = spark_session.createDataFrame(customer_demographics, customer_demographics_schema)
    df_customer_demographics.registerTempTable("customer_demographics")
    df_customer_demographics.cache().count()

    date_dim_split = spark_context.textFile(data_path + "/date_dim.dat").map(lambda l: l.split('|'))
    date_dim = date_dim_split.map(lambda l: [int(l[0].strip() or 0),
                                             str(l[1]),
                                             datetime.strptime(l[2].strip() or '1970-01-01', '%Y-%m-%d'),
                                             int(l[3].strip() or 0),
                                             int(l[4].strip() or 0),
                                             int(l[5].strip() or 0),
                                             int(l[6].strip() or 0),
                                             int(l[7].strip() or 0),
                                             int(l[8].strip() or 0),
                                             int(l[9].strip() or 0),
                                             int(l[10].strip() or 0),
                                             int(l[11].strip() or 0),
                                             int(l[12].strip() or 0),
                                             int(l[13].strip() or 0),
                                             str(l[14]),
                                             str(l[15]),
                                             str(l[16]),
                                             str(l[17]),
                                             str(l[18]),
                                             int(l[19].strip() or 0),
                                             int(l[20].strip() or 0),
                                             int(l[21].strip() or 0),
                                             int(l[22].strip() or 0),
                                             str(l[23]),
                                             str(l[24]),
                                             str(l[25]),
                                             str(l[26]),
                                             str(l[27])])
    df_date_dim = spark_session.createDataFrame(date_dim, date_dim_schema)
    df_date_dim.registerTempTable("date_dim")
    df_date_dim.cache().count()

    dbgen_version_split = spark_context.textFile(data_path + "/dbgen_version.dat").map(lambda l: l.split('|'))
    dbgen_version = dbgen_version_split.map(lambda l: [str(l[0]),
                                                       datetime.strptime(l[1].strip() or '1970-01-01', '%Y-%m-%d'),
                                                       str(l[2]),
                                                       str(l[3])])
    df_dbgen_version = spark_session.createDataFrame(dbgen_version, dbgen_version_schema)
    df_dbgen_version.registerTempTable("dbgen_version")
    df_dbgen_version.cache().count()

    household_demographics_split = spark_context.textFile(data_path + "/household_demographics.dat").map(lambda l: l.split('|'))
    household_demographics = household_demographics_split.map(lambda l: [int(l[0].strip() or 0),
                                                                         int(l[1].strip() or 0),
                                                                         str(l[2]),
                                                                         int(l[3].strip() or 0),
                                                                         int(l[4].strip() or 0)])
    df_household_demographics = spark_session.createDataFrame(household_demographics, household_demographics_schema)
    df_household_demographics.registerTempTable("household_demographics")
    df_household_demographics.cache().count()

    income_band_split = spark_context.textFile(data_path + "/income_band.dat").map(lambda l: l.split('|'))
    income_band = income_band_split.map(lambda l: [int(l[0].strip() or 0),
                                                   int(l[1].strip() or 0),
                                                   int(l[2].strip() or 0)])
    df_income_band = spark_session.createDataFrame(income_band, income_band_schema)
    df_income_band.registerTempTable("income_band")
    df_income_band.cache().count()

    inventory_split = spark_context.textFile(data_path + "/inventory.dat").map(lambda l: l.split('|'))
    inventory = inventory_split.map(lambda l: [int(l[0].strip() or 0),
                                               int(l[1].strip() or 0),
                                               int(l[2].strip() or 0),
                                               int(l[3].strip() or 0)])
    df_inventory = spark_session.createDataFrame(inventory, inventory_schema)
    df_inventory.registerTempTable("inventory")
    df_inventory.cache().count()

    item_split = spark_context.textFile(data_path + "/item.dat").map(lambda l: l.split('|'))
    item = item_split.map(lambda l: [int(l[0].strip() or 0),
                                     str(l[1]),
                                     datetime.strptime(l[2].strip() or '1970-01-01', '%Y-%m-%d'),
                                     datetime.strptime(l[3].strip() or '1970-01-01', '%Y-%m-%d'),
                                     str(l[4]),
                                     float(l[5].strip() or 0.0),
                                     float(l[6].strip() or 0.0),
                                     int(l[7].strip() or 0),
                                     str(l[8]),
                                     int(l[9].strip() or 0),
                                     str(l[10]),
                                     int(l[11].strip() or 0),
                                     str(l[12]),
                                     int(l[13].strip() or 0),
                                     str(l[14]),
                                     str(l[15]),
                                     str(l[16]),
                                     str(l[17]),
                                     str(l[18]),
                                     str(l[19]),
                                     int(l[20].strip() or 0),
                                     str(l[21])])
    df_item = spark_session.createDataFrame(item, item_schema)
    df_item.registerTempTable("item")
    df_item.cache().count()

    promotion_split = spark_context.textFile(data_path + "/promotion.dat").map(lambda l: l.split('|'))
    promotion = promotion_split.map(lambda l: [int(l[0].strip() or 0),
                                               str(l[1]),
                                               int(l[2].strip() or 0),
                                               int(l[3].strip() or 0),
                                               int(l[4].strip() or 0),
                                               float(l[5].strip() or 0.0),
                                               int(l[6].strip() or 0),
                                               str(l[7]),
                                               str(l[8]),
                                               str(l[9]),
                                               str(l[10]),
                                               str(l[11]),
                                               str(l[12]),
                                               str(l[13]),
                                               str(l[14]),
                                               str(l[15]),
                                               str(l[16]),
                                               str(l[17]),
                                               str(l[18])])
    df_promotion = spark_session.createDataFrame(promotion, promotion_schema)
    df_promotion.registerTempTable("promotion")
    df_promotion.cache().count()

    reason_split = spark_context.textFile(data_path + "/reason.dat").map(lambda l: l.split('|'))
    reason = reason_split.map(lambda l: [int(l[0].strip() or 0),
                                         str(l[1]),
                                         str(l[2])])
    df_reason = spark_session.createDataFrame(reason, reason_schema)
    df_reason.registerTempTable("reason")
    df_reason.cache().count()

    ship_mode_split = spark_context.textFile(data_path + "/ship_mode.dat").map(lambda l: l.split('|'))
    ship_mode = ship_mode_split.map(lambda l: [int(l[0].strip() or 0),
                                               str(l[1]),
                                               str(l[2]),
                                               str(l[3]),
                                               str(l[4]),
                                               str(l[5])])
    df_ship_mode = spark_session.createDataFrame(ship_mode, ship_mode_schema)
    df_ship_mode.registerTempTable("ship_mode")
    df_ship_mode.cache().count()

    store_split = spark_context.textFile(data_path + "/store.dat").map(lambda l: l.split('|'))
    store = store_split.map(lambda l: [int(l[0].strip() or 0),
                                       str(l[1]),
                                       datetime.strptime(l[2].strip() or '1970-01-01', '%Y-%m-%d'),
                                       datetime.strptime(l[3].strip() or '1970-01-01', '%Y-%m-%d'),
                                       int(l[4].strip() or 0),
                                       str(l[5]),
                                       int(l[6].strip() or 0),
                                       int(l[7].strip() or 0),
                                       str(l[8]),
                                       str(l[9]),
                                       int(l[10].strip() or 0),
                                       str(l[11]),
                                       str(l[12]),
                                       str(l[13]),
                                       int(l[14].strip() or 0),
                                       str(l[15]),
                                       int(l[16].strip() or 0),
                                       str(l[17]),
                                       str(l[18]),
                                       str(l[19]),
                                       str(l[20]),
                                       str(l[21]),
                                       str(l[22]),
                                       str(l[23]),
                                       str(l[24]),
                                       str(l[25]),
                                       str(l[26]),
                                       float(l[27].strip() or 0.0),
                                       float(l[28].strip() or 0.0)])
    df_store = spark_session.createDataFrame(store, store_schema)
    df_store.registerTempTable("store")
    df_store.cache().count()

    store_returns_split = spark_context.textFile(data_path + "/store_returns.dat").map(lambda l: l.split('|'))
    store_returns = store_returns_split.map(lambda l: [int(l[0].strip() or 0),
                                                       int(l[1].strip() or 0),
                                                       int(l[2].strip() or 0),
                                                       int(l[3].strip() or 0),
                                                       int(l[4].strip() or 0),
                                                       int(l[5].strip() or 0),
                                                       int(l[6].strip() or 0),
                                                       int(l[7].strip() or 0),
                                                       int(l[8].strip() or 0),
                                                       int(l[9].strip() or 0),
                                                       int(l[10].strip() or 0),
                                                       float(l[11].strip() or 0.0),
                                                       float(l[12].strip() or 0.0),
                                                       float(l[13].strip() or 0.0),
                                                       float(l[14].strip() or 0.0),
                                                       float(l[15].strip() or 0.0),
                                                       float(l[16].strip() or 0.0),
                                                       float(l[17].strip() or 0.0),
                                                       float(l[18].strip() or 0.0),
                                                       float(l[19].strip() or 0.0)])
    df_store_returns = spark_session.createDataFrame(store_returns, store_returns_schema)
    df_store_returns.registerTempTable("store_returns")
    df_store_returns.cache().count()

    store_sales_split = spark_context.textFile(data_path + "/store_sales.dat").map(lambda l: l.split('|'))
    store_sales = store_sales_split.map(lambda l: [int(l[0].strip() or 0),
                                                   int(l[1].strip() or 0),
                                                   int(l[2].strip() or 0),
                                                   int(l[3].strip() or 0),
                                                   int(l[4].strip() or 0),
                                                   int(l[5].strip() or 0),
                                                   int(l[6].strip() or 0),
                                                   int(l[7].strip() or 0),
                                                   int(l[8].strip() or 0),
                                                   int(l[9].strip() or 0),
                                                   int(l[10].strip() or 0),
                                                   float(l[11].strip() or 0.0),
                                                   float(l[12].strip() or 0.0),
                                                   float(l[13].strip() or 0.0),
                                                   float(l[14].strip() or 0.0),
                                                   float(l[15].strip() or 0.0),
                                                   float(l[16].strip() or 0.0),
                                                   float(l[17].strip() or 0.0),
                                                   float(l[18].strip() or 0.0),
                                                   float(l[19].strip() or 0.0),
                                                   float(l[20].strip() or 0.0),
                                                   float(l[21].strip() or 0.0),
                                                   float(l[22].strip() or 0.0)])
    df_store_sales = spark_session.createDataFrame(store_sales, store_sales_schema)
    df_store_sales.registerTempTable("store_sales")
    df_store_sales.cache().count()

    time_dim_split = spark_context.textFile(data_path + "/time_dim.dat").map(lambda l: l.split('|'))
    time_dim = time_dim_split.map(lambda l: [int(l[0].strip() or 0),
                                             str(l[1]),
                                             int(l[2].strip() or 0),
                                             int(l[3].strip() or 0),
                                             int(l[4].strip() or 0),
                                             int(l[5].strip() or 0),
                                             str(l[6]),
                                             str(l[7]),
                                             str(l[8]),
                                             str(l[9])])
    df_time_dim = spark_session.createDataFrame(time_dim, time_dim_schema)
    df_time_dim.registerTempTable("time_dim")
    df_time_dim.cache().count()

    warehouse_split = spark_context.textFile(data_path + "/warehouse.dat").map(lambda l: l.split('|'))
    warehouse = warehouse_split.map(lambda l: [int(l[0].strip() or 0),
                                               str(l[1]),
                                               str(l[2]),
                                               int(l[3].strip() or 0),
                                               str(l[4]),
                                               str(l[5]),
                                               str(l[6]),
                                               str(l[7]),
                                               str(l[8]),
                                               str(l[9]),
                                               str(l[10]),
                                               str(l[11]),
                                               str(l[12]),
                                               float(l[13].strip() or 0.0)])
    df_warehouse = spark_session.createDataFrame(warehouse, warehouse_schema)
    df_warehouse.registerTempTable("warehouse")
    df_warehouse.cache().count()

    web_page_split = spark_context.textFile(data_path + "/web_page.dat").map(lambda l: l.split('|'))
    web_page = web_page_split.map(lambda l: [int(l[0].strip() or 0),
                                             str(l[1]),
                                             datetime.strptime(l[2].strip() or '1970-01-01', '%Y-%m-%d'),
                                             datetime.strptime(l[3].strip() or '1970-01-01', '%Y-%m-%d'),
                                             int(l[4].strip() or 0),
                                             int(l[5].strip() or 0),
                                             str(l[6]),
                                             int(l[7].strip() or 0),
                                             str(l[8]),
                                             str(l[9]),
                                             int(l[10].strip() or 0),
                                             int(l[11].strip() or 0),
                                             int(l[12].strip() or 0),
                                             int(l[13].strip() or 0)])
    df_web_page = spark_session.createDataFrame(web_page, web_page_schema)
    df_web_page.registerTempTable("web_page")
    df_web_page.cache().count()

    web_returns_split = spark_context.textFile(data_path + "/web_returns.dat").map(lambda l: l.split('|'))
    web_returns = web_returns_split.map(lambda l: [int(l[0].strip() or 0),
                                                   int(l[1].strip() or 0),
                                                   int(l[2].strip() or 0),
                                                   int(l[3].strip() or 0),
                                                   int(l[4].strip() or 0),
                                                   int(l[5].strip() or 0),
                                                   int(l[6].strip() or 0),
                                                   int(l[7].strip() or 0),
                                                   int(l[8].strip() or 0),
                                                   int(l[9].strip() or 0),
                                                   int(l[10].strip() or 0),
                                                   int(l[11].strip() or 0),
                                                   int(l[12].strip() or 0),
                                                   int(l[13].strip() or 0),
                                                   int(l[14].strip() or 0),
                                                   float(l[15].strip() or 0.0),
                                                   float(l[16].strip() or 0.0),
                                                   float(l[17].strip() or 0.0),
                                                   float(l[18].strip() or 0.0),
                                                   float(l[19].strip() or 0.0),
                                                   float(l[20].strip() or 0.0),
                                                   float(l[21].strip() or 0.0),
                                                   float(l[22].strip() or 0.0),
                                                   float(l[23].strip() or 0.0)])
    df_web_returns = spark_session.createDataFrame(web_returns, web_returns_schema)
    df_web_returns.registerTempTable("web_returns")
    df_web_returns.cache().count()

    web_sales_split = spark_context.textFile(data_path + "/web_sales.dat").map(lambda l: l.split('|'))
    web_sales = web_sales_split.map(lambda l: [int(l[0].strip() or 0),
                                               int(l[1].strip() or 0),
                                               int(l[2].strip() or 0),
                                               int(l[3].strip() or 0),
                                               int(l[4].strip() or 0),
                                               int(l[5].strip() or 0),
                                               int(l[6].strip() or 0),
                                               int(l[7].strip() or 0),
                                               int(l[8].strip() or 0),
                                               int(l[9].strip() or 0),
                                               int(l[10].strip() or 0),
                                               int(l[11].strip() or 0),
                                               int(l[12].strip() or 0),
                                               int(l[13].strip() or 0),
                                               int(l[14].strip() or 0),
                                               int(l[15].strip() or 0),
                                               int(l[16].strip() or 0),
                                               int(l[17].strip() or 0),
                                               float(l[18].strip() or 0.0),
                                               float(l[19].strip() or 0.0),
                                               float(l[20].strip() or 0.0),
                                               float(l[21].strip() or 0.0),
                                               float(l[22].strip() or 0.0),
                                               float(l[23].strip() or 0.0),
                                               float(l[24].strip() or 0.0),
                                               float(l[25].strip() or 0.0),
                                               float(l[26].strip() or 0.0),
                                               float(l[27].strip() or 0.0),
                                               float(l[28].strip() or 0.0),
                                               float(l[29].strip() or 0.0),
                                               float(l[30].strip() or 0.0),
                                               float(l[31].strip() or 0.0),
                                               float(l[32].strip() or 0.0),
                                               float(l[33].strip() or 0.0)])
    df_web_sales = spark_session.createDataFrame(web_sales, web_sales_schema)
    df_web_sales.registerTempTable("web_sales")
    df_web_sales.cache().count()

    web_site_split = spark_context.textFile(data_path + "/web_site.dat").map(lambda l: l.split('|'))
    web_site = web_site_split.map(lambda l: [int(l[0].strip() or 0),
                                             str(l[1]),
                                             datetime.strptime(l[2].strip() or '1970-01-01', '%Y-%m-%d'),
                                             datetime.strptime(l[3].strip() or '1970-01-01', '%Y-%m-%d'),
                                             str(l[4]),
                                             int(l[5].strip() or 0),
                                             int(l[6].strip() or 0),
                                             str(l[7]),
                                             str(l[8]),
                                             int(l[9].strip() or 0),
                                             str(l[10]),
                                             str(l[11]),
                                             str(l[12]),
                                             int(l[13].strip() or 0),
                                             str(l[14]),
                                             str(l[15]),
                                             str(l[16]),
                                             str(l[17]),
                                             str(l[18]),
                                             str(l[19]),
                                             str(l[20]),
                                             str(l[21]),
                                             str(l[22]),
                                             str(l[23]),
                                             float(l[24].strip() or 0.0),
                                             float(l[25].strip() or 0.0)])
    df_web_site = spark_session.createDataFrame(web_site, web_site_schema)
    df_web_site.registerTempTable("web_site")
    df_web_site.cache().count()


def run_query(query, sc, query_name, result_path, open_mode='w+'):
    result_file = Path(result_path + "/" + query_name + '.answer')

    with result_file.open(open_mode) as f:
        start = time()
        query_result = sc.sql(query)
        end = time()

        for idx, d in enumerate(query_result.collect()):
            if idx == 0:
                header = ''
                for r in d.asDict().keys():
                    header = header + r + ' | '
                f.write(header + '\n')
            row_record = ''
            for v in d.asDict().values():
                row_record = row_record + str(v) + ' | '
            f.write(row_record + '\n')

        f.write("\n{} query time: {} seconds\n".format(query_name, (end-start)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', '-q', type=str, required=True, help='indicate TPC-DS query id such as query.')
    parser.add_argument('--data_path', '-d', type=str, required=True, help='indicate the generated TPC-DS dataset')
    parser.add_argument('--results_path', '-r', type=str, required=True, help='indicate the output of TPC-H ')
    args = parser.parse_args()

    query_id = args.query
    data_path = args.data_path
    results_path = args.results_path

    spark_sess, spark_ctx = conf_setup()
    read_table(spark_sess, spark_ctx)
    exec_query = globals()[query_id].query

    if isinstance(exec_query, list):
        for idx, query in enumerate(exec_query):
            if idx == 0:
                run_query(query, spark_sess, query_id, results_path)
            else:
                run_query(query, spark_sess, query_id, results_path, 'a+')
    else:
        run_query(exec_query, spark_sess, query_id, results_path)
