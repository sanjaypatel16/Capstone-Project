# Lets imports all the necessary libraries

import pandas as pd
import numpy as np
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,TimestampType,VarcharType
from pyspark.sql.functions import *
import requests

#create spark session
spark = SparkSession.builder.appName('Session3').getOrCreate()


#Extract process

def extract_branch_file(json_file):
    '''takes branch json file and create spark data frame : branch_df'''
     # load json file and create spark DF
    branch_df = spark.read.json(json_file)
    return branch_df

def extract_customer_file(json_file):
    '''takes branch json file and create spark data frame : customer_df'''
     # load json file and create spark DF
    customer_df = spark.read.json(json_file)
    return customer_df

def extract_credit_file(json_file):
    '''takes branch json file and create spark data frame : credit_df'''
     # read file to create df
    credit_df = spark.read.json(json_file)
    return credit_df



# Transform Process

# function to transform branch_df dataframe
def prepare_branchdf(branch_df):
    '''Take branch_df for branch as input and return updated DF as per requirements'''

    # fill null zip column values with 99999 according to requirements
    branch_df.na.fill(99999, ['BRANCH_ZIP'])

    # rearrange columns + create branch_phone format according to requirements: USING INDEX
    rdd2 = branch_df.rdd.map(lambda x: (x[1],x[2],x[5], x[0], x[4], x[6], \
                                        '('+x[3][:3]+')' + x[3][3:6] + "-"+ x[3][6:],\
                                            x[7]))

    # rename the columns according to requirements
    df2 = rdd2.toDF([
    'BRANCH_CODE',
    'BRANCH_NAME',
    'BRANCH_STREET',
    'BRANCH_CITY',
    'BRANCH_STATE',
    'BRANCH_ZIP',
    'BRANCH_PHONE',
    'LAST_UPDATED'])
    

    # update columns type according to requirements 
    updated_branch_df = df2.withColumn("BRANCH_CODE",df2["BRANCH_CODE"].cast(IntegerType()))\
        .withColumn("BRANCH_NAME",df2["BRANCH_NAME"].cast(StringType()))\
        .withColumn("BRANCH_STREET",df2["BRANCH_STREET"].cast(StringType()))\
        .withColumn("BRANCH_CITY",df2["BRANCH_CITY"].cast(StringType()))\
        .withColumn("BRANCH_STATE",df2["BRANCH_STATE"].cast(StringType()))\
        .withColumn("BRANCH_ZIP",df2["BRANCH_ZIP"].cast(IntegerType()))\
        .withColumn("BRANCH_PHONE",df2["BRANCH_PHONE"].cast(StringType()))\
        .withColumn("LAST_UPDATED",df2["LAST_UPDATED"].cast(TimestampType()))
    
    return updated_branch_df


# function to create customer json file and return spark datafram as per requirement
def prepare_customerdf(customer_df):
    '''Prepare customer dataframe from customer_df'''
    
    # read CSV file that contains cities and phone area code
    df_cities = spark.read.csv('us-area-code-cities.csv', header=False)
    # since the CSV file have unnecessary columns, select first 2 columns that contains area code and cities name
    df_cities2 = df_cities.select(df_cities.columns[:2])
    # naming the columns
    df_cities3 = df_cities2.toDF('AREA_CODE', 'CITIES')
    # since the target DF CUST_CITY have cities name without spaces, lets remove spaces from here also to match
    df_cities3 = df_cities3.withColumn('CITIES', regexp_replace('CITIES', ' ', ''))


    # since CUST_PHONE column is int type, convert in str to be able to modify later
    customer_df = customer_df.withColumn("CUST_PHONE",customer_df["CUST_PHONE"].cast(StringType()))
    # make sure there is not city name with space, so remove spaces
    customer_df = customer_df.withColumn('CUST_CITY', regexp_replace('CUST_CITY', ' ', ''))


    # create the left from of customer df and df cities to get area codes in customer df 
    customer_df = customer_df.join(df_cities3, customer_df.CUST_CITY == df_cities3.CITIES ,"left")
    # we still have about 300/952 null values in area code, replace it with 123
    customer_df= customer_df.na.fill(value="123", subset=['AREA_CODE'])
    customer_df = customer_df.na.fill(99999, ['CUST_ZIP'])


    # create RDD to map each colum and transform as per requeirements:USING COLUMN NAME
    rdd3 = customer_df.rdd.map(lambda x: \
                               (x["SSN"], x["FIRST_NAME"].title(), x["MIDDLE_NAME"].lower(),\
                                x["LAST_NAME"].title(), x["CREDIT_CARD_NO"],\
                                x["STREET_NAME"]+ ', ' + x["APT_NO"],\
                                x["CUST_CITY"], x["CUST_STATE"], x["CUST_COUNTRY"], x["CUST_ZIP"],\
                                '('+ x["AREA_CODE"] +')' + x["CUST_PHONE"][:3] + '-' + x["CUST_PHONE"][3:],\
                                x["CUST_EMAIL"],x["LAST_UPDATED"]))
    
    # Name all the columns according to requirements
    updated_cust_df = rdd3.toDF([
                                'SSN',
                                'FIRST_NAME',
                                'MIDDLE_NAME',
                                'LAST_NAME',
                                'Credit_card_no',
                                'FULL_STREET_ADDRESS',
                                'CUST_CITY',
                                'CUST_STATE',
                                'CUST_COUNTRY',
                                'CUST_ZIP', 
                                'CUST_PHONE',
                                'CUST_EMAIL', 
                                'LAST_UPDATED' 
                                ])
    

    # update column types according to requirements
    updated_cust_df = updated_cust_df\
        .withColumn('SSN',updated_cust_df['SSN'].cast(IntegerType()))\
        .withColumn('FIRST_NAME',updated_cust_df['FIRST_NAME'].cast(VarcharType(40)))\
        .withColumn('MIDDLE_NAME',updated_cust_df['MIDDLE_NAME'].cast(VarcharType(40)))\
        .withColumn('LAST_NAME',updated_cust_df['LAST_NAME'].cast(VarcharType(40)))\
        .withColumn('Credit_card_no',updated_cust_df['Credit_card_no'].cast(VarcharType(18)))\
        .withColumn('FULL_STREET_ADDRESS',updated_cust_df['FULL_STREET_ADDRESS'].cast(VarcharType(40)))\
        .withColumn('CUST_CITY',updated_cust_df['CUST_CITY'].cast(VarcharType(25)))\
        .withColumn('CUST_STATE',updated_cust_df['CUST_STATE'].cast(VarcharType(25)))\
        .withColumn('CUST_COUNTRY',updated_cust_df['CUST_COUNTRY'].cast(VarcharType(25)))\
        .withColumn('CUST_ZIP',updated_cust_df['CUST_ZIP'].cast(IntegerType()))\
        .withColumn('CUST_PHONE',updated_cust_df['CUST_PHONE'].cast(VarcharType(25)))\
        .withColumn('CUST_EMAIL',updated_cust_df['CUST_EMAIL'].cast(VarcharType(40)))\
        .withColumn('LAST_UPDATED',updated_cust_df['LAST_UPDATED'].cast(TimestampType()))
        

    return updated_cust_df


# function to transforn transactional data credit_df
def prepare_creditdf(credit_df):
    '''takes credit_df and output requires DF'''

    # convert DAY MONTH and YEAE to string to be able to create YYYYMMDD formate in varchar type
    credit_df = credit_df\
        .withColumn('DAY',credit_df['DAY'].cast(StringType()))\
        .withColumn('MONTH',credit_df['MONTH'].cast(StringType()))\
        .withColumn('YEAR',credit_df['YEAR'].cast(StringType()))
    
    # since some DAY and MONTH values are single digit. lets left pad them with "0"
    credit_df = credit_df.select('BRANCH_CODE',
                                    'CREDIT_CARD_NO',
                                    'CUST_SSN',
                                    lpad(credit_df.DAY, 2, '0').alias("DAY"),
                                    lpad(credit_df.MONTH, 2, '0').alias("MONTH"),
                                    'TRANSACTION_ID',
                                    'TRANSACTION_TYPE',
                                    'TRANSACTION_VALUE',
                                    'YEAR')
    
    # create RDD to apply lambda function to 
    rdd_credit = credit_df.rdd.map(lambda x: (x[1], x[8]+x[4]+x[3] , x[2], x[0],\
                                        x[6], x[7], x[5]))
    
    #convert back to DF with required column names
    updated_credit_df = rdd_credit.toDF(['CUST_CC_NO',
                                        'TIMEID',
                                        'CUST_SSN',
                                        'BRANCH_CODE',
                                        'TRANSACTION_TYPE',
                                        'TRANSACTION_VALUE',
                                        'TRANSACTION_ID'])
    
    # update types of each colums according to requirements
    updated_credit_df = updated_credit_df\
        .withColumn('CUST_CC_NO',updated_credit_df['CUST_CC_NO'].cast(VarcharType(20)))\
        .withColumn('TIMEID',updated_credit_df['TIMEID'].cast(VarcharType(10)))\
        .withColumn('CUST_SSN',updated_credit_df['CUST_SSN'].cast(IntegerType()))\
        .withColumn('BRANCH_CODE',updated_credit_df['BRANCH_CODE'].cast(IntegerType()))\
        .withColumn('TRANSACTION_TYPE',updated_credit_df['TRANSACTION_TYPE'].cast(VarcharType(40)))\
        .withColumn('TRANSACTION_VALUE',updated_credit_df['TRANSACTION_VALUE'].cast(DoubleType()))\
        .withColumn('TRANSACTION_ID',updated_credit_df['TRANSACTION_ID'].cast(IntegerType()))
    
    return updated_credit_df




def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')



# ETL PROCESS

log("==========ETL Job Started==========")

log("Extract Phase Started")
branch_df = extract_branch_file("cdw_sapp_branch.json")
log("branch file extract: Successful")
customer_df = extract_customer_file("cdw_sapp_custmer.json")
log("Customer file extract: Successful")
credit_df = extract_credit_file("cdw_sapp_credit.json")
log("Credit file extract: Successful")


log("Transform Phase Started")
transformed_branch_df = prepare_branchdf(branch_df)
log("branch df transform: Successful")
transformed_customer_df = prepare_customerdf(customer_df)
log("customer df transform: Successful")
transfromed_credit_df = prepare_creditdf(credit_df)
log("credit df transform: Successful")


log("Load Phase Started")
branch_df.write.mode('overwrite').option("header",True).csv("branchfile")
log("branch df load: Successful")
customer_df.write.mode('overwrite').option("header",True).csv("customerfile")
log("customer df load: Successful")
credit_df.write.mode('overwrite').option("header",True).csv("creditfile")
log("credit df load: Successful")

log("=====ETL job completed successfully=====")