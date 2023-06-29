# Lets imports all the necessary libraries

import pandas as pd
import numpy as np
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,TimestampType,VarcharType
from pyspark.sql.functions import *
import time
import requests


#create spark session
spark = SparkSession.builder.appName('Session5').getOrCreate()


# load /read the file to create the DF
branch_df = spark.read.json("TransformedBranchData.json")
customer_df = spark.read.json("TransformedCustomerData.json")
credit_df = spark.read.json("TransformedCreditData.json")


customer_df = customer_df.select('SSN',
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
                                'LAST_UPDATED')

branch_df = branch_df.select('BRANCH_CODE',
                            'BRANCH_NAME',
                            'BRANCH_STREET',
                            'BRANCH_CITY',
                            'BRANCH_STATE',
                            'BRANCH_ZIP',
                            'BRANCH_PHONE',
                            'LAST_UPDATED')

credit_df = credit_df.select('CUST_CC_NO',
                            'TIMEID',
                            'CUST_SSN',
                            'BRANCH_CODE',
                            'TRANSACTION_TYPE',
                            'TRANSACTION_VALUE',
                            'TRANSACTION_ID')
#print(credit_df.show())



# Function Requirement 1.2 - utilizes Python, PySpark, and Python modules to load data into RDBMS(SQL), perform the following:
 
# a) Create a Database in SQL(MySQL), named “creditcard_capstone.”
# b) Create a Python and Pyspark Program to load/write the “Credit Card System Data” into RDBMS(creditcard_capstone).
# Tables should be created by the following names in RDBMS:
# CDW_SAPP_BRANCH
# CDW_SAPP_CREDIT_CARD
# CDW_SAPP_CUSTOMER 

'''
branch_df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "CDW_SAPP_BRANCH") \
  .option("user", "root") \
  .option("password", "Password") \
  .save()


customer_df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "CDW_SAPP_CUSTOMER") \
  .option("user", "root") \
  .option("password", "Password") \
  .save()

credit_df.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
  .option("user", "root") \
  .option("password", "Password") \
  .save()
'''

# 211 Used to display the transactions made by customers living 
# in a given zip code for a given month and year. Order by day in descending order.
# 53151  2018  08
def req211():
    a = input("Please enter zip code: ")
    b = input("Enter Year: ") + input("Enter Month: ") + "%"

    result211_df= credit_df.join(customer_df, credit_df.CUST_CC_NO == customer_df.Credit_card_no)\
                        .select("TRANSACTION_ID", "TRANSACTION_VALUE","TRANSACTION_TYPE","CUST_ZIP","TIMEID")\
                        .filter((col("CUST_ZIP") == a) & (credit_df.TIMEID.like(b)))\
                        .dropDuplicates()\
                        .sort(col("TIMEID").desc())
    
    return result211_df


# Used to display the number and total values of transactions for a given type.
def req212():
    '''display the number and total values of transactions for a given type'''
    a = input("""\n
            1. Bills \n
            2. Healthcare \n
            3. Gas \n
            4. Education \n
            5. Test \n
            6. Entertainment \n
            7. Grocery \n
            .....Enter Number for type of trasaction: """)
    dict = {"1":"Bills","2":"Healthcare", 
            "3":"Gas", "4":"Education", 
            "5":"Test", "6":"Entertainment", 
            "7":"Grocery"}
    result212_df = credit_df.filter(credit_df.TRANSACTION_TYPE == dict[a])\
                        .groupby("TRANSACTION_TYPE")\
                        .agg(
                            count("TRANSACTION_TYPE").alias("NUMBER_OF_TRANSACTION"), 
                            round(sum("TRANSACTION_VALUE"),2).alias("TOTAL_VALUES_OF_TRANSACTION")
                            )
    return result212_df



# 213 Used to display the total number and total values of transactions for branches in a given state.
def req213():
    '''display the total number and total values of transactions for branches in a given state'''
    state_code = input("Please input 2 letters STATE code: ").upper()
    result213_df =  credit_df.join(branch_df, credit_df.BRANCH_CODE == branch_df.BRANCH_CODE)\
                        .filter(col("BRANCH_STATE") == state_code)\
                        .groupby("BRANCH_STATE")\
                        .agg(
                            count(credit_df.TRANSACTION_VALUE).alias("Number_of_Transaction"),
                            round(sum(credit_df.TRANSACTION_VALUE),2).alias("Total_Value_of_Transaction")
                            )
    return result213_df




# 221 Used to check the existing account details of a customer.
# (123)124-3018, 4210653310116272, Wilber Dunham, 123454487
def req221():
    '''to check the existing account details of a customer.'''
    first_name = input("First Name: ")
    last_name = input("Last Name: ")
    ssn = input("SSN: ")
    ccnumber = input("Credit Cart Number: ")

    if len(ssn) != 0 or len(ccnumber) != 0:
        result221_df = customer_df.filter((col("SSN") == ssn) | (col("Credit_card_no") == ccnumber))
    elif len(first_name) != 0 and len(last_name) != 0:
        result221_df= customer_df.filter((col("FIRST_NAME") == first_name) & (col("LAST_NAME") == last_name))\
                                .dropDuplicates()
    elif len(first_name) != 0 or len(last_name) != 0:
        result221_df= customer_df.filter((col("FIRST_NAME") == first_name) | (col("LAST_NAME") == last_name))\
                                .dropDuplicates()
    return result221_df




# 222 to modify the existing account details of a customer. ask for enter all numbers you want to update after entering SSN
# once all numbers entered, split to make list to check if that number is in the list.
def req222():
    '''to modify the existing account details of a customer'''
    ssn = (int(input("Modify Customer Detail. What is SSN of the Customer: ")))

    # you can also use f string below to show current details of customer 
    details = list(input("enter all letters that needed to be modified (reference below):\n\
 a. 'FIRST_NAME',\n b. 'MIDDLE_NAME',\n c. 'LAST_NAME',\n d. 'FULL_STREET_ADDRESS',\n e. 'CUST_CITY',\n\
 f. 'CUST_STATE',\n g. 'CUST_COUNTRY',\n h. 'CUST_ZIP',\n i. 'CUST_PHONE',\n j. 'CUST_EMAIL'\n \
 ENTER THE LETTER/LETTERS: "))
    
    dict1 = {'a':'FIRST_NAME','b':'MIDDLE_NAME','c':'LAST_NAME', 'd':'FULL_STREET_ADDRESS',\
            'e':'CUST_CITY', 'f':'CUST_STATE', 'g':'CUST_COUNTRY','h':'CUST_ZIP', 'i':'CUST_PHONE', 'j':'CUST_EMAIL'}
    
    df2 = customer_df
    
    for letter in details:
        newdetail = (input(f"New {dict1[letter]}: "))
        df2 = df2.withColumn(dict1[letter], when(col("SSN")== ssn , newdetail).otherwise(df2[dict1[letter]]))
        df2 = df2.withColumn("LAST_UPDATED", when(col("SSN")== ssn , from_unixtime(current_timestamp().cast("long"))).otherwise(df2.LAST_UPDATED))

    return df2
    



# 223 Used to generate a monthly bill for a credit card number for a given month and year.
# sample: 4210653349028689     2018  08
def req223():
    '''to generate a monthly bill for a credit card number for a given month and year.'''
    billccno = input("Please enter Credit Card number without dashes for monthly bill: ")
    billyear = input("Enter the Year: ")
    billmonth = input("Enter the Month: ")
    timeidlike = billyear + billmonth + "%"
    firstname = customer_df.filter(col("Credit_card_no") == billccno).head()[1]
    middlename = customer_df.filter(col("Credit_card_no") == billccno).head()[2]
    lastname = customer_df.filter(col("Credit_card_no") == billccno).head()[3]
    addressstreet = customer_df.filter(col("Credit_card_no") == billccno).head()[5]
    addresscity = customer_df.filter(col("Credit_card_no") == billccno).head()[6]
    addressstate = customer_df.filter(col("Credit_card_no") == billccno).head()[7]
    addresszip = customer_df.filter(col("Credit_card_no") == billccno).head()[9]
                                                                          
    print("\n \n")
    print(f"MONTHLY BILL FOR {firstname} {lastname} ")
    print()
    # below code can be upgraded to include if statement so month does not become 13 
    print(f"Billing Period: {billmonth}-01-{billyear} to {int(billmonth) + 1}-01-{billyear}")
    print()
    print(firstname," ",middlename.title()," ",lastname)
    print(addressstreet,",")
    print(addresscity,", ",addressstate," ",addresszip)
    print()
    print("+-------------------------+\n|   MINIMUM PAYMENT DUE   |\n+-------------------------+\n|          $15.00         |\n+-------------------------+")
    print()

    resultdf = credit_df.filter((col("CUST_CC_NO") == billccno) & (credit_df.TIMEID.like(timeidlike)))\
            .withColumn("Date", to_date(col("TIMEID"), "yyyyMMdd"))\
            .select("Date", "TRANSACTION_TYPE", "TRANSACTION_VALUE", "TRANSACTION_ID")
    resultdf2 = resultdf.agg(round(sum(credit_df.TRANSACTION_VALUE),2).alias("Total_MONTHLY_PAYMENT_DUE"))
    
    return resultdf, resultdf2



# 224 Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
# 4210653349028689  09-01-2018    12-31-2018
def req224():
    '''to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.'''
    custcc = input("Please enter customer Credit Card Number wihtout Dashes: ")
    startdate = (input("Enter Starting date MM-DD-YYYY formate: "))
    enddate = (input("Enter Ending date MM-DD-YYYY formate: "))
    starting_date = startdate[6:] + startdate[0:2] + startdate[3:5]
    ending_date = enddate[6:] + enddate[0:2] + enddate[3:5]
    result224_df = credit_df.filter((col("TIMEID") > starting_date ) & (col("TIMEID") < ending_date ))\
                            .filter(col("CUST_CC_NO") == custcc)\
                            .withColumn("Date", to_date(col("TIMEID"), "yyyyMMdd"))\
                            .sort(col("TIMEID").desc())\
                            .select("CUST_CC_NO", "Date", "BRANCH_CODE", "TRANSACTION_TYPE", "TRANSACTION_VALUE","TRANSACTION_ID")
    return result224_df


# branch_df.write.mode('overwrite').option("header",True).csv("branchfile")

def write_file(df):
    '''takes DF and write the file as CSV'''
    answer = input("Would you like to save the above output as a CSV file?(Y/N): ")
    if answer.upper() == "Y":
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        df.write.mode("overwrite").option("header", True).csv(timestamp)
        print(f"Your file has been saved in {timestamp} folder")
    print("Returning to main menu........")
    time.sleep(2.5)
    print()
    print()



def print_menu():
    '''Print OPTIONS to the user'''
    print("====================Please choose from one of the following options:=============================================")
    print("1. TRANSACTION made by customers living in a given ZIP CODE for a given MONTH and YEAR")
    print("2. The number and total values of TRANSACTIONS for a given TYPE")
    print("3. Total number and total values of TRANSACTIONS for branches in a given STATE")
    print("4. Check the existing ACCOUNT DETAILS of a CUSTOMER")
    print("5. MODIFY the existing ACCOUNT DETAILS of a CUSTOMER")
    print("6. Generate a MONTHLY BILL for a credit card number for a given MONTH and YEAR")
    print("7. TRANSACTIONS made by a CUSTOMER between two DATES")
    print("8. EXIT")


option = 0 

print('''

       /$$ /$$       /$$$$$$$$ /$$   /$$  /$$$$$$  /$$      /$$ /$$$$$$$  /$$       /$$$$$$$$       /$$$$$$$   /$$$$$$  /$$   /$$ /$$   /$$             /$$ /$$      
      /$$//$$/      | $$_____/| $$  / $$ /$$__  $$| $$$    /$$$| $$__  $$| $$      | $$_____/      | $$__  $$ /$$__  $$| $$$ | $$| $$  /$$/            /$$//$$/      
     /$$//$$/       | $$      |  $$/ $$/| $$  \ $$| $$$$  /$$$$| $$  \ $$| $$      | $$            | $$  \ $$| $$  \ $$| $$$$| $$| $$ /$$/            /$$//$$/       
    /$$//$$/        | $$$$$    \  $$$$/ | $$$$$$$$| $$ $$/$$ $$| $$$$$$$/| $$      | $$$$$         | $$$$$$$ | $$$$$$$$| $$ $$ $$| $$$$$/            /$$//$$/        
   /$$//$$/         | $$__/     >$$  $$ | $$__  $$| $$  $$$| $$| $$____/ | $$      | $$__/         | $$__  $$| $$__  $$| $$  $$$$| $$  $$           /$$//$$/         
  /$$//$$/          | $$       /$$/\  $$| $$  | $$| $$\  $ | $$| $$      | $$      | $$            | $$  \ $$| $$  | $$| $$\  $$$| $$\  $$         /$$//$$/          
 /$$//$$/           | $$$$$$$$| $$  \ $$| $$  | $$| $$ \/  | $$| $$      | $$$$$$$$| $$$$$$$$      | $$$$$$$/| $$  | $$| $$ \  $$| $$ \  $$       /$$//$$/           
|__/|__/            |________/|__/  |__/|__/  |__/|__/     |__/|__/      |________/|________/      |_______/ |__/  |__/|__/  \__/|__/  \__/      |__/|__/            
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
''')


while True:

    
    print_menu()
    print()
    try:
        option = int(input("Enter Single Digit Option Number: "))
    
    except:
        print("invalid input. Please try again.")

    if option == 1:
        df211 = req211()
        df211.show()
        write_file(df211)
    
    elif option == 2:
        df212 = req212()
        df212.show()
        write_file(df212)

    elif option == 3:
        df213 = req213()
        df213.show()
        write_file(df213)

    elif option == 4:
        df221 = req221()
        df221.show()
        write_file(df221)

    elif option == 5:
        df222 = req222()
        df222.show()
        write_file(df222)

    elif option == 6:
        df2231, df2232 = req223()
        df2231.show()
        df2232.show()
        write_file(df2231)

    elif option == 7:
        df224 = req224()
        df224.show()
        write_file(df224)
    
    elif option == 8:
        print("\n EXIT OPTION SELECTED \n")
        break
    
    else:
        option = 0
        print("Please enter valid input.")

print("\n THANK YOU, HAVE A NICE DAY.\n")





