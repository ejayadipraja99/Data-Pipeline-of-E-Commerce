'''
=======================================================
Name : Erlangga Jayadipraja

This program was created to automate data fetch from the PostgreSQL 
database, Data Cleaning process, and load CSV clean data to be loaded 
into Elasticsearchm using E-Commerece dataset.
=======================================================
'''
import pandas as pd
import psycopg2 as db
import numpy as np
from elasticsearch import Elasticsearch

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def fetch_data():
    '''
    This function is intended to retrieve data from PostgreSQL and will be continue to data cleaning
    '''
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)

    # Read raw data from SQL
    df = pd.read_sql("select * from table_m3", conn)

    # Save raw data from SQL
    df.to_csv('data_raw.csv', index=False)

def data_cleaning():
    '''
    This function is intended to cleaning raw data and save the clean data to CSV
    '''
    # Read Raw Data
    df = pd.read_csv('data_raw.csv', index_col=False)

    # Rename columns name to give whitespace between word
    df.rename(columns= {"CustomerID":"Customer ID","PreferredLoginDevice":"Preferred Login Device","CityTier":"City Tier",
                        "WarehouseToHome":"Warehouse To Home","PreferredPaymentMode":"Preferred Payment Mode","HourSpendOnApp":"Hour Spend On App",
                        "NumberOfDeviceRegistered":"Number Of Device Registered","PreferedOrderCat":"Prefered Order Cat","SatisfactionScore":"Satisfaction Score",
                        "MaritalStatus":"Marital Status","NumberOfAddress":"Number Of Address","OrderAmountHikeFromlastYear":"Order Amount Hike From last Year",
                        "CouponUsed":"Coupon Used","OrderCount":"Order Count","DaySinceLastOrder":"Day Since Last Order","CashbackAmount":"Cashback Amount"},
                        inplace=True)
    
    # Change whitespace between word to "_" in the columns name 
    df.columns = df.columns.str.replace(' ','_')

    # Change columns name to lowercase
    df.columns = df.columns.str.strip().str.lower()

    # Handling duplicated, if there is a duplicate data it will be dropped
    if df.duplicated().sum() > 0:
        df.drop_duplicates(ignore_index=True, inplace=True)

    # Handing missing values, if there is a missing values data it will be dropped
    if df.isnull().sum().sum() > 0:
         df.dropna(inplace=True)

    # Save clean data
    df.to_csv('data_clean.csv', index=True)
    
def post_to_es():
    '''
    This function is intended to upload to Elasticsearch and visualize using Kibana
    '''

    # Data Read
    df = pd.read_csv('data_clean.csv', index_col=False)
    es = Elasticsearch("Elasticsearch")
    es.ping()

    index_name = 'milestone_3'

    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index=index_name, body=doc)

default_args = {
    'owner' : 'erlangga',
    'start_date' : dt.datetime(2024, 1, 11, 6, 30, 0) - dt.timedelta(hours=7)
} 

with DAG('milestone_3',
         default_args=default_args,
         schedule_interval='30 23 * * *', # Set For 23.30 UTC or 06.30 WIB
         catchup=False
        ) as dag:
    
    # Task 1
    fetch_task = PythonOperator(task_id='fetch_task', python_callable=fetch_data)

    # Task 2
    clean_task = PythonOperator(task_id='clean_task', python_callable=data_cleaning)

    # Task 3 
    post_to_es_task = PythonOperator(task_id='post_to_es_task', python_callable=post_to_es)

fetch_task >> clean_task >> post_to_es_task 