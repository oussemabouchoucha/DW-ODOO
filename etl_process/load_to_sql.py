"""
Load Data to SQL Server

This script loads the transformed data from the Silver layer into SQL Server
using a dimensional model with dimension and fact tables.
"""

import os
from datetime import datetime
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import pyodbc
import logging

# Set JAVA_HOME environment variable
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jre1.8.0_451'

# Configure logging
date_str = datetime.now().strftime("%Y-%m-%d")
log_dir = os.path.join("logs", "sql_load", date_str)
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "sql_load.log")

logging.basicConfig(filename=log_path, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_message(message, level="info"):
    """Logs messages with the specified level."""
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)
    print(message)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load to SQL Server") \
    .config("spark.driver.extraClassPath", "C:/spark-3.4.3/sqljdbc_4.2.8112.200_enu/sqljdbc_4.2/enu/jre8/sqljdbc42.jar") \
    .getOrCreate()

# Define paths
current_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(current_dir)
silver_base_path = os.path.join(base_dir, "output", "silverLayer")

# Find the latest date folder in silver layer
import glob
silver_date_folders = glob.glob(os.path.join(silver_base_path, "*"))
if not silver_date_folders:
    raise Exception(f"No date folders found in {silver_base_path}")

latest_silver_folder = max(silver_date_folders)
silver_path = latest_silver_folder

log_message(f"Using silver data from: {silver_path}")

# SQL Server connection parameters
server = 'localhost'
database = 'DW_ODOO'
trusted_connection = 'yes'  # Windows authentication

# Create connection string
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection={trusted_connection}'

# Function to execute SQL query
def execute_sql(query, params=None):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        log_message(f"Error executing SQL: {str(e)}", level="error")
        return False

# Function to load data to SQL Server
def load_dataframe_to_sql(df, table_name, if_exists='append'):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Generate insert statements
        for index, row in df.iterrows():
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['?' for _ in range(len(df.columns))])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Convert any NaN values to None
            values = [None if pd.isna(val) else val for val in row]
            
            cursor.execute(query, values)
            
            # Commit every 1000 rows to avoid memory issues
            if index % 1000 == 0:
                conn.commit()
                log_message(f"Committed {index} rows to {table_name}")
        
        conn.commit()
        cursor.close()
        conn.close()
        log_message(f"Successfully loaded {len(df)} rows to {table_name}")
        return True
    except Exception as e:
        log_message(f"Error loading data to {table_name}: {str(e)}", level="error")
        return False

# Read the silver layer data
try:
    # Read data from silver layer
    silver_df = spark.read.parquet(os.path.join(silver_path, "mrp_production.parquet"))
    log_message(f"Successfully read data from {os.path.join(silver_path, 'mrp_production.parquet')}")
    log_message(f"Row count: {silver_df.count()}")
    
    # Convert to pandas for easier processing
    df = silver_df.toPandas()
    log_message("Converted to pandas DataFrame")
except Exception as e:
    log_message(f"Error reading silver layer data: {str(e)}", level="error")
    import traceback
    log_message(traceback.format_exc(), level="error")
    exit(1)

# Process and load dimension tables
try:
    # 1. Create DimDate
    log_message("Creating DimDate table...")
    
    # Get all dates from the dataset (Start, End, Deadline)
    all_dates = pd.concat([
        df['Start'].dropna(),
        df['End'].dropna(),
        df['Deadline'].dropna()
    ]).drop_duplicates().sort_values().reset_index(drop=True)
    
    # Create date dimension
    dim_date = pd.DataFrame({
        'Date': all_dates,
        'Year': all_dates.dt.year,
        'Quarter': all_dates.dt.quarter,
        'Month': all_dates.dt.month,
        'MonthName': all_dates.dt.strftime('%B'),
        'Day': all_dates.dt.day,
        'DayOfWeek': all_dates.dt.dayofweek + 1,  # 1-7 instead of 0-6
        'DayOfWeekName': all_dates.dt.strftime('%A')
    })
    
    # Create DateKey (YYYYMMDD format)
    dim_date['DateKey'] = dim_date['Date'].dt.strftime('%Y%m%d').astype(int)
    
    # Reorder columns
    dim_date = dim_date[['DateKey', 'Date', 'Year', 'Quarter', 'Month', 'MonthName', 'Day', 'DayOfWeek', 'DayOfWeekName']]
    
    # Load DimDate to SQL
    load_dataframe_to_sql(dim_date, 'dbo.DimDate', if_exists='replace')
    
    # 2. Create DimDepartment
    log_message("Creating DimDepartment table...")
    
    # Get unique departments
    departments = df['Responsible'].dropna().unique()
    dim_department = pd.DataFrame({
        'DepartmentID': [f"DEPT_{i+1}" for i in range(len(departments))],
        'DepartmentName': departments
    })
    
    # Load DimDepartment to SQL
    load_dataframe_to_sql(dim_department, 'dbo.DimDepartment', if_exists='replace')
    
    # Create a mapping of department names to keys
    department_mapping = {}
    for i, dept in enumerate(departments):
        department_mapping[dept] = i + 1  # DepartmentKey starts at 1
    
    # 3. Create DimProductCategory
    log_message("Creating DimProductCategory table...")
    
    # Get unique product categories
    product_categories = df['Product_Category'].dropna().unique()
    dim_product_category = pd.DataFrame({
        'ProductCategoryName': product_categories
    })
    
    # Load DimProductCategory to SQL
    load_dataframe_to_sql(dim_product_category, 'dbo.DimProductCategory', if_exists='replace')
    
    # Create a mapping of category names to keys
    category_mapping = {}
    for i, cat in enumerate(product_categories):
        category_mapping[cat] = i + 1  # ProductCategoryKey starts at 1
    
    # 4. Create DimProduct
    log_message("Creating DimProduct table...")
    
    # Get unique products
    products = df[['Product_Code', 'Product_Name', 'Product_Category', 'Product_Cost', 
                  'Product_Sales_Price', 'Profit_Margin_Percent', 'Price_Category', 'Margin_Category']].drop_duplicates()
    
    # Add ProductCategoryKey
    products['ProductCategoryKey'] = products['Product_Category'].map(category_mapping)
    
    # Rename columns to match SQL table
    dim_product = products.rename(columns={
        'Product_Code': 'ProductCode',
        'Product_Name': 'ProductName',
        'Product_Cost': 'ProductCost',
        'Product_Sales_Price': 'ProductSalesPrice',
        'Profit_Margin_Percent': 'ProfitMarginPercent',
        'Price_Category': 'PriceCategory',
        'Margin_Category': 'MarginCategory'
    })
    
    # Select only the columns we need
    dim_product = dim_product[['ProductCode', 'ProductName', 'ProductCategoryKey', 'ProductCost', 
                              'ProductSalesPrice', 'ProfitMarginPercent', 'PriceCategory', 'MarginCategory']]
    
    # Load DimProduct to SQL
    load_dataframe_to_sql(dim_product, 'dbo.DimProduct', if_exists='replace')
    
    # Create a mapping of product codes to keys
    product_mapping = {}
    for i, code in enumerate(products['Product_Code'].unique()):
        product_mapping[code] = i + 1  # ProductKey starts at 1
    
    # 5. Create FactProduction
    log_message("Creating FactProduction table...")
    
    # Create date key mapping function
    def date_to_key(date):
        if pd.isna(date):
            return None
        return int(date.strftime('%Y%m%d'))
    
    # Create the fact table
    fact_production = df[['Reference', 'Product_Code', 'Responsible', 'Start', 'End', 'Deadline',
                         'State', 'Quantity_Producing', 'Quantity_To_Produce', 'Total_Quantity',
                         'Production_Efficiency', 'Production_Duration_Days']].copy()
    
    # Map dimension keys
    fact_production['ProductKey'] = fact_production['Product_Code'].map(product_mapping)
    fact_production['DepartmentKey'] = fact_production['Responsible'].map(department_mapping)
    fact_production['StartDateKey'] = fact_production['Start'].apply(date_to_key)
    fact_production['EndDateKey'] = fact_production['End'].apply(date_to_key)
    fact_production['DeadlineDateKey'] = fact_production['Deadline'].apply(date_to_key)
    
    # Drop original columns that have been mapped
    fact_production = fact_production.drop(['Product_Code', 'Responsible', 'Start', 'End', 'Deadline'], axis=1)
    
    # Rename remaining columns
    fact_production = fact_production.rename(columns={
        'Quantity_Producing': 'QuantityProducing',
        'Quantity_To_Produce': 'QuantityToProduce',
        'Total_Quantity': 'TotalQuantity',
        'Production_Efficiency': 'ProductionEfficiency',
        'Production_Duration_Days': 'ProductionDurationDays'
    })
    
    # Load FactProduction to SQL
    load_dataframe_to_sql(fact_production, 'dbo.FactProduction', if_exists='replace')
    
    log_message("Data loading completed successfully!")
    
except Exception as e:
    log_message(f"Error processing and loading data: {str(e)}", level="error")
    import traceback
    log_message(traceback.format_exc(), level="error")

# Stop Spark session
spark.stop()
log_message("Spark session stopped.")
