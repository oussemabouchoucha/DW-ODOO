# DW-ODOO: Data Warehouse ETL Pipeline for Odoo Manufacturing Data

## Project Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline for processing manufacturing data from Odoo ERP system. The pipeline follows a medallion architecture with Bronze, Silver, and Gold layers to progressively refine and analyze production data.

## Architecture

The ETL process follows a three-layer medallion architecture:

1. **Bronze Layer**: Raw data extracted from source systems
2. **Silver Layer**: Cleaned, validated, and enriched data
3. **Gold Layer**: Business-ready analytics models

## Project Structure

```
DW-ODOO/
├── etl_process/
│   ├── extract.ipynb          # Data extraction from source to Bronze layer
│   ├── audit_report.ipynb     # Data quality checks on Bronze layer data
│   ├── transform.ipynb        # Data transformation from Bronze to Silver layer
│   └── gold_layer.ipynb       # Analytics models creation in Gold layer
├── data/
│   └── source/                # Source data files
├── output/
│   ├── bronzeLayer/           # Raw data storage
│   ├── silverLayer/           # Cleaned and transformed data
│   └── goldLayer/             # Business-ready analytics models
└── logs/                      # Process execution logs
```

## ETL Process

### 1. Extract

The extraction process reads data from source files and loads it into the Bronze layer. Key features:
- Reads data from CSV/Excel files
- Preserves raw data in its original form
- Organizes data by date
- Logs all operations for traceability

### 2. Audit Report

The audit process performs data quality checks on the Bronze layer data:
- Checks for missing values
- Identifies data type issues
- Detects duplicate records
- Generates comprehensive audit reports

### 3. Transform

The transformation process cleans and enriches the data:
- Standardizes column names
- Handles missing values
- Extracts product codes and names
- Calculates derived metrics (production efficiency, profit margins)
- Adds time dimensions for analysis
- Categorizes products by type, price range, and margin

### 4. Gold Layer

The gold layer creates business-ready analytics models:
- Department Production Summary
- Product Category Performance
- Monthly Production Trends
- Top Performing Products
- Production Efficiency Dashboard

## Getting Started

### Prerequisites

- Python 3.8+
- PySpark
- Java 8 (for Spark)
- Jupyter Notebook

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/DW-ODOO.git
cd DW-ODOO
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

### Running the ETL Pipeline

Execute the notebooks in the following order:

1. `extract.ipynb` - Extract data from source to Bronze layer
2. `audit_report.ipynb` - Perform data quality checks
3. `transform.ipynb` - Transform data from Bronze to Silver layer
4. `gold_layer.ipynb` - Create analytics models in Gold layer

## Data Models

### Silver Layer Schema

The Silver layer contains cleaned and enriched data with the following key fields:
- Product information (Product_Code, Product_Name, Product_Category)
- Production metrics (Quantity_Producing, Quantity_To_Produce, Total_Quantity)
- Financial data (Product_Cost, Product_Sales_Price)
- Derived metrics (Production_Efficiency, Profit_Margin_Percent)
- Time dimensions (Start_Year, Start_Month, Start_Day, etc.)
- Categorization (Product_Category, Price_Category, Margin_Category)

### Gold Layer Models

1. **Department Production Summary**
   - Aggregates production metrics by department
   - Includes efficiency and profitability metrics

2. **Product Category Performance**
   - Analyzes performance by product category
   - Includes production volume, efficiency, and profit margins

3. **Monthly Production Trends**
   - Tracks production metrics over time
   - Identifies seasonal patterns

4. **Top Performing Products**
   - Identifies most profitable and efficient products
   - Ranks products by various performance metrics

5. **Production Efficiency Dashboard**
   - Cross-tabulates efficiency by department and product category
   - Highlights areas for improvement

## Best Practices

This project follows these ETL best practices:
- Clear separation of concerns (Extract, Transform, Load)
- Data quality validation before transformation
- Column standardization in the transform phase
- Comprehensive logging and error handling
- Well-documented notebooks with markdown sections
- Consistent naming conventions


## Contact

Bouchoucha1oussema@gmail.com

linkedin.com/in/oussema-bouchoucha-3ba12420a/