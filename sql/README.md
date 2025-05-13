# SQL Server Data Loading

This directory contains SQL scripts and documentation for loading the transformed data into SQL Server Management Studio (SSMS).

## Dimensional Model

The data warehouse follows a dimensional model with the following tables:

### Dimension Tables

1. **DimDate**
   - DateKey (PK)
   - Date
   - Year
   - Quarter
   - Month
   - MonthName
   - Day
   - DayOfWeek
   - DayOfWeekName

2. **DimDepartment**
   - DepartmentKey (PK)
   - DepartmentID
   - DepartmentName

3. **DimProductCategory**
   - ProductCategoryKey (PK)
   - ProductCategoryName

4. **DimProduct**
   - ProductKey (PK)
   - ProductCode
   - ProductName
   - ProductCategoryKey (FK)
   - ProductCost
   - ProductSalesPrice
   - ProfitMarginPercent
   - PriceCategory
   - MarginCategory

### Fact Table

**FactProduction**
   - ProductionKey (PK)
   - Reference
   - ProductKey (FK)
   - DepartmentKey (FK)
   - StartDateKey (FK)
   - EndDateKey (FK)
   - DeadlineDateKey (FK)
   - State
   - QuantityProducing
   - QuantityToProduce
   - TotalQuantity
   - ProductionEfficiency
   - ProductionDurationDays

## Database Setup

1. Create a new database in SQL Server Management Studio named `DW_ODOO`.
2. Run the `create_tables.sql` script to create the dimension and fact tables.
3. Run the Python script `etl_process/load_to_sql.py` or the notebook `etl_process/load_to_sql.ipynb` to load the data.

## Loading Process

The loading process follows these steps:

1. Read the transformed data from the Silver layer
2. Create dimension tables:
   - Extract unique values for each dimension
   - Create mappings from dimension values to keys
3. Create fact table:
   - Map dimension keys to the fact data
   - Rename columns to match SQL table structure
4. Load all tables to SQL Server

## Sample Queries

Here are some sample queries you can run to analyze the data:

### Production by Department

```sql
SELECT 
    d.DepartmentName,
    COUNT(*) AS TotalProductions,
    SUM(f.TotalQuantity) AS TotalUnits,
    AVG(f.ProductionEfficiency) AS AvgEfficiency
FROM 
    dbo.FactProduction f
    JOIN dbo.DimDepartment d ON f.DepartmentKey = d.DepartmentKey
GROUP BY 
    d.DepartmentName
ORDER BY 
    TotalProductions DESC;
```

### Production by Product Category

```sql
SELECT 
    pc.ProductCategoryName,
    COUNT(*) AS TotalProductions,
    SUM(f.TotalQuantity) AS TotalUnits,
    AVG(p.ProfitMarginPercent) AS AvgProfitMargin
FROM 
    dbo.FactProduction f
    JOIN dbo.DimProduct p ON f.ProductKey = p.ProductKey
    JOIN dbo.DimProductCategory pc ON p.ProductCategoryKey = pc.ProductCategoryKey
GROUP BY 
    pc.ProductCategoryName
ORDER BY 
    TotalUnits DESC;
```

### Monthly Production Trends

```sql
SELECT 
    d.Year,
    d.Month,
    d.MonthName,
    COUNT(*) AS ProductionCount,
    SUM(f.TotalQuantity) AS TotalUnits,
    AVG(f.ProductionEfficiency) AS AvgEfficiency
FROM 
    dbo.FactProduction f
    JOIN dbo.DimDate d ON f.StartDateKey = d.DateKey
GROUP BY 
    d.Year, d.Month, d.MonthName
ORDER BY 
    d.Year, d.Month;
```

## Troubleshooting

If you encounter issues with the loading process:

1. Check the logs in the `logs/sql_load` directory
2. Verify that the SQL Server connection parameters are correct
3. Ensure that the SQL Server instance is running and accessible
4. Check that the database `DW_ODOO` exists and you have permissions to create tables and insert data
