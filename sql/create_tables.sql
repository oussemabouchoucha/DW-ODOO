-- SQL Script to create dimension and fact tables for the Odoo Manufacturing Data Warehouse

-- Create DimDate table
CREATE TABLE dbo.DimDate (
    DateKey INT PRIMARY KEY,
    Date DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    MonthName NVARCHAR(10) NOT NULL,
    Day INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayOfWeekName NVARCHAR(10) NOT NULL
);

-- Create DimDepartment table
CREATE TABLE dbo.DimDepartment (
    DepartmentKey INT IDENTITY(1,1) PRIMARY KEY,
    DepartmentID NVARCHAR(20) NOT NULL,
    DepartmentName NVARCHAR(100) NOT NULL
);

-- Create DimProductCategory table
CREATE TABLE dbo.DimProductCategory (
    ProductCategoryKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductCategoryName NVARCHAR(100) NOT NULL
);

-- Create DimProduct table
CREATE TABLE dbo.DimProduct (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductCode NVARCHAR(20) NOT NULL,
    ProductName NVARCHAR(100) NOT NULL,
    ProductCategoryKey INT NOT NULL,
    ProductCost DECIMAL(10,2) NOT NULL,
    ProductSalesPrice DECIMAL(10,2) NOT NULL,
    ProfitMarginPercent DECIMAL(10,2) NOT NULL,
    PriceCategory NVARCHAR(20) NOT NULL,
    MarginCategory NVARCHAR(20) NOT NULL,
    CONSTRAINT FK_DimProduct_DimProductCategory FOREIGN KEY (ProductCategoryKey) 
        REFERENCES dbo.DimProductCategory (ProductCategoryKey)
);

-- Create FactProduction table
CREATE TABLE dbo.FactProduction (
    ProductionKey INT IDENTITY(1,1) PRIMARY KEY,
    Reference NVARCHAR(20) NOT NULL,
    ProductKey INT NOT NULL,
    DepartmentKey INT NOT NULL,
    StartDateKey INT NOT NULL,
    EndDateKey INT NOT NULL,
    DeadlineDateKey INT NOT NULL,
    State NVARCHAR(20) NOT NULL,
    QuantityProducing DECIMAL(10,2) NOT NULL,
    QuantityToProduce DECIMAL(10,2) NOT NULL,
    TotalQuantity DECIMAL(10,2) NOT NULL,
    ProductionEfficiency DECIMAL(10,2) NOT NULL,
    ProductionDurationDays INT NOT NULL,
    CONSTRAINT FK_FactProduction_DimProduct FOREIGN KEY (ProductKey) 
        REFERENCES dbo.DimProduct (ProductKey),
    CONSTRAINT FK_FactProduction_DimDepartment FOREIGN KEY (DepartmentKey) 
        REFERENCES dbo.DimDepartment (DepartmentKey),
    CONSTRAINT FK_FactProduction_StartDate FOREIGN KEY (StartDateKey) 
        REFERENCES dbo.DimDate (DateKey),
    CONSTRAINT FK_FactProduction_EndDate FOREIGN KEY (EndDateKey) 
        REFERENCES dbo.DimDate (DateKey),
    CONSTRAINT FK_FactProduction_DeadlineDate FOREIGN KEY (DeadlineDateKey) 
        REFERENCES dbo.DimDate (DateKey)
);

-- Create indexes for better performance
CREATE INDEX IX_FactProduction_ProductKey ON dbo.FactProduction (ProductKey);
CREATE INDEX IX_FactProduction_DepartmentKey ON dbo.FactProduction (DepartmentKey);
CREATE INDEX IX_FactProduction_StartDateKey ON dbo.FactProduction (StartDateKey);
CREATE INDEX IX_FactProduction_EndDateKey ON dbo.FactProduction (EndDateKey);
CREATE INDEX IX_FactProduction_DeadlineDateKey ON dbo.FactProduction (DeadlineDateKey);
