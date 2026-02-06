/*
Purpose:
Creates the core database and logical schemas required for a
layered Data Warehouse architecture (Bronze, Silver, Gold).

- Bronze: Raw, unprocessed source data
- Silver: Cleaned and conformed data
- Gold: Business-ready, aggregated data
*/

USE master;
GO

-- Create the main Data Warehouse database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the Data Warehouse database
USE DataWarehouse;
GO

-- Schema for raw ingestion from source systems
CREATE SCHEMA bronze;
GO

-- Schema for cleaned and transformed data
CREATE SCHEMA silver;
GO

-- Schema for business-level aggregates and analytics
CREATE SCHEMA gold;
GO
