/*
================================================================================
Purpose:
This script performs data quality assessment and validation across
Bronze and Silver layers for CRM and ERP datasets.

Key Principles:
- Bronze layer is used ONLY for data quality assessment and profiling.
- No data is modified in the Bronze layer.
- All cleansing, normalization, and corrections are applied in the Silver layer.
- Silver-layer checks validate that Bronze issues were properly resolved.

Usage Notes:
- Run this script after Bronze ingestion and Silver transformation jobs.
- All queries are read-only and safe to execute.
- Most checks are expected to return rows in Bronze (data issues),
  and ZERO rows in Silver (issues resolved).
================================================================================
*/

-------------------------------------------------------------------------------
-- BRONZE LAYER : CRM CUSTOMER INFORMATION – DATA QUALITY PROFILING
-------------------------------------------------------------------------------

-- Identify NULL or duplicate customer identifiers
SELECT
    cst_id,
    COUNT(*) AS record_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Detect unwanted spaces in first names
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

-- Detect unwanted spaces in last names
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname);

-- Profile raw gender values prior to standardization
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-------------------------------------------------------------------------------
-- SILVER LAYER : CRM CUSTOMER INFORMATION – QUALITY VERIFICATION
-------------------------------------------------------------------------------

-- Verify that duplicates and NULL identifiers were resolved
SELECT
    cst_id,
    COUNT(*) AS record_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Verify whitespace cleanup in first names
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

-- Verify whitespace cleanup in last names
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname);

-- Verify standardized gender values
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

-------------------------------------------------------------------------------
-- BRONZE LAYER : CRM PRODUCT INFORMATION – DATA QUALITY PROFILING
-------------------------------------------------------------------------------

-- Identify NULL or duplicate product identifiers
SELECT
    prd_id,
    COUNT(*) AS record_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Detect unwanted spaces in product names
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- Identify NULL or negative product costs
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Profile raw product line codes
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Identify invalid product date ranges
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-------------------------------------------------------------------------------
-- SILVER LAYER : CRM PRODUCT INFORMATION – QUALITY VERIFICATION
-------------------------------------------------------------------------------

-- Verify duplicate and NULL product identifiers were resolved
SELECT
    prd_id,
    COUNT(*) AS record_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Verify trimmed product names
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- Verify corrected product cost values
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Verify standardized product line values
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Verify valid product date ranges
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-------------------------------------------------------------------------------
-- BRONZE LAYER : CRM SALES DETAILS – DATA QUALITY PROFILING
-------------------------------------------------------------------------------

-- Detect unwanted spaces in order numbers
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);

-- Identify missing product references
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Identify missing customer references
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Identify invalid order dates (format and range)
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
   OR LEN(sls_order_dt) <> 8
   OR sls_order_dt > 20500101
   OR sls_order_dt < 19000101;

-- Identify invalid shipping dates
SELECT NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
   OR LEN(sls_ship_dt) <> 8
   OR sls_ship_dt > 20500101
   OR sls_ship_dt < 19000101;

-- Validate sales date chronology
-- Rule: order_date < ship_date <= due_date
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt
   OR sls_ship_dt >= sls_due_dt;

-- Profile violations of sales calculation business rule
-- Rule: sales = quantity * price (no NULL, zero, or negative values)
SELECT DISTINCT
    sls_sales      AS original_sales,
    sls_price      AS original_price,
    sls_quantity
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
   OR sls_sales <> sls_quantity * sls_price;

-------------------------------------------------------------------------------
-- SILVER LAYER : CRM SALES DETAILS – QUALITY VERIFICATION
-------------------------------------------------------------------------------

-- Verify whitespace cleanup in order numbers
SELECT sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);

-- Verify referential integrity to product dimension
SELECT *
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Verify referential integrity to customer dimension
SELECT *
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Verify valid sales date chronology
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt
   OR sls_ship_dt >= sls_due_dt;

-------------------------------------------------------------------------------
-- ERP CUSTOMER & REFERENCE DATA – PROFILING AND VERIFICATION
-------------------------------------------------------------------------------

-- Profile out-of-range birth dates (Bronze)
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Verify cleaned birth dates (Silver)
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Profile raw gender values (Bronze)
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

-- Verify standardized gender values (Silver)
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- Identify location records without matching CRM customers (Bronze)
SELECT
    REPLACE(cid, '-', '') AS cid,
    cntry
FROM bronze.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- Identify unlinked product categories (Bronze)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info);

-- Detect whitespace issues in category values (Bronze)
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat <> TRIM(cat)
   OR subcat <> TRIM(subcat);

-- Profile maintenance flag values (Bronze)
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;
