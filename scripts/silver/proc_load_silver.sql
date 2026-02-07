/*
Purpose:
Transforms raw Bronze-layer CRM and ERP data into cleaned, standardized,
and business-consumable Silver-layer tables. This procedure applies
deduplication, data normalization, type corrections, and basic
data-quality rules before loading the Silver layer.
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @whole_start_time DATETIME,
            @whole_end_time DATETIME;

    BEGIN TRY
        SET @whole_start_time = GETDATE();
        PRINT '==================================';
        PRINT 'START: Silver Layer Load';
        PRINT '==================================';

        ------------------------------------------------------------------
        -- CRM CUSTOMER INFORMATION
        -- Deduplicate customers and normalize textual attributes
        ------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,        -- Remove leading/trailing spaces
            TRIM(cst_lastname)  AS cst_lastname,         -- Remove leading/trailing spaces
            CASE                                         -- Normalize marital status codes
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE                                         -- Normalize gender codes
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC
                ) AS flag_last                  -- Identify latest record per customer
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;                    -- Keep most recent customer record

        SET @end_time = GETDATE();
        PRINT '>> Duration (silver.crm_cust_info): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';
        PRINT '----------------------------------';

        ------------------------------------------------------------------
        -- CRM PRODUCT INFORMATION
        -- Standardize product attributes and derive validity periods
        ------------------------------------------------------------------

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  -- Derive category ID
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,         -- Extract product key
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,                        -- Default missing cost
            CASE                                                    -- Map product line codes
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                DATEADD(
                    DAY,
                    -1,
                    LEAD(prd_start_dt) OVER (
                        PARTITION BY prd_key
                        ORDER BY prd_start_dt
                    )
                ) AS DATE
            ) AS prd_end_dt                                     -- Derive end date from next start
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Duration (silver.crm_prd_info): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';
        PRINT '----------------------------------';

        ------------------------------------------------------------------
        -- CRM SALES DETAILS
        -- Correct invalid dates and recalculate inconsistent measures
        ------------------------------------------------------------------

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE                                               -- Validate order date
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE                                               -- Validate ship date
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE                                               -- Validate due date
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE                                               -- Recalculate sales if incorrect
                WHEN sls_sales IS NULL
                  OR sls_sales <= 0
                  OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE                                               -- Derive price if missing
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Duration (silver.crm_sales_details): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';
        PRINT '----------------------------------';

        ------------------------------------------------------------------
        -- ERP CUSTOMER DEMOGRAPHICS
        -- Clean identifiers and normalize demographic attributes
        ------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE                                               -- Remove source-specific prefix
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE                                               -- Exclude invalid future dates
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE                                               -- Normalize gender values
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Duration (silver.erp_cust_az12): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';
        PRINT '----------------------------------';

        ------------------------------------------------------------------
        -- ERP CUSTOMER LOCATION
        -- Standardize country names and identifiers
        ------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,                      -- Normalize customer ID
            CASE                                               -- Standardize country values
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Duration (silver.erp_loc_a101): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';
        PRINT '----------------------------------';
        ------------------------------------------------------------------
        -- ERP PRODUCT CATEGORY
        -- Pass-through load (no transformation required)
        ------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Duration (silver.erp_px_cat_g1v2): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
              + ' seconds';
        PRINT '----------------------------------';

        PRINT '==================================';
        PRINT 'END: Silver Layer Load Completed';
        PRINT '==================================';
    END TRY
    BEGIN CATCH
         PRINT '==================================';
        PRINT 'ERROR: Silver Layer Load Failed';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==================================';
    END CATCH

    SET @whole_end_time = GETDATE()

    PRINT '==================================';
    PRINT '>> Duration (silver layer): '
              + CAST(DATEDIFF(SECOND, @whole_start_time, @whole_end_time) AS NVARCHAR)
              + ' seconds';
    PRINT '==================================';
    PRINT '==================================';
END;
GO
EXEC silver.load_silver;
