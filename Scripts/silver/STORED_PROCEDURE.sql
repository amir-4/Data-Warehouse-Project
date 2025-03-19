-- WILL Create Stored PROCEDURE
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @ST DATETIME , @ET DATETIME  , @SB DATETIME  , @EB DATETIME 
	SET @SB = GETDATE()
	BEGIN TRY
		SET @ST = GETDATE()
		PRINT'======================================'
		PRINT'START INSERTING CRM SYSTEM'
		PRINT'======================================'
  
		PRINT ' '
  
		PRINT'--------------------------------------'
		PRINT 'Start Inserting crm_cust_info Table'
		PRINT'--------------------------------------'
  
		Print 'Before Insert Make Truncate becuase if it run twich you will have dublicates in the data'
		TRUNCATE TABLE silver.crm_cust_info ;
		Print 'Insert The Data silver.crm_cust_info'
		-- THEN Insert Statment
		INSERT INTO silver.crm_cust_info(cst_id ,
		cst_key ,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		SELECT
		cst_id ,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,

		CASE WHEN UPPER(TRIM(cst_marital_status))  = 'S' THEN 'Single'  
			 WHEN UPPER(TRIM(cst_marital_status))  = 'M' THEN 'Married'
			 ELSE 'n/a'
		END AS cst_marital_status,

		CASE WHEN UPPER(TRIM(cst_gndr))  = 'F' THEN 'Female'  -- Ay F hykhleha Female
			 WHEN UPPER(TRIM(cst_gndr))  = 'M' THEN 'Male'
			 ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
		FROM (
		SELECT * ,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as Flag_Last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE Flag_Last = 1 ;
		SET @ET = GETDATE()
		PRINT'THE DURATION TIME FOR THIS TABLE IS : ' + CAST(DATEDIFF(SECOND , @ST,@ET) AS NVARCHAR) + 'SECONDS' 
		----------------------------------------------------------------------------------------------------------------
		SET @ST = GETDATE()
		PRINT'--------------------------------------'
		PRINT 'Start Inserting crm_prd_info Table'
		PRINT'--------------------------------------'
		TRUNCATE TABLE silver.crm_prd_info ;
		Print 'Insert The Data silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info (
		prd_id,
		prd_key ,
		prd_cat_id,
		prd_nm ,
		prd_cost ,
		prd_line ,
		prd_start_dt ,
		prd_end_dt 
		)
		SELECT prd_id,
		-- WE have here alot of info so will split it
		REPLACE(SUBSTRING(prd_key , 1 , 5),'-' , '_') AS prd_cat_id , -- khlenha b uderscore 3shan ne3rf nrbtoha m3 table tane
		SUBSTRING(prd_key , 7 , len(prd_key)) AS prd_key ,  -- khlenha Len 3shan msh ad b3dhom
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' THEN 'Mountian'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line, 
		CAST (prd_start_dt AS DATE) AS prd_start_dt,
		CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info ;
		SET @ET = GETDATE()
		PRINT'THE DURATION TIME FOR THIS TABLE IS : ' + CAST(DATEDIFF(SECOND , @ST,@ET) AS NVARCHAR) + 'SECONDS' 
------------------------------------------------------------------------------------------
PRINT'--------------------------------------'
		PRINT 'Start Inserting crm_sales_details Table'
		PRINT'--------------------------------------'
		SET @ST = GETDATE()
		TRUNCATE TABLE silver.crm_sales_details ;
		Print 'Insert The Data silver.crm_sales_details'
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
					CASE 
						WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
					END AS sls_order_dt,
					CASE 
						WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
					END AS sls_ship_dt,
					CASE 
						WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
					END AS sls_due_dt,
					CASE 
						WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
							THEN sls_quantity * ABS(sls_price)
						ELSE sls_sales
					END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
					sls_quantity,
					CASE 
						WHEN sls_price IS NULL OR sls_price <= 0 
							THEN sls_sales / NULLIF(sls_quantity, 0)
						ELSE sls_price  -- Derive price if original value is invalid
					END AS sls_price
				FROM bronze.crm_sales_details;

		SET @ET = GETDATE()
		PRINT'THE DURATION TIME FOR THIS TABLE IS : ' + CAST(DATEDIFF(SECOND , @ST,@ET) AS NVARCHAR) + 'SECONDS' 
		-------------------------------------------------------------------------------------------------------
		PRINT'--------------------------------------'
		PRINT 'Start Inserting erp_cust_az12 Table'
		PRINT'--------------------------------------'
		SET @ST = GETDATE()
		-- THEN WILL INSERT THE VALUES
		TRUNCATE TABLE silver.erp_cust_az12 ;
		Print 'Insert The Data silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 
		(
		cid,
		bdate,
		gen
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid , 4 , LEN(cid))
			 ELSE cid
		END AS cid,

		CASE WHEN bdate > GETDATE() THEN NULL 
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'Male'
			 ELSE'n/a'
		END gen
		FROM bronze.erp_cust_az12 
		SET @ET = GETDATE()
		PRINT'THE DURATION TIME FOR THIS TABLE IS : ' + CAST(DATEDIFF(SECOND , @ST,@ET) AS NVARCHAR) + 'SECONDS' 

		-----------------------------------------------------------------------------------------------------
		PRINT'--------------------------------------'
		PRINT 'Start Inserting erp_loc_a101 Table'
		PRINT'--------------------------------------'
		SET @ST = GETDATE()
		-- Insert Value In erp_loc_a101
		TRUNCATE TABLE silver.erp_loc_a101 ;
		Print 'Insert The Data silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(cid , cntry)
		SELECT 
		REPLACE(TRIM(cid),'-','') AS cid ,
		CASE 
				WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a' 
				WHEN TRIM(cntry) = 'DE' THEN 'Germany' 
				WHEN TRIM(cntry) IN ('US' ,'USA') THEN 'United State'
				ELSE TRIM(cntry) 
			END AS cntry 
		FROM bronze.erp_loc_a101
		SET @ET = GETDATE()
		PRINT'THE DURATION TIME FOR THIS TABLE IS : ' + CAST(DATEDIFF(SECOND , @ST,@ET) AS NVARCHAR) + 'SECONDS' 
		------------------------------------------------------------------------------------
		PRINT'--------------------------------------'
		PRINT 'Start Inserting erp_px_cat_g1v2 Table'
		PRINT'--------------------------------------'
		SET @ST = GETDATE()
		-- ITS PERFECT SO LETS INSERT IT AS IT
		TRUNCATE TABLE silver.erp_px_cat_g1v2 ;
		Print 'Insert The Data silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2 ; 
		SET @ET = GETDATE()
		PRINT'THE DURATION TIME FOR THIS TABLE IS : ' + CAST(DATEDIFF(SECOND , @ST,@ET) AS NVARCHAR) + 'SECONDS' 
		PRINT'------------------------------------------------------'
    END TRY
		BEGIN CATCH
		PRINT'==================================================='
		PRINT 'There Is An Erorr Occur While Loading Silver Layer';
		PRINT 'Error Massege Is : ' + ERROR_MESSAGE() ;
		PRINT 'Error Massege Is : ' + CAST(ERROR_NUMBER() AS NVARCHAR) ;
		PRINT 'Error Massege Is : ' + CAST(ERROR_STATE() AS NVARCHAR) ;
		PRINT'===================================================';
		END CATCH
	SET @EB = GETDATE()
		PRINT'===================================================';

	PRINT'THE DURATION TIME FOR ALL BATCH IS : ' + CAST(DATEDIFF(SECOND , @SB,@EB) AS NVARCHAR) + 'SECONDS'
		PRINT'===================================================';

END 

EXEC silver.load_silver
