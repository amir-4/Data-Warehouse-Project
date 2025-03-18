
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME ,@BST DATETIME , @BET DATETIME
	SET @BST = GETDATE()
	BEGIN TRY	
		PRINT 'LOADING BRONZE LAYER';
		PRINT '================================================' ;

		PRINT '------------------------------------------------' ;
		PRINT 'LOADING CRM SOURCE SYSTEM';
		PRINT '------------------------------------------------' ;

		--Insert data into tabels by BULK INSERT 
		--If it run again will do dublicated data inside same table, fa ne3ml truncate

		SET @start_time = GETDATE();
		print '>> TRUNCATE TABLE : crm_cust_info:  '
		TRUNCATE TABLE bronze.crm_cust_info ;

		-- law fe data adema hymsa7ha b3d kda hy3ml insert lel gdeda de

		print '>> INSERT DATA INTO TABLE : crm_cust_info:  '
		BULK INSERT bronze.crm_cust_info 
		FROM 'E:\Project Material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE() ;
		PRINT 'THE LOAD DURATION : ' + CAST (DATEDIFF(second , @start_time , @end_time)AS NVARCHAR)+ ' SECONDS' 
		PRINT '------------------------------'

		SET @start_time = GETDATE();
		print '>> TRUNCATE TABLE : crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info ;

		print '>> INSERT DATA INTO TABLE : crm_prd_info'
		BULK INSERT bronze.crm_prd_info 
		FROM 'E:\Project Material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE() ;
		PRINT 'THE LOAD DURATION : ' + CAST (DATEDIFF(second , @start_time , @end_time)AS NVARCHAR)+ ' SECONDS' 
		PRINT '------------------------------'

		SET @start_time = GETDATE();
		print '>> TRUNCATE TABLE : crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details ;

		print '>> INSERT DATA INTO TABLE : crm_sales_details'
		BULK INSERT bronze.crm_sales_details 
		FROM 'E:\Project Material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE() ;
		PRINT 'THE LOAD DURATION : ' + CAST (DATEDIFF(second , @start_time , @end_time)AS NVARCHAR)+ ' SECONDS' 
		PRINT '------------------------------'


		PRINT '------------------------------------------------'
		PRINT 'LOADING ERP SOURCE SYSTEM'
		PRINT '------------------------------------------------'

		SET @start_time = GETDATE();
		print '>> TRUNCATE TABLE : erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12 ;

		print '>> INSERT DATA INTO TABLE : erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\Project Material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE() ;
		PRINT 'THE LOAD DURATION : ' + CAST (DATEDIFF(second , @start_time , @end_time)AS NVARCHAR)+ ' SECONDS'
		PRINT '------------------------------'

		SET @start_time = GETDATE();
		print '>> TRUNCATE TABLE : erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101 ;

		print '>> INSERT DATA INTO TABLE : erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\Project Material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE() ;
		PRINT 'THE LOAD DURATION : ' + CAST (DATEDIFF(second , @start_time , @end_time)AS NVARCHAR)+ ' SECONDS'
		PRINT '------------------------------'

		SET @start_time = GETDATE();
		print '>> TRUNCATE TABLE : erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2 ;

		print '>> INSERT DATA INTO TABLE : erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\Project Material\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
			SET @end_time = GETDATE() ;
			PRINT 'THE LOAD DURATION : ' + CAST (DATEDIFF(second , @start_time , @end_time)AS NVARCHAR)+ ' SECONDS' 
			PRINT '------------------------------'

		END TRY
	BEGIN CATCH
		PRINT '================================================' ;
		PRINT 'ERROR OCCUR WHEN LOADING BRONZE LAYER' ;
		PRINT 'Erorr Massege' + ERROR_MESSAGE();
		PRINT 'Erorr Massege' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Erorr Massege' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '================================================' ;
	END CATCH
	SET @BET = GETDATE()
	PRINT 'THE BATCH DURATION IS : ' + CAST(DATEDIFF(SECOND , @BST , @BET) AS NVARCHAR) + ' SECONDS'
END
