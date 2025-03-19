-- >> Start With Fisrt Table bronze.crm_cust_info
-- check from nulls & dublicates in primary key

SELECT cst_id , COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL; -- 3SHAN YTL3LE LW FE RAKM METKRAR AKTAR MN 1

-- Tele3 feha dublicated fa hamsk wahed wahedd w azbato

SELECT *
FROM bronze.crm_cust_info
where cst_id = 29466 ;

-- da fe nafs el haga bs fe wahed bs el kamel fa ha3ml rank w sort bel date w hakhod a3la wahed w f kolo kda
-- da hykhlene akhod agdad data
SELECT *
FROM (
SELECT * ,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as Flag_Last
FROM bronze.crm_cust_info)t WHERE Flag_Last = 1 ;


-- Check The Unwanted Spces

-- RESULT : There Is Unwanted Space 
SELECT cst_firstname 
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- RESULT : There Is Unwanted Space 
SELECT  cst_lastname 
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname) ;

-- RESULT : NO Unwanted Space 
SELECT cst_marital_status 
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status) ;

-- RESULT : NO Unwanted Space 
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr) ;

-- RESULT : NO Unwanted Space 
SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key) ;

-- Check Data Consistancy And Standardization IN cst_material_status & cst_gndr
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info


-- CLEAN ALL Table 
SELECT
cst_id ,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,

CASE WHEN UPPER(TRIM(cst_marital_status))  = 'S' THEN 'Single'  
	 WHEN UPPER(TRIM(cst_marital_status))  = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr))  = 'F' THEN 'Female'  -- Ay F hykhleha Female
	 WHEN UPPER(TRIM(cst_gndr))  = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
SELECT * ,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as Flag_Last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE Flag_Last = 1 ;
-------------------------------------------------------------------------------------------------
-- >> The Second Table crm_prd_info
-- CHECK THE DUBLICATES IN PRIMARY KEY
SELECT prd_id , COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- will split the prd_key to can relation it with other tabels

-- check the unwanted spaces
-- RESULT = SAFE

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) ;

-- check nulls & negativee number in prd_cost 

-- RESULT : There is 2 null values and i will replace it with 0 b is null method
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL ;

-- CHECK THE Cardeinality for prd_line
-- RESULT : HADE KOL WAHED esmo el kamel 
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;


-- CHECK INVALID DATES
-- Tele3 fee fa hanst5dm LEAD() 3shan tegble el tane start date w thotoo ka end date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt ;

SELECT *,
    DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS LLEEAADDD
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


-- Becuase Changing Some Meta Data

IF OBJECT_ID('silver.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info ;
CREATE TABLE silver.crm_prd_info (
prd_id INT ,
prd_key nvarchar(50),
prd_cat_id nvarchar(50),
prd_nm nvarchar(100),
prd_cost INT,
prd_line nvarchar(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_creadted_date DATETIME2 DEFAULT GETDATE()
);

-- Clean It All
SELECT prd_id,
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
END prd_line, 
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info ;
----------------------------------------------------------------------------------------------------------
-- >> The Third Table crm_sales_details
-- CHECK UNWANTED SPACES FOR FIRST COL

SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- CHECK THAT I CAN RELATION BETWEEN TABELS WITH THIS TABLE 


SELECT * 
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_cat_id FROM silver.crm_prd_info)

SELECT * 
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)


-- TRANSFORM sls_order_dt
-- RESULT : THERE IS ALOT = 0 SO WILL CHANGE IN TO NULL
SELECT 
NULLIF(sls_order_dt , 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 ;

-- FOR THREE COULMNS DATE

/*
sls_cust_id  ,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8  THEN NULL
	ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE )
	END AS sls_order_dt ,
  
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8  THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE )
	END AS sls_ship_dt ,

	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8  THEN NULL
	ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE )
	END AS sls_due_dt ,
*/

-- CHECK IF ORDER DATE GREATER THAN SHIPPING 
-- RESULT = PERFECT
SELECT * 
FROM bronze.crm_sales_details
Where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt or sls_ship_dt > sls_due_dt;

-- BUSINESS Role Sales = quantity * Price
-- RESULT I FOUND SUM OF PROPLEMS SO I WILL TALK TO THE DOMIN EXPERT

SELECT * 
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price  
or sls_sales IS NULL or sls_sales <= 0
or sls_quantity IS NULL or sls_quantity <= 0
or sls_price IS NULL or sls_price <= 0 ;


/* RULES :
1 - IF Sales is zero, negative or null drive it by quantity multibile price
2 - IF Price is zero , Calculate it with sales and quantity
3 - IF Price is negative , Convert it to postize 
*/

SELECT *, 
    CASE 
        WHEN sls_sales IS NULL OR sls_sales = 0 THEN sls_quantity * sls_price 
        ELSE sls_sales 
    END AS sls_sales,
    
    CASE 
        WHEN sls_price IS NULL THEN ABS(sls_sales / NULLIF(sls_quantity, 0))
        ELSE ABS(sls_price)
    END AS sls_price 
FROM bronze.crm_sales_details

-- Clean It ALL 
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
-----------------------------------------------------------------------------------------------------------------------------------
-- START WITH ERP SYSTEM
-----------------------------------------------------------------------------------------------------------------------------------
-- >> THE erp_cust_az12 Table

--CHECK THE PRIMARY KEY

SELECT cid 
FROM bronze.erp_cust_az12;  -- TELE3 BYBDA2 B 'NAS' FA LAZEM AGHYRHA 3SHAN A3RF ARBOTO

SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid , 4 , LEN(cid)) -- MYA MYA
	 ELSE cid
END AS cid
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid , 4 , LEN(cid)) -- 3shan at2aked en kol el hena hnak 3shan a2dr arbothom
	 ELSE cid
END NOT IN(SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- CHECK INVALID AND IMPOSIPOLE DATES

SELECT bdate , 
CASE WHEN bdate > GETDATE() THEN NULL 
	 ELSE bdate
END AS bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' or bdate > GETDATE();

-- CHECK low cardinalities FOR GENDER

SELECT gen ,
CASE WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'Male'
	 ELSE'n/a'
END gen
FROM bronze.erp_cust_az12 ;

-- Clean It ALL 
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

-------------------------------------------------------------------------------------------------------------------
-- >> The crm_cust_info Table
-- Check Primary Key
SELECT cid ,
REPLACE(TRIM(cid),'-','')AS cid 
FROM bronze.erp_loc_a101
WHERE REPLACE(TRIM(cid),'-','') NOT IN(SELECT cst_key FROM silver.crm_cust_info)  -- TO CHECK THAT I CAN MAKE A RELATION


-- 2- CHECK cntry

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101

SELECT
CASE 
        WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a' 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany' 
		WHEN TRIM(cntry) IN ('US' ,'USA') THEN 'United State'
        ELSE TRIM(cntry) 
    END AS cntry 
FROM bronze.erp_loc_a101

-- Clean It All
SELECT 
REPLACE(TRIM(cid),'-','') AS cid ,
CASE 
        WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a' 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany' 
		WHEN TRIM(cntry) IN ('US' ,'USA') THEN 'United State'
        ELSE TRIM(cntry) 
    END AS cntry 
FROM bronze.erp_loc_a101
--------------------------------------------------------------------------------------------------------------------------------
-- >> THE erp_px_cat_g1v2 Table
-- CHECK UNWANTED SPACES 
SELECT * 
FROM bronze.erp_px_cat_g1v2 
WHERE TRIM(cat) != cat OR TRIM(subcat) != subcat OR TRIM(maintenance) != maintenance;

-- CHECK CONSISTANCY & STANDARDIZATION
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2 

-- Its Perfect Table
