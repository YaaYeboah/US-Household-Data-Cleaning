-- 1 DUPLICATING RAW DATA
DROP TABLE IF EXISTS us_household_income;
CREATE TABLE us_household_income AS
SELECT * FROM USHouseholdIncome;




DELIMITER $$
DROP PROCEDURE IF EXISTS Copy_and_Clean_data;
CREATE PROCEDURE Copy_and_Clean_data()
BEGIN
-- CREATING THE TABLE
	CREATE TABLE IF NOT EXISTS us_household_income_cleaned (
	  `row_id` int DEFAULT NULL,
	  `id` int DEFAULT NULL,
	  `State_Code` int DEFAULT NULL,
	  `State_Name` text,
	  `State_ab` text,
	  `County` text,
	  `City` text,
	  `Place` text,
	  `Type` text,
	  `Primary` text,
	  `Zip_Code` int DEFAULT NULL,
	  `Area_Code` varchar(10) DEFAULT NULL,
	  `ALand` bigint DEFAULT NULL,
	  `AWater` bigint DEFAULT NULL,
	  `Lat` double DEFAULT NULL,
	  `Lon` double DEFAULT NULL,
	   `TimeStamp`TIMESTAMP DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    
-- COPY DATA INTO NEW TABLE

INSERT INTO us_household_income_cleaned
SELECT *,CURRENT_TIMESTAMP
FROM us_household_income;

-- Data Cleaning Steps

-- 1.Remove Duplicates
DELETE FROM us_household_income_cleaned
WHERE 
	row_id IN (
	SELECT row_id
FROM (
	SELECT row_id, id,
		ROW_NUMBER() OVER (
			PARTITION BY id,TimeStamp
			ORDER BY id,TimeStamp) AS row_num
	FROM 
		us_household_income_cleaned
) duplicates
WHERE 
	row_num > 1
);



-- 2.Standardization
UPDATE us_household_income_cleaned
SET State_Name = 'Georgia'
WHERE State_Name = 'georia';

UPDATE us_household_income_cleaned
SET County = UPPER(County);

UPDATE us_household_income_cleaned
SET City = UPPER(City);

UPDATE us_household_income_cleaned
SET Place = UPPER(Place);

UPDATE us_household_income_cleaned
SET State_Name = UPPER(State_Name);

UPDATE us_household_income_cleaned
SET `Type` = 'CDP'
WHERE `Type` = 'CPD';

UPDATE us_household_income_cleaned
SET `Type` = 'Borough'
WHERE `Type` = 'Boroughs';


END $$
DELIMITER ;

CALL Copy_and_Clean_data();

-- CREATE EVENT
DROP EVENT run_data_cleaning;
CREATE EVENT run_data_cleaning
  ON SCHEDULE EVERY 30 DAY
  DO CALL Copy_and_Clean_data();







-- DEBUGGING OR CHECKIMNG THAT STORED PROCEDURE WORKS
	-- 1.Check Duplicates
		SELECT row_id,id,row_num
	FROM (
		SELECT row_id, id,
			ROW_NUMBER() OVER (
				PARTITION BY id
				ORDER BY id) AS row_num
		FROM 
			us_household_income
	) duplicates
	WHERE 
		row_num > 1;

	-- Number of rows in raw data
	SELECT COUNT(row_id)
	FROM us_household_income;

	-- Count of each state name
	SELECT State_Name,COUNT(State_Name)
	FROM us_household_income
	GROUP BY State_Name;

	-- Checking cleaned data
	-- Check Duplicates
		SELECT row_id,id,row_num
	FROM (
		SELECT row_id, id,
			ROW_NUMBER() OVER (
				PARTITION BY id
				ORDER BY id) AS row_num
		FROM 
			us_household_income_cleaned
	) duplicates
	WHERE 
		row_num > 1;

	-- Number of rows in clean data
	SELECT COUNT(row_id)
	FROM us_household_income_cleaned;

	-- Count of each state name in clean data
	SELECT State_Name,COUNT(State_Name)
	FROM us_household_income_cleaned
	GROUP BY State_Name;



