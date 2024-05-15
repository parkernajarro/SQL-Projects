-- SQL Project for Data Cleaning Practice
-- Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Goals
-- 1. Remove Duplicates
-- 2. Standardize Data
-- 3. Remove Null or Blank Values
-- 4. Remove Unnecessary Values


-- Create a table copy to perserve original raw data
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;



-- 1. Remove Duplicates
-- Begin by finding duplicate values
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

WITH duplicate_cte AS (
    SELECT *, 
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num 
    FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;


-- Create another new table and filter duplicates
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2 
SELECT *, 
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num 
    FROM layoffs_staging;

DELETE FROM layoffs_staging2 
WHERE row_num > 1;

-- Double check that duplicates where in fact removed
SELECT * FROM layoffs_staging2;



-- 2. Standardizing Data
-- Starting with company column
SELECT TRIM(COMPANY) FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(COMPANY);

-- Combine matching industries of 'Crypto', 'CryptoCurrency', and 'Cyrpto Currency' under 1 distinct title 'Crypto'
SELECT * FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Repeat for all other effected columns

-- Country: Resolve 'United States' & 'United States.'
SELECT DISTINCT(country) FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Date Column
SELECT `date`, str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Actually convert 'date' to standard date format
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- Convert the 'date' column to date datatype instead of txt
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;



-- 3. Correct Null & Blank Values

-- total_laid_off and percentage_laid_off null values appear to be within normal bounds so no change needed
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- Find all industries with null or blank values
SELECT * FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Convert all blank values to Null so we can ensure data consistency
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Find records where 'industry' is missing and join with records of the exact same companies that have 'industry' defined in order to fill in missing values
SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Populate the null values if possible
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- After checking we see that only the company "Bally's" was null since there is only a single null instance of that record from the start
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;



-- 4. Remove unneccesary values

-- Delete useless data we can't use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete row_num from the table now that we are finished using it
SELECT * FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
