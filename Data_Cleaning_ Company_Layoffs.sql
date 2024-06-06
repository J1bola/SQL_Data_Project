-- Data Cleaning

SELECT *
FROM layoffs;

-- Here are the steps I will take to ensure this data is usable:
-- 1. Remove duplicates.
-- 2. Standardize the data.
-- 3. Null Values
-- 4. Remove unnecessary columns

-- Please note: to ensure I do not tamper wiht my raw data, I will be creating a duplicate table - staging. 

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;
-- Inserting data from layoffs into layoffs_staging

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT company
FROM layoffs_staging;
-- Staging table created. 

-- 1. Identifying and removing duplicates

-- finding unique keys here

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- creating a CTE for the query above so I can source out all the duplicates below.

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num >1;

-- was unable to delete duplicates because CTEs cannot be updated - 

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)

DELETE
FROM duplicate_cte
WHERE row_num >1;

-- alternatively, I will be creating an extra table that filters just the duplicates, then delete them. 

CREATE TABLE `layoffs_staging2` (
`compaany` TEXT,
`location` TEXT,
`industry` TEXT, 
`total_laid_off` int DEFAULT NULL, 
`percentage_laid_off` TEXT,
`date` TEXT, 
`stage` TEXT, 
`country` TEXT,
`funds_raised_millions` int DEFAULT NULL,
`row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SELECT *
FROM layoffs_staging2;  -- table creation worked

INSERT INTO layoffs_staging2 -- inserted data into new table created
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
from layoffs_staging;


DELETE -- deleted the duplicates
FROM layoffs_staging2
WHERE row_num >1;

SELECT *
FROM layoffs_staging2
WHERE row_num = 1;

ALTER TABLE layoffs_staging2 RENAME COLUMN compaany TO company;

-- STEP 2 - Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company); -- did this to clear out the unnecessary space to the left. 

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; -- Merging multiple columns with different variations of "crypto" in their name. 


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
-- United states had another column with the same name but with a '.'.

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
-- I changed the format of date and also altered the data type from TEXT to DATE for future use. 
-- I ensured not to do this on the main table - only my staging table.


-- Working with NUll or Blank Values here. 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

UPDATE layoffs_staging2
SET industry = 'Travel' AND percentage_laid_off = '0.25' -- this was an errror
WHERE company LIKE 'Airbnb';

UPDATE layoffs_staging2
SET industry = 'Travel', percentage_laid_off = '0.25' -- this is correct. I updated the Airbnb blanks with values. 
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; -- I do not need them when doing exploratory DS because total laid off and percentage laid off would be needed. 


SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
-- Removed the row number column which was no longer usefull. 








