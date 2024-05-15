-- SQL Project: Exploratory Data Analysis

-- Display all records within the layoffs_staging2 table
SELECT * 
FROM layoffs_staging2;


-- Find Highest amount of people laid of at one time and highest layoff percent
SELECT MAX(total_laid_off), MAX(percentage_laid_off) 
FROM layoffs_staging2;


-- Retrieve list of companies that went under (laid off 100% of staff) and order by highest funding received
SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


-- Find total sum of workers laid off per country and order results by greatest amount
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;


-- Retrieves the earliest and latest dates the layoffs occured within the dataset
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;


-- Find the total amount of layoffs per stage of companies
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;


-- Find the total percent a company has laid off over the span of the dataset
SELECT company, SUM(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


-- Calculate the total sum of layoffs per month over the years
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;


-- Calculate a rolling sum of lay offs per month that displays each month's total and cumulative total
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, 
SUM(total_off) OVER(ORDER BY `MONTH`) as rolling_total
FROM Rolling_Total;


-- Find the total amount of a company's layoffs per year and ranks by the highest
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;


-- Rank companies each year by the highest amount of layoffs and displays the top 5 companies for each year
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <= 5;