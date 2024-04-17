-- Data Cleaning

SELECT * 
FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardize Data
-- 3. Handle Null or blank values
-- 4. Remove irrelevant columns

-- Removing duplicates

CREATE  TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT * from  layoffs;

with duplicate_cte as 
(SELECT *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)

select * from duplicate_cte
where row_num > 1;

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

select * from layoffs_staging2;

insert into layoffs_staging2
SELECT *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;

select * from layoffs_staging2
where row_num>1;

delete from layoffs_staging2
where row_num>1;

select * from layoffs_staging2;

-- Standardizing data

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country
from layoffs_staging2
where country like 'United States%';

update layoffs_staging2
set country = 'United States'
where industry like 'United States%';

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date` from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

-- Handling null values

select *
from layoffs_staging2
where industry is null or industry='';

update layoffs_staging2
set industry = null
where industry ='';

select t1.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null and t2.industry is not null;

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

DELETE
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_staging2;

-- Remove irrelevant columns
alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;