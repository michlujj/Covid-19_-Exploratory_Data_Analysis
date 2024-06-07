

SELECT * FROM Github..Covid19_deaths
ORDER BY 3,4;

-- dataset contains info from 2020 to 2024
SELECT MIN(date), MAX(date) FROM Github..Covid19_deaths;

-- to check if date is categorise in the correct format
SELECT date FROM Github..Covid19_deaths;

-- to permanently change datatype of 'date' column to DATE format
ALTER TABLE Covid19_deaths
ALTER COLUMN date DATE;

-- to check for any duplicated rows in the dataset using CTE, there are no duplicated values
WITH duplicate_cte AS(
SELECT *,
		ROW_NUMBER() OVER(
		PARTITION BY iso_code, continent, location, date, population, total_cases, new_cases, new_cases_smoothed, total_deaths, new_deaths, new_deaths_smoothed,
		total_cases_per_million, new_cases_per_million, new_cases_smoothed_per_million, total_deaths_per_million, new_deaths_per_million, new_deaths_smoothed_per_million, reproduction_rate,
		icu_patients,icu_patients_per_million,hosp_patients, hosp_patients_per_million, weekly_icu_admissions,weekly_icu_admissions_per_million,
		weekly_hosp_admissions, weekly_hosp_admissions_per_million
		ORDER BY iso_code) row_num
		FROM Github..Covid19_deaths
		)
		SELECT * FROM duplicate_cte
		WHERE row_num > 1
		ORDER BY continent;


-- to select out columns useful for further analysis
SELECT Location, date, total_cases, new_cases, total_deaths, population
FROM Github..Covid19_deaths
ORDER BY Location, date;

-- there are 255 countries report in the dataset
SELECT DISTINCT location FROM Github..Covid19_deaths;

-- To explore Death % by countries, to convert 'numeric' data type into float before division, order in descending
-- total_deaths & total_cases are string (varchar) types need to be convert into float before division 
--using NULLIF because there are missing values in both columns
SELECT Location, date, total_cases, total_deaths,
(CONVERT(float, total_deaths) / NULLIF(CONVERT(float, total_cases), 0)) * 100 AS Death_percentage
FROM Github..Covid19_deaths
ORDER BY Death_percentage DESC;	

-- What are the % of deaths from Covid-19 in 'Singapore'? % deaths reported in daily basis
-- maximum is 0.46% deaths
SELECT location, date, total_cases, total_deaths,
(CONVERT(float, total_deaths) / NULLIF(CONVERT(float, total_cases), 0))* 100 AS Death_percent
FROM Github..Covid19_deaths
WHERE Location = 'Singapore'
ORDER BY Death_percent DESC;

--What % of population in Singapore contracted Covid-19? report on daily basis
-- in Mar 2023, 53.33% of Singapore population is infected with Covid-19
SELECT location, date, Population, total_cases,
(CONVERT(float, total_cases)/ NULLIF(CONVERT(float, Population), 0))*100 AS Percent_pop_infected
FROM Github..Covid19_deaths
WHERE Location = 'Singapore'
ORDER BY Percent_pop_infected DESC;

--What are the countries with Highest Covid-19 infection rates? (total_cases/Population)
-- Cyprus has the highest Covid-19 infection rates
SELECT location, Population, MAX(total_cases) AS Highest_infection_count,
MAX(CONVERT(float, total_cases) / NULLIF(CONVERT(float, Population), 0))* 100 
AS Percent_population_infected
FROM Github..Covid19_deaths
GROUP BY Population, Location
ORDER BY Percent_population_infected DESC;

-- there are missing values in 'continent' column
SELECT * FROM Github..Covid19_deaths
WHERE continent = '';

--Which countries have the highest death count per population? Europe has the high death count
--to convert total_deaths varchar(255) into Integer data type using CAST function
SELECT location, MAX(CAST(total_deaths AS INT)) AS Total_death_Count FROM Github..Covid19_deaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY Total_death_Count DESC;

--Which Continent have the highest death count per population?
--North America has the highest death count
SELECT continent, MAX(CAST(total_deaths AS INT)) AS Total_death_count FROM Github..Covid19_deaths
WHERE continent != ''
GROUP BY continent
ORDER BY Total_death_Count DESC;

-- What is death % from Covid-19 across the world?
-- World wide death % from Covid-19 is 0.91%
SELECT SUM(CONVERT(float, new_cases)) AS total_cases, SUM(CONVERT(FLOAT, new_deaths)) AS total_deaths,
SUM(CONVERT(FLOAT, new_deaths))/SUM(CONVERT(FLOAT,new_cases))*100 As Death_percentage
FROM Github..Covid19_deaths
WHERE continent != ''
ORDER BY 1,2;

-- What are the maximum numbers for total infected and death rates from Covid-19 World-wide?
SELECT MAX(total_cases) AS Total_infected, MAX(total_deaths) AS Total_deaths FROM Github..Covid19_deaths;

-- to see total deaths from Covid-19 by year, 2023 saw the highest deaths count from Covid-19 World wide
SELECT YEAR(date), SUM(CONVERT(FLOAT, total_deaths)) as Total_Deaths FROM Github..Covid19_deaths
GROUP BY YEAR(date)
ORDER BY Total_Deaths DESC;

SELECT date FROM Github..Covid19_deaths;

-- to view cumulative frequency of Covid-19 deaths by Year, month
-- to pass convert(varchar(50),23) to date to convert date column into yyyy-mm format before substring functions(1,7)
SELECT SUBSTRING(CONVERT(varchar(50),date,23), 1,7) as dates, SUM(CONVERT(FLOAT, total_deaths)) AS Total_Deaths
FROM Github..Covid19_deaths
GROUP BY date
ORDER BY date DESC;

-- to use CTE to view the cumulative frequency of total deaths, Over order by date, Order by date
-- 1, 2 can only be used in 'Order By'
WITH cumulative_freq AS 
(
SELECT SUBSTRING(CONVERT(varchar(50),date,23), 1,7) AS date, SUM(CONVERT(FLOAT, total_deaths)) AS Total_Deaths
FROM Github..Covid19_deaths
GROUP BY date
)
SELECT date,  total_deaths, SUM(CONVERT(FLOAT, total_deaths)) OVER(ORDER BY date) AS cumulative_freq_deaths
FROM cumulative_freq
ORDER BY 1;

-- Europe has the highest total deaths counts from Covid-19 in 2023 and 2022
SELECT continent, YEAR(date) AS Year, SUM(CONVERT(FLOAT,total_deaths)) AS Total_Deaths FROM Github..Covid19_deaths
WHERE continent != ''
GROUP BY continent, YEAR(date)
ORDER BY Total_Deaths DESC;

SELECT continent, YEAR(date) FROM Github..Covid19_deaths
WHERE continent != '';

-- to rank the total number of total deaths from Covid-19 by continent by Year
-- Europe has consistently the highest number of deaths from Covid-19 in 2023 and 2022
SELECT location, YEAR(date) AS Year, SUM(CONVERT(FLOAT,total_deaths)) AS Total_Deaths FROM Github..Covid19_deaths
WHERE location NOT IN ('World','High income','Upper middle income', 'Lower middle income') --remove unwanted variables
GROUP BY location, YEAR(date)
ORDER BY Total_Deaths DESC;

--- Using CTE to look at locations with the most deaths from Covid-19 by Year, by Rankings
WITH location_Year (Location, year, Total_deaths) AS
(
SELECT location, YEAR(date), SUM(CONVERT(FLOAT, total_deaths)) AS Total_Deaths FROM Github..Covid19_deaths
GROUP BY location, YEAR(date)
)
SELECT *, DENSE_RANK() OVER (PARTITION BY YEAR ORDER BY CONVERT(FLOAT, total_deaths) DESC) AS Ranking
FROM location_Year
WHERE location NOT IN ('World','High income','Upper middle income', 'Lower middle income')
ORDER BY Ranking;

-- to rank by total no. of deaths from Covid-19 in Ascending order
-- Europe has consistently highest number of Covid-19 deaths in 2021, 2022, 2023, 2024
WITH Location_Year (Location, year, Total_deaths) AS
(
SELECT location, YEAR(date), SUM(CONVERT(FLOAT, total_deaths)) AS Total_Deaths FROM Github..Covid19_deaths
GROUP BY location, YEAR(date)
), Location_Year_Rank AS
(SELECT *, DENSE_RANK() OVER (PARTITION BY YEAR ORDER BY CONVERT(FLOAT, total_deaths) DESC) AS Ranking
FROM location_Year
WHERE location NOT IN ('World','High income','Upper middle income', 'Lower middle income')
)
SELECT * FROM Location_Year_Rank
WHERE Ranking <= 5; -- to print top 5 countries rankings only

SELECT * FROM [Portfolio project]..CovidDeaths dea
JOIN [Portfolio project]..CovidVaccinations vac
ON dea.location = vac.location
AND dea.date = vac.date;

-- to join 2 tables: Covid-19 deaths & Vaccination
SELECT * FROM Github..Covid19_deaths dea
JOIN Github..Covid19_vaccinations vac
ON dea.location = vac.location
AND dea.date = vac.date

-- What is the total number of people in the world that have been vaccination against Covid-19?
-- Cuba has the highest number of people who are fully vaccinated from Covid-19 as of 2022
SELECT dea.continent, dea.location, dea.date, dea.population, vac.people_fully_vaccinated
FROM Github..Covid19_deaths dea
JOIN Github..Covid19_vaccinations vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent != ''  --removes null values
ORDER BY people_fully_vaccinated DESC;

--using Partition BY, to separate by Location
SELECT dea.continent, dea.location, dea.date, dea.population, vac.people_fully_vaccinated,
SUM(CONVERT(FLOAT,vac.people_fully_vaccinated)) OVER (PARTITION BY dea.Location)
FROM Github..Covid19_deaths dea
JOIN Github..Covid19_vaccinations vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent != ''
ORDER BY people_fully_vaccinated DESC;

-- To use CTE(common table expression) for cumulative frequency % computation
With Pop_vs_Vac (Continent, Location, Date, Population, Total_vaccinations, Cumulative_freq_Vaccinated)
AS
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.total_vaccinations,
SUM(CONVERT(FLOAT, vac.total_vaccinations)) OVER (PARTITION BY dea.Location Order by dea.location,
dea.date) AS cumulative_freq
FROM Github..Covid19_deaths dea
JOIN Github..Covid19_vaccinations vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent != ''
) --convert population to float
SELECT * ,NULLIF(CONVERT(FLOAT,Cumulative_freq_Vaccinated),0)/CONVERT(FLOAT,Population)*100 AS
Percentage_vaccinated
FROM Pop_vs_Vac
ORDER By date DESC;
















-- 