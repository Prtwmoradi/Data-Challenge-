-- Create a new table named train_events by reading data from a CSV file
CREATE TABLE train_events AS 
SELECT * 
FROM read_csv_auto("C:\Users\Perry\OneDrive\Desktop\child-mind-institute-detect-sleep-states\child-mind-institute-detect-sleep-states\train_events.csv");

-- Count the number of distinct values in the series_id column of the train_events table
SELECT COUNT(DISTINCT series_id) 
FROM train_events;

-- Count the number of rows in the train_events table where the step column is null
SELECT COUNT(*) 
FROM train_events 
WHERE step IS NULL;

-- Retrieve all rows and columns from the train_events table
SELECT * 
FROM train_events;

--creating two other tables as a middle table to join data from them 
CREATE TABLE wakeup AS
SELECT  night,series_id, step,timestamp
FROM main.train_events where event='wakeup'

CREATE TABLE onset AS
SELECT  night,series_id, step,timestamp
FROM main.train_events where event='onset';

--then I insert data into a table by joining from two tables 
INSERT
	INTO
	main.train_events_new(series_id,night,
	onsetstep,
	onsettimestamp,
	wakeupstep,
	wakeuptimestamp)
select
b.series_id,b.night,
	b.step,
	b.timestamp,
	a.step,
	a.timestamp,
from
	wakeup as a,
	onset as b
where
	a.night = b.night and a.series_id=b.series_id


-- from the beginig we had about 12 million rows and it reduced to 7254 rows:
	select count(*) from train_events_new where 	series_id = '05e1944c3818' order by night asc
--you can divide timestamp in duckdb by using:
SELECT
night,event,step,
        EXTRACT(YEAR FROM CAST(timestamp AS TIMESTAMP)) AS year,
        EXTRACT(month FROM CAST(timestamp AS TIMESTAMP)) AS month,
        EXTRACT(day FROM CAST(timestamp AS TIMESTAMP)) AS day,
        EXTRACT(hour FROM CAST(timestamp AS TIMESTAMP)) AS hour,
FROM train_events
        
 -- and also this:
SELECT
        series_id,
        CAST (step AS INTEGER) AS step,
        CAST(timestamp AS TIMESTAMP) AS TIMESTAMPTZ,
        CAST (anglez AS FLOAT) AS anglez,
        CAST (enmo AS FLOAT) AS enmo,
        EXTRACT(YEAR FROM CAST(timestamp AS TIMESTAMP)) AS year,
        EXTRACT(month FROM CAST(timestamp AS TIMESTAMP)) AS month,
        EXTRACT(day FROM CAST(timestamp AS TIMESTAMP)) AS day,
        EXTRACT(hour FROM CAST(timestamp AS TIMESTAMP)) AS hour,

FROM train_series


SELECT AVG(anglez)  as average_anglez
FROM train_series
WHERE
    CAST(timestamp AS TIMESTAMP) < '2018-08-15 02:26:00.000'
    AND series_id = '038441c925bb';

   
SELECT
    series_id,
    CAST(step AS INTEGER) AS step,
    CAST(timestamp AS TIMESTAMP) AS TIMESTAMPTZ,
    CAST(anglez AS FLOAT) AS anglez,
    CAST(enmo AS FLOAT) AS enmo,
    EXTRACT(YEAR FROM CAST(timestamp AS TIMESTAMP)) AS year,
    EXTRACT(month FROM CAST(timestamp AS TIMESTAMP)) AS month,
    EXTRACT(day FROM CAST(timestamp AS TIMESTAMP)) AS day,
    EXTRACT(hour FROM CAST(timestamp AS TIMESTAMP)) AS hour,
    EXTRACT(minute FROM CAST(timestamp AS TIMESTAMP)) AS minute,
    EXTRACT(second FROM CAST(timestamp AS TIMESTAMP)) AS second
FROM
    train_series
WHERE
    CAST(timestamp AS TIMESTAMP) <= '2018-08-15 02:26:00.000'
    and series_id='038441c925bb'
