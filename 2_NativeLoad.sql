USE test_db;

SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE 'C:\\Users\\admin\\Documents\\Air_Quality.csv' 
INTO TABLE air_quality 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

select * from air_quality
where unique_id = 419355;