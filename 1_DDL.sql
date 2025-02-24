CREATE DATABASE test_db;
USE test_db;

DROP TABLE air_quality;

CREATE TABLE air_quality (
    unique_id 		INT PRIMARY KEY,
    indicator_id 	INT,
    name 			VARCHAR(255),
    measure 		VARCHAR(255),
    measure_info 	VARCHAR(255),
    geo_type_name 	VARCHAR(100),
    geo_join_id 	INT,
    geo_place_name 	VARCHAR(255),
    time_period 	VARCHAR(50),
    start_date 		DATE,
    data_value 		DECIMAL(10,2)
);

TRUNCATE TABLE test_db.air_quality;

DESCRIBE air_quality;