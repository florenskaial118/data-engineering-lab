CREATE DATABASE IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.product;
CREATE EXTERNAL TABLE raw.product (
    maker STRING,
    model INT,
    type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','  
STORED AS TEXTFILE     
LOCATION '/warehouse/raw/product';


DROP TABLE IF EXISTS raw.pc;
CREATE EXTERNAL TABLE raw.pc (
    model INT,
    speed INT,
    ram INT,
    hd INT,
    price INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','  
STORED AS TEXTFILE     
LOCATION '/warehouse/raw/pc';

CREATE DATABASE IF NOT EXISTS ods;

DROP TABLE IF EXISTS ods.product;
CREATE EXTERNAL TABLE ods.product (
    maker STRING,
    model INT
)
PARTITIONED BY (type STRING)
STORED AS PARQUET
LOCATION '/warehouse/ods/product';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE ods.product
PARTITION (type)
SELECT
    maker,
    model,
    type
FROM raw.product;

SHOW TABLES;

SELECT *
FROM raw.product;

SELECT *
FROM raw.pc;

SELECT maker, count(DISTINCT pc.model) as pc_models_count, avg(price) as avg_price,
    min(price) as min_price, max(price) as max_price
from raw.product p join raw.pc pc on p.model = pc.model
group by p.maker;