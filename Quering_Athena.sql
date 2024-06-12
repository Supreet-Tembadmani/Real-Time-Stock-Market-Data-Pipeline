CREATE DATABASE stock_data_db;

CREATE EXTERNAL TABLE stock_data_db.stock_data (
    `timestamp` string,
    `close` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://your-s3-bucket/processed_data/';
