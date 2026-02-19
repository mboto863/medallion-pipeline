-- Setup Unity Catalog environment

USE CATALOG demo_mboto;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze = raw ingestion
-- Silver = cleaned and conformed
-- Gold = business aggregates

USE SCHEMA bronze;
CREATE VOLUME IF NOT EXISTS raw_orders;
CREATE VOLUME IF NOT EXISTS checkpoints;