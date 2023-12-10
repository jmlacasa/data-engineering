CREATE SCHEMA deliverable3;
SET search_path TO deliverable3;

-- Source Types Table
-- Similar to asset_types.
CREATE TABLE source_types (
    type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(255) NOT NULL UNIQUE
)
;

-- Assets Table
-- As this table will be a central table for joins,
-- distributing it by pair_id seems logical.
CREATE TABLE assets (
    asset_id SERIAL PRIMARY KEY,
    asset_name VARCHAR(255) NOT NULL UNIQUE
)
;

-- Data Sources Table
-- This table will likely be small and infrequently joined, 
-- so DISTSTYLE ALL can be used.
CREATE TABLE data_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(255) NOT NULL UNIQUE,
    type_id INT REFERENCES source_types(type_id)
)
;

-- OHLCV Data Table
-- pair_id declared as DISTKEY because we will often join by this column.
-- timestamp as SORTKEY since time-based queries will be frequent.


CREATE TABLE ohlcv_data (
    asset_id INT REFERENCES assets(asset_id),
    source_id INT REFERENCES data_sources(source_id),
    ts TIMESTAMP NOT NULL,
    "open" DECIMAL(20,10) NOT NULL,
    high DECIMAL(20,10) NOT NULL,
    low DECIMAL(20,10) NOT NULL,
    "close" DECIMAL(20,10) NOT NULL,
    adj_close DECIMAL(20,10) NOT NULL,
    volume DECIMAL(20,10) NOT null,
    PRIMARY KEY (asset_id, source_id, ts)
)
;

CREATE TABLE staging_ohlcv_data (
    asset_id INT REFERENCES assets(asset_id),
    source_id INT REFERENCES data_sources(source_id),
    ts TIMESTAMP NOT NULL,
    "open" DECIMAL(20,10) NOT NULL,
    high DECIMAL(20,10) NOT NULL,
    low DECIMAL(20,10) NOT NULL,
    "close" DECIMAL(20,10) NOT NULL,
    adj_close DECIMAL(20,10) NOT NULL,
    volume DECIMAL(20,10) NOT NULL,
    PRIMARY KEY (asset_id, source_id, ts)
)
;

-- UPDATE ohlcv_data
-- SET composite_id = CAST("asset_id" AS VARCHAR) || CAST("source_id" AS VARCHAR) || CAST("ts" AS VARCHAR);

-- UPDATE staging_ohlcv_data
-- SET composite_id = CAST("asset_id" AS VARCHAR) || CAST("source_id" AS VARCHAR) || CAST("ts" AS VARCHAR);


-- Inserting Source Types
INSERT INTO source_types (type_name) VALUES
('Broker'),
('Platform'),
('Exchange'),
('Bank'),
('Research Firm'),
('Regulatory Body'),
('News Agency'),
('Third-party API'),
('Other');
