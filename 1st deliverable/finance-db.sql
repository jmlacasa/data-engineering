-- Source Types Table
-- Similar to asset_types.
CREATE TABLE source_types (
    type_id INT IDENTITY(1, 1) PRIMARY KEY,
    type_name VARCHAR(255) NOT NULL UNIQUE
)
DISTSTYLE ALL;

-- Assets Table
-- As this table will be a central table for joins,
-- distributing it by pair_id seems logical.
CREATE TABLE assets (
    asset_id INT IDENTITY(1, 1) PRIMARY KEY,
    asset_name VARCHAR(255) NOT NULL UNIQUE
)
DISTSTYLE KEY
DISTKEY(asset_id);

-- Data Sources Table
-- This table will likely be small and infrequently joined, 
-- so DISTSTYLE ALL can be used.
CREATE TABLE data_sources (
    source_id INT IDENTITY(1, 1) PRIMARY KEY,
    source_name VARCHAR(255) NOT NULL UNIQUE,
    type_id INT REFERENCES source_types(type_id)
)
DISTSTYLE ALL;

-- OHLCV Data Table
-- pair_id declared as DISTKEY because we will often join by this column.
-- timestamp as SORTKEY since time-based queries will be frequent.


CREATE TABLE ohlcv_data (
    data_id INT IDENTITY(1, 1) PRIMARY KEY,
    asset_id INT REFERENCES assets(asset_id),
    source_id INT REFERENCES data_sources(source_id),
    "timestamp" TIMESTAMP NOT NULL,
    "open" DECIMAL(20,10) NOT NULL,
    high DECIMAL(20,10) NOT NULL,
    low DECIMAL(20,10) NOT NULL,
    "close" DECIMAL(20,10) NOT NULL,
    adj_close DECIMAL(20,10) NOT NULL,
    volume DECIMAL(20,10) NOT NULL
)
DISTSTYLE KEY
DISTKEY(pair_id)
SORTKEY("timestamp");


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
