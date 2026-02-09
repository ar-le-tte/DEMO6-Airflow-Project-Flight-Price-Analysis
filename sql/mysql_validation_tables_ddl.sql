USE flightdb;
-- Table for VALID records
DROP TABLE IF EXISTS stg_flight_prices_valid;
CREATE TABLE stg_flight_prices_valid (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source_code VARCHAR(10),
    source_name VARCHAR(255),
    destination_code VARCHAR(10),
    destination_name VARCHAR(255),
    departure_dt DATETIME,
    arrival_dt DATETIME,
    duration_hrs DECIMAL(5,2),
    stopovers INT,
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare_bdt DECIMAL(12,2),
    tax_surcharge_bdt DECIMAL(12,2),
    total_fare_bdt DECIMAL(12,2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    ingested_at_utc        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Additional columns from validation
    computed_total_fare_bdt DECIMAL(12,2) NULL,
    fare_diff_bdt DECIMAL(12,2) NULL,
    fare_mismatch_flag TINYINT(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table for INVALID records (same schema + reasons column)
DROP TABLE IF EXISTS stg_flight_prices_invalid;
CREATE TABLE stg_flight_prices_invalid (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source_code VARCHAR(10),
    source_name VARCHAR(255),
    destination_code VARCHAR(10),
    destination_name VARCHAR(255),
    departure_dt DATETIME,
    arrival_dt DATETIME,
    duration_hrs DECIMAL(5,2),
    stopovers INT,
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare_bdt DECIMAL(12,2),
    tax_surcharge_bdt DECIMAL(12,2),
    total_fare_bdt DECIMAL(12,2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    ingested_at_utc        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    computed_total_fare_bdt DECIMAL(12,2) NULL,
    fare_diff_bdt DECIMAL(12,2) NULL,
    fare_mismatch_flag TINYINT(1) NOT NULL DEFAULT 0,
    -- Column to store validation failure reasons
    __reasons TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;