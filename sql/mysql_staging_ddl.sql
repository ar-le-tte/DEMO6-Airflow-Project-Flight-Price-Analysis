CREATE DATABASE IF NOT EXISTS flightdb;
USE flightdb;

CREATE TABLE IF NOT EXISTS stg_flight_prices (
  id INT AUTO_INCREMENT PRIMARY KEY,
  airline                VARCHAR(100)  NOT NULL,
  source_code            VARCHAR(20)   NOT NULL,
  source_name            VARCHAR(255)  NULL,
  destination_code       VARCHAR(20)   NOT NULL,
  destination_name       VARCHAR(255)  NULL,
  departure_dt           DATETIME      NOT NULL,
  arrival_dt             DATETIME      NOT NULL,
  duration_hrs           DOUBLE        NULL,
  stopovers              VARCHAR(50)   NULL,
  aircraft_type          VARCHAR(100)  NULL,
  travel_class           VARCHAR(50)   NULL,
  booking_source         VARCHAR(100)  NULL,
  base_fare_bdt          DOUBLE        NULL,
  tax_surcharge_bdt      DOUBLE        NULL,
  total_fare_bdt         DOUBLE        NULL,
  seasonality            VARCHAR(100)  NULL,
  days_before_departure  INT           NULL,

  -- helper column
  ingested_at_utc        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  -- Unique constraint on duplicate check columns
  UNIQUE KEY uk_flight_unique (source_code, destination_code, departure_dt),

  INDEX idx_route (source_code, destination_code),
  INDEX idx_airline (airline),
  INDEX idx_departure (departure_dt)
);
