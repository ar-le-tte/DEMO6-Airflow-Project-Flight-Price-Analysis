CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.kpi_avg_fare_by_airline (
  airline VARCHAR(100) PRIMARY KEY,
  avg_total_fare_bdt NUMERIC(12,2),
  booking_count BIGINT
);

CREATE TABLE IF NOT EXISTS analytics.kpi_seasonal_fare_variation (
  airline VARCHAR(100) NOT NULL,
  season_type VARCHAR(20) NOT NULL,   -- 'PEAK' or 'NON_PEAK'
  avg_total_fare_bdt NUMERIC(12,2),
  booking_count BIGINT,
  PRIMARY KEY (airline, season_type)
);

CREATE TABLE IF NOT EXISTS analytics.kpi_booking_count_by_airline (
  airline VARCHAR(100) PRIMARY KEY,
  booking_count BIGINT
);

CREATE TABLE IF NOT EXISTS analytics.kpi_top_routes (
  source_code VARCHAR(50) NOT NULL,
  destination_code VARCHAR(50) NOT NULL,
  booking_count BIGINT,
  PRIMARY KEY (source_code, destination_code)
);
