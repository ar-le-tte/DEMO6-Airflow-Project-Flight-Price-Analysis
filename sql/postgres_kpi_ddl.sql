CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.kpi_avg_fare_by_airline (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    avg_total_fare_bdt NUMERIC(12,2),
    booking_count BIGINT,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (airline, calculated_at)  
);

CREATE TABLE IF NOT EXISTS analytics.kpi_seasonal_fare_variation (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    season_type VARCHAR(20) NOT NULL, -- PEAK or NON-PEAK
    avg_total_fare_bdt NUMERIC(12,2),
    booking_count BIGINT,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (airline, season_type, calculated_at)
);

CREATE TABLE IF NOT EXISTS analytics.kpi_booking_count_by_airline (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    booking_count BIGINT,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (airline, calculated_at)
);

CREATE TABLE IF NOT EXISTS analytics.kpi_top_routes (
    id SERIAL PRIMARY KEY,
    source_code VARCHAR(50) NOT NULL,
    destination_code VARCHAR(50) NOT NULL,
    booking_count BIGINT,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- indexes
CREATE INDEX idx_avg_fare_calculated ON analytics.kpi_avg_fare_by_airline(calculated_at);
CREATE INDEX idx_seasonal_calculated ON analytics.kpi_seasonal_fare_variation(calculated_at);
CREATE INDEX idx_booking_calculated ON analytics.kpi_booking_count_by_airline(calculated_at);
CREATE INDEX idx_routes_calculated ON analytics.kpi_top_routes(calculated_at);