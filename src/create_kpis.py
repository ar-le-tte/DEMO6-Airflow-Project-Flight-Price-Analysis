import os
import psycopg2

def _pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
    )


    # ---------- 1) Avg fare by airline ----------
def create_kpi_avg_fare(source_table: str = "analytics.flight_prices_valid"):
    """Calculate average fare by airline with timestamp"""
    conn = _pg_conn()
    cur = conn.cursor()
    
    # NO TRUNCATE - just insert with timestamp
    cur.execute(f"""
        INSERT INTO analytics.kpi_avg_fare_by_airline (
            airline, avg_total_fare_bdt, booking_count, calculated_at
        )
        SELECT
            airline,
            ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare_bdt,
            COUNT(*)::bigint AS booking_count,
            NOW() AS calculated_at
        FROM {source_table}
        GROUP BY airline;
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("KPI: Average fare by airline - completed")


    # ---------- 2) Seasonal fare variation ----------
def create_kpi_seasonal(source_table: str = "analytics.flight_prices_valid"):
    """Calculate seasonal fare variation with timestamp"""
    conn = _pg_conn()
    cur = conn.cursor()
    
    cur.execute(f"""
        INSERT INTO analytics.kpi_seasonal_fare_variation (
            airline, season_type, avg_total_fare_bdt, booking_count, calculated_at
        )
        SELECT
            airline,
            CASE
                WHEN seasonality IN ('Eid', 'Winter Holidays')
                    THEN 'PEAK'
                ELSE 'NON_PEAK'
            END AS season_type,
            ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare_bdt,
            COUNT(*)::bigint AS booking_count,
            NOW() AS calculated_at
        FROM {source_table}
        GROUP BY airline, season_type;
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("KPI: Seasonal fare variation - completed")

    # ---------- 3) Booking count by airline ----------
def create_kpi_booking_count(source_table: str = "analytics.flight_prices_valid"):
    """Calculate booking count by airline with timestamp"""
    conn = _pg_conn()
    cur = conn.cursor()
    
    cur.execute(f"""
        INSERT INTO analytics.kpi_booking_count_by_airline (
            airline, booking_count, calculated_at
        )
        SELECT
            airline,
            COUNT(*)::bigint AS booking_count,
            NOW() AS calculated_at
        FROM {source_table}
        GROUP BY airline;
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("KPI: Booking count by airline - completed")
    # ---------- 4) Top routes ----------
def create_kpi_top_routes(source_table: str = "analytics.flight_prices_valid"):
    """Calculate top 10 routes by booking count with timestamp"""
    conn = _pg_conn()
    cur = conn.cursor()
    
    cur.execute(f"""
        INSERT INTO analytics.kpi_top_routes (
            source_code, destination_code, booking_count, calculated_at
        )
        SELECT
            source_code,
            destination_code,
            COUNT(*)::bigint AS booking_count,
            NOW() AS calculated_at
        FROM {source_table}
        GROUP BY source_code, destination_code
        ORDER BY booking_count DESC
        LIMIT 10;
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("KPI: Top routes - completed")