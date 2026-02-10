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

def create_kpis(
    source_table: str = "analytics.flight_prices_valid",
):
    conn = _pg_conn()
    cur = conn.cursor()

    # ---------- refresh tables ----------
    cur.execute("TRUNCATE analytics.kpi_avg_fare_by_airline;")
    cur.execute("TRUNCATE analytics.kpi_seasonal_fare_variation;")
    cur.execute("TRUNCATE analytics.kpi_booking_count_by_airline;")
    cur.execute("TRUNCATE analytics.kpi_top_routes;")

    # ---------- 1) Avg fare by airline ----------
    cur.execute(f"""
        INSERT INTO analytics.kpi_avg_fare_by_airline (
            airline, avg_total_fare_bdt, booking_count
        )
        SELECT
            airline,
            ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare_bdt,
            COUNT(*)::bigint AS booking_count
        FROM {source_table}
        GROUP BY airline;
    """)

    # ---------- 2) Seasonal fare variation ----------
    cur.execute(f"""
        INSERT INTO analytics.kpi_seasonal_fare_variation (
            airline, season_type, avg_total_fare_bdt, booking_count
        )
        SELECT
            airline,
            CASE
                WHEN seasonality IN ('Eid', 'Winter Holidays')
                    THEN 'PEAK'
                ELSE 'NON_PEAK'
            END AS season_type,
            ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare_bdt,
            COUNT(*)::bigint AS booking_count
        FROM {source_table}
        GROUP BY airline, season_type;
    """)

    # ---------- 3) Booking count by airline ----------
    cur.execute(f"""
        INSERT INTO analytics.kpi_booking_count_by_airline (
            airline, booking_count
        )
        SELECT
            airline,
            COUNT(*)::bigint AS booking_count
        FROM {source_table}
        GROUP BY airline;
    """)

    # ---------- 4) Top routes ----------
    cur.execute(f"""
        INSERT INTO analytics.kpi_top_routes (
            source_code, destination_code, booking_count
        )
        SELECT
            source_code,
            destination_code,
            COUNT(*)::bigint AS booking_count
        FROM {source_table}
        GROUP BY source_code, destination_code
        ORDER BY booking_count DESC
        LIMIT 10;
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("KPI tables populated successfully.")