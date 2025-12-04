from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statement,
)

from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_data,
    consolidate_station_statement_data,
    consolidate_nantes_station_data,
    consolidate_nantes_station_statement_data,
)

from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data,
)


def main():
    print("Process start.")

    # -------------------------------------
    # 1) Ingestion
    # -------------------------------------
    print("Data ingestion started.")
    get_paris_realtime_bicycle_data()
    get_nantes_realtime_bicycle_data()   # Nantes en plus
    print("Data ingestion ended.")

    # -------------------------------------
    # 2) Consolidation
    # -------------------------------------
    print("Consolidation data started.")
    create_consolidate_tables()

    # Paris
    consolidate_city_data()
    consolidate_station_data()
    consolidate_station_statement_data()

    # Nantes
    consolidate_nantes_station_data()
    consolidate_nantes_station_statement_data()

    print("Consolidation data ended.")

    # -------------------------------------
    # 3) Agrégation
    # -------------------------------------
    print("Agregate data started.")
    create_agregate_tables()

    agregate_dim_city()
    agregate_dim_station()
    agregate_fact_station_statement()

    print("Agregate data ended.")
    print("Process completed successfully.")


if __name__ == "__main__":
    main()
