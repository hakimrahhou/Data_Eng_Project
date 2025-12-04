import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")


# --------------------------------------------------------
# 1) CREATE CONSOLIDATE TABLES
# --------------------------------------------------------
def create_consolidate_tables():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            if statement.strip():
                print(statement)
                con.execute(statement)


# --------------------------------------------------------
# 2) CONSOLIDATE CITY (PARIS + AJOUTER NANTES)
# --------------------------------------------------------
def consolidate_city_data():

    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Charger les données Paris
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None  # pas besoin des habitants

    # Extraire communes depuis Paris
    city_data_df = raw_data_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]]

    city_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)

    city_data_df.drop_duplicates(inplace=True)

    # 🔵 Ajouter Nantes manuellement (nécessaire car pas dans l’API Vélib)
    nantes_row = {
        "id": "44109",         # code INSEE Nantes
        "name": "Nantes",
        "nb_inhabitants": None
    }
    city_data_df = pd.concat(
        [city_data_df, pd.DataFrame([nantes_row])],
        ignore_index=True
    ).drop_duplicates(subset=["id"])

    city_data_df["created_date"] = date.today()

    print("\n---- City DF preview ----")
    print(city_data_df.head())

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")


# --------------------------------------------------------
# 3) CONSOLIDATE STATION PARIS
# --------------------------------------------------------
def consolidate_station_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    df = pd.json_normalize(data)

    station_df = pd.DataFrame()
    station_df["ID"] = df["stationcode"]
    station_df["CODE"] = df["stationcode"]
    station_df["NAME"] = df["name"]
    station_df["CITY_NAME"] = df["nom_arrondissement_communes"]
    station_df["CITY_CODE"] = df["code_insee_commune"]

    station_df["ADDRESS"] = df["address"] if "address" in df.columns else None
    station_df["LONGITUDE"] = df["coordonnees_geo.lon"]
    station_df["LATITUDE"] = df["coordonnees_geo.lat"]
    station_df["STATUS"] = df["station_state"] if "station_state" in df.columns else None
    station_df["CREATED_DATE"] = date.today()
    station_df["CAPACITTY"] = df["capacity"]

    print("\n---- Paris Station DF preview ----")
    print(station_df.head())

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_df;")


# --------------------------------------------------------
# 4) CONSOLIDATE STATION STATEMENT PARIS
# --------------------------------------------------------
def consolidate_station_statement_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    df = pd.json_normalize(data)

    statement_df = pd.DataFrame()
    statement_df["STATION_ID"] = df["stationcode"]

    statement_df["BICYCLE_DOCKS_AVAILABLE"] = df["numdocksavailable"]
    statement_df["BICYCLE_AVAILABLE"] = df["numbikesavailable"]

    if "record_timestamp" in df.columns:
        statement_df["LAST_STATEMENT_DATE"] = pd.to_datetime(df["record_timestamp"]).dt.date
    else:
        statement_df["LAST_STATEMENT_DATE"] = date.today()

    statement_df["CREATED_DATE"] = date.today()

    print("\n---- Paris Station STATEMENT DF preview ----")
    print(statement_df.head())

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM statement_df;")


# --------------------------------------------------------
# 5) CONSOLIDATE NANTES STATIONS
# --------------------------------------------------------
def consolidate_nantes_station_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    df = pd.json_normalize(data["results"])

    station_df = pd.DataFrame()
    station_df["ID"] = df["number"].astype(str)
    station_df["CODE"] = station_df["ID"]
    station_df["NAME"] = df["name"]

    station_df["CITY_NAME"] = "Nantes"
    station_df["CITY_CODE"] = "44109"   # code INSEE Nantes

    station_df["ADDRESS"] = df.get("address", None)
    station_df["LONGITUDE"] = df["position.lon"]
    station_df["LATITUDE"] = df["position.lat"]
    station_df["STATUS"] = df.get("status", None)
    station_df["CREATED_DATE"] = date.today()
    station_df["CAPACITTY"] = df["bike_stands"]

    print("\n---- Nantes Station DF preview ----")
    print(station_df.head())

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_df;")


# --------------------------------------------------------
# 6) CONSOLIDATE NANTES STATION STATEMENTS
# --------------------------------------------------------
def consolidate_nantes_station_statement_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    df = pd.json_normalize(data["results"])

    statement_df = pd.DataFrame()
    statement_df["STATION_ID"] = df["number"].astype(str)
    statement_df["BICYCLE_DOCKS_AVAILABLE"] = df["available_bike_stands"]
    statement_df["BICYCLE_AVAILABLE"] = df["available_bikes"]
    statement_df["LAST_STATEMENT_DATE"] = pd.to_datetime(df["last_update"]).dt.date
    statement_df["CREATED_DATE"] = date.today()

    print("\n---- Nantes Station STATEMENT DF preview ----")
    print(statement_df.head())

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM statement_df;")
