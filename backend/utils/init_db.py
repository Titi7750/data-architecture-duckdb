import os
import sys
import duckdb
import pandas as pd
from auth import hash_password

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

database_path = os.path.join(os.path.dirname(__file__), '..', 'city_bike.duckdb')
connection_database = duckdb.connect(database_path)

df_trip = pd.read_csv("./datasets/trip.csv", sep=",", on_bad_lines='skip')
df_station = pd.read_csv("./datasets/station.csv", sep=",")
df_weather = pd.read_csv("./datasets/weather.csv", sep=",")

connection_database.register("trip_df", df_trip)
connection_database.register("station_df", df_station)
connection_database.register("weather_df", df_weather)

connection_database.execute("""
    CREATE TABLE IF NOT EXISTS trip
    AS SELECT * FROM trip_df
""")
connection_database.execute("""
    CREATE TABLE IF NOT EXISTS station
    AS SELECT * FROM station_df
""")
connection_database.execute("""
    CREATE TABLE IF NOT EXISTS weather
    AS SELECT * FROM weather_df
""")
connection_database.execute("""
    CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password TEXT)
""")

hashed = hash_password("administrator")

connection_database.execute("INSERT INTO users VALUES (?, ?)", ("admin", hashed))

connection_database.commit()
print("La base a bien été initialisée avec utilisateur admin / administrator")

connection_database.close()