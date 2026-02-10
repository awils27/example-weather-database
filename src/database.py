import sqlite3

# Connect to the SQLite database (or create it if it doesn't exist)
connection_obj = sqlite3.connect('weather.sqlite')

# Create a cursor object to interact with the database
cursor_obj = connection_obj.cursor()

# Drop the tables if it already exists (for clean setup)
cursor_obj.execute("DROP TABLE IF EXISTS LOCATIONS")
cursor_obj.execute("DROP TABLE IF EXISTS OBSERVATIONS")

# SQL query to create the table
locations_creation_query = """
    CREATE TABLE LOCATIONS (
        LOCATION TEXT PRIMARY KEY,
        LAT REAL,
        LON REAL,
        TZ TEXT,
        OBSERVATIONS INT,
        FC3HR INT,
        FC7DAY INT,
        CREATED INT
    );
"""

observations_creation_query = """
    CREATE TABLE OBSERVATIONS (
        LOCATION STRING PRIMARY KEY,
        CREATED INT,
        DAYTIME INT,
        CONDITION TEXT,
        TEMP REAL,
        FELLS_LIKE REAL,
        DEW_POINT REAL,
        HEAT_INDEX REAL,
        WIND_CHILL REAL,
        HUMIDITY REAL,
        UV_INDEX REAL,
        PRECIPITATION_PROB REAL,
        PRECIPITATION_TYPE TEXT,
        THUNDERSTORM_PROB REAL,
        AIR_PRESSURE REAL,
        WIND_DIRECTION REAL,
        WIND_CARDINAL TEXT,
        WIND_SPEED REAL,
        WIND_GUST REAL,
        VISIBILITY REAL,
        CLOUD_COVER REAL,
        MAX_TEMP REAL,
        MIN_TEMP REAL,
        SNOW_HISTORY REAL,
        RAIN_HISTORY REAL
    );
"""

# Execute the table creation querys
cursor_obj.execute(locations_creation_query)
cursor_obj.execute(observations_creation_query)

# Confirm that the table has been created
print("Tables Ready")

# Close the connection to the database
connection_obj.close()