import os
import sqlite3
import requests
from datetime import datetime, timezone

WEATHER_URL = "https://weather.googleapis.com/v1/currentConditions:lookup"
API_KEY = os.environ["KEY"]

def get_in(d, path, default=None):
    """Safely fetch nested dict values: get_in(data, ["a","b","c"])."""
    cur = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur

def rfc3339_to_epoch_seconds(ts: str) -> int:
    """
    Convert Google RFC3339 timestamp (often with nanoseconds + 'Z') to epoch seconds.

    Example: "2026-02-10T01:21:26.127548894Z"
    Python supports up to microseconds, so we truncate fractional seconds to 6 digits.
    """
    if ts.endswith("Z"):
        ts = ts[:-1]  # drop Z, we'll set tz=UTC below
        tz = timezone.utc
    else:
        tz = None

    if "." in ts:
        main, frac = ts.split(".", 1)
        # keep only digits for fraction, truncate/pad to 6 (microseconds)
        frac_digits = "".join(ch for ch in frac if ch.isdigit())
        frac6 = (frac_digits + "000000")[:6]
        ts = f"{main}.{frac6}"

    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz or timezone.utc)
    return int(dt.timestamp())

with sqlite3.connect("weather.sqlite") as conn:
    cur = conn.cursor()

    # Pull your known locations (adjust column names if different)
    cur.execute("SELECT LOCATION, LAT, LON FROM LOCATIONS WHERE OBSERVATIONS = 1")
    locations = cur.fetchall()

    session = requests.Session()

    # UPSERT: one row per LOCATION (LOCATION must be PRIMARY KEY or UNIQUE)
    # The WHERE clause ensures we only update if the incoming reading is newer.
    upsert_sql = """
    INSERT INTO OBSERVATIONS (
        LOCATION, CREATED, DAYTIME, CONDITION,
        TEMP, FELLS_LIKE, DEW_POINT, HEAT_INDEX, WIND_CHILL,
        HUMIDITY, UV_INDEX,
        PRECIPITATION_PROB, PRECIPITATION_TYPE,
        THUNDERSTORM_PROB, AIR_PRESSURE,
        WIND_DIRECTION, WIND_CARDINAL, WIND_SPEED, WIND_GUST,
        VISIBILITY, CLOUD_COVER,
        MAX_TEMP, MIN_TEMP, SNOW_HISTORY, RAIN_HISTORY
    ) VALUES (
        ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?
    )
    ON CONFLICT(LOCATION) DO UPDATE SET
        CREATED              = excluded.CREATED,
        DAYTIME              = excluded.DAYTIME,
        CONDITION            = excluded.CONDITION,
        TEMP                 = excluded.TEMP,
        FELLS_LIKE           = excluded.FELLS_LIKE,
        DEW_POINT            = excluded.DEW_POINT,
        HEAT_INDEX           = excluded.HEAT_INDEX,
        WIND_CHILL           = excluded.WIND_CHILL,
        HUMIDITY             = excluded.HUMIDITY,
        UV_INDEX             = excluded.UV_INDEX,
        PRECIPITATION_PROB   = excluded.PRECIPITATION_PROB,
        PRECIPITATION_TYPE   = excluded.PRECIPITATION_TYPE,
        THUNDERSTORM_PROB    = excluded.THUNDERSTORM_PROB,
        AIR_PRESSURE         = excluded.AIR_PRESSURE,
        WIND_DIRECTION       = excluded.WIND_DIRECTION,
        WIND_CARDINAL        = excluded.WIND_CARDINAL,
        WIND_SPEED           = excluded.WIND_SPEED,
        WIND_GUST            = excluded.WIND_GUST,
        VISIBILITY           = excluded.VISIBILITY,
        CLOUD_COVER          = excluded.CLOUD_COVER,
        MAX_TEMP             = excluded.MAX_TEMP,
        MIN_TEMP             = excluded.MIN_TEMP,
        SNOW_HISTORY         = excluded.SNOW_HISTORY,
        RAIN_HISTORY         = excluded.RAIN_HISTORY
    WHERE excluded.CREATED >= OBSERVATIONS.CREATED
    """

    for (location_name, lat, lon) in locations:
        # Use requests params instead of string building (handles encoding safely)
        params = {
            "key": API_KEY,
            "location.latitude": lat,
            "location.longitude": lon,
        }

        resp = session.get(WEATHER_URL, params=params, timeout=15)  # endpoint + params per docs :contentReference[oaicite:2]{index=2}
        if resp.status_code != 200:
            print(f"[{location_name}] HTTP {resp.status_code}: {resp.text[:200]}")
            continue

        data = resp.json()

        current_time = data.get("currentTime")
        if not current_time:
            print(f"[{location_name}] Missing currentTime in response")
            continue

        created = rfc3339_to_epoch_seconds(current_time)

        # Note: precipitation probability fields are in precipitation.probability.{percent,type} :contentReference[oaicite:3]{index=3}
        row = (
            location_name,
            created,
            1 if data.get("isDaytime") else 0,
            get_in(data, ["weatherCondition", "type"]) or get_in(data, ["weatherCondition", "description", "text"]),
            get_in(data, ["temperature", "degrees"]),
            get_in(data, ["feelsLikeTemperature", "degrees"]),
            get_in(data, ["dewPoint", "degrees"]),
            get_in(data, ["heatIndex", "degrees"]),
            get_in(data, ["windChill", "degrees"]),
            data.get("relativeHumidity"),
            data.get("uvIndex"),
            get_in(data, ["precipitation", "probability", "percent"]),
            get_in(data, ["precipitation", "probability", "type"]),
            data.get("thunderstormProbability"),
            get_in(data, ["airPressure", "meanSeaLevelMillibars"]),
            get_in(data, ["wind", "direction", "degrees"]),
            get_in(data, ["wind", "direction", "cardinal"]),
            get_in(data, ["wind", "speed", "value"]),
            get_in(data, ["wind", "gust", "value"]),
            get_in(data, ["visibility", "distance"]),
            data.get("cloudCover"),
            get_in(data, ["currentConditionsHistory", "maxTemperature", "degrees"]),
            get_in(data, ["currentConditionsHistory", "minTemperature", "degrees"]),
            get_in(data, ["currentConditionsHistory", "snowQpf", "quantity"]),
            get_in(data, ["currentConditionsHistory", "qpf", "quantity"]),
        )

        cur.execute(upsert_sql, row)
