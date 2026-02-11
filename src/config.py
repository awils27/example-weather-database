import json
import sqlite3
import time

def quote_ident(name: str) -> str:
    """
    Safely quote an SQL identifier (e.g., a column name).

    We only allow alphanumeric + underscore names to avoid SQL injection when
    building SQL strings that include identifiers (you *can't* parameterize
    identifiers with ? placeholders).
    """
    if not all(c.isalnum() or c == "_" for c in name):
        raise ValueError(f"Bad column name: {name!r}")
    return f'"{name}"'  # double quotes are the standard SQLite identifier quotes


# Connect to the database. Using `with` ensures the connection is committed
with sqlite3.connect("weather.sqlite") as conn:
    cur = conn.cursor()

    # Load the JSON config that contains your LOCATIONS array.
    with open("config.json", "r", encoding="utf-8") as f:
        locationDict = json.load(f)

    # Loop over each location object in the JSON.
    for idx, row in enumerate(locationDict["LOCATIONS"]):
        # Grab the JSON keys (these should match your table column names).
        cols = list(row.keys())

        # Columns we will insert: all JSON columns + an UPDATED_AT timestamp.
        insert_cols = cols + ["UPDATED"]

        # Build the INSERT column list and matching placeholders:
        insert_cols_sql = ", ".join(quote_ident(c) for c in insert_cols)
        placeholders = ", ".join(["?"] * len(insert_cols))

        # Columns we will update if a conflict occurs.
        update_cols = cols + ["UPDATED"]
        update_sql = ", ".join(
            f'{quote_ident(c)} = excluded.{quote_ident(c)}' for c in update_cols
        )

        # UPSERT:
        sql = f"""
        INSERT INTO LOCATIONS ({insert_cols_sql})
        VALUES ({placeholders})
        ON CONFLICT(LOCATION) DO UPDATE SET
          {update_sql}
        """

        # Build the parameter list in the same order as `insert_cols`.
        params = [row[c] for c in cols] + [time.time()]

        # Execute the UPSERT for this row.
        cur.execute(sql, params)
