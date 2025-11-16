import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os

SUPABASE_DB_USER = os.environ["SUPABASE_DB_USER"]
SUPABASE_DB_PASSWORD = os.environ["SUPABASE_DB_PASSWORD"]
SUPABASE_DB_HOST = os.environ["SUPABASE_DB_HOST"]
SUPABASE_DB_PORT = os.environ.get("SUPABASE_DB_PORT", "5432")
SUPABASE_DB_NAME = os.environ.get("SUPABASE_DB_NAME", "postgres")

# 1. Build a safe connection URL object
url_object = URL.create(
    drivername="postgresql+psycopg2",
    username=SUPABASE_DB_USER,
    password=SUPABASE_DB_PASSWORD,
    host=SUPABASE_DB_HOST,
    port=int(SUPABASE_DB_PORT),
    database=SUPABASE_DB_NAME,
    query={"sslmode": "require"},  # Supabase needs SSL
)

# create_engine() is how SQLAlchemy builds a connection “engine” — an object that knows how to talk to your database (PostgreSQL in this case).
# You can think of it as a bridge between Python and your database.
engine = create_engine(url_object)

# 2. Read your CSV into a DataFrame
df = pd.read_csv("dirk_data.csv")  # path to your CSV file

# 3. Upload DataFrame to Supabase (Postgres)
# if_exists="append" → add rows
# if_exists="replace" → drop + recreate table (be careful!)
df.to_sql("dirk_data", engine, if_exists="append", index=False)

