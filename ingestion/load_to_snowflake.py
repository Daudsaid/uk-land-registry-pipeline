import snowflake.connector
import pandas as pd
import glob
import os
from snowflake.connector.pandas_tools import write_pandas

conn = snowflake.connector.connect(
    account='KDFGNXS-SA64515',
    user='DAU',
    password=os.environ['SNOWFLAKE_PASSWORD'],
    role='ACCOUNTADMIN',
    warehouse='COMPUTE_WH',
    database='UK_LAND_REGISTRY',
    schema='RAW'
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS RAW.PRICE_PAID (
    TRANSACTION_ID VARCHAR,
    PRICE_PAID INTEGER,
    TRANSFER_DATE DATE,
    POSTCODE VARCHAR,
    PROPERTY_TYPE VARCHAR,
    OLD_NEW VARCHAR,
    DURATION VARCHAR,
    PAON VARCHAR,
    SAON VARCHAR,
    STREET VARCHAR,
    LOCALITY VARCHAR,
    TOWN VARCHAR,
    DISTRICT VARCHAR,
    COUNTY VARCHAR,
    TRANSACTION_CAT VARCHAR,
    RECORD_STATUS VARCHAR
)
""")

columns = [
    'TRANSACTION_ID', 'PRICE_PAID', 'TRANSFER_DATE', 'POSTCODE',
    'PROPERTY_TYPE', 'OLD_NEW', 'DURATION', 'PAON', 'SAON',
    'STREET', 'LOCALITY', 'TOWN', 'DISTRICT', 'COUNTY',
    'TRANSACTION_CAT', 'RECORD_STATUS'
]

files = sorted(glob.glob(os.path.expanduser('~/pp-20*.csv')))

for file in files:
    print(f"Loading {file}...")
    df = pd.read_csv(file, header=None, names=columns)
    df['PRICE_PAID'] = pd.to_numeric(df['PRICE_PAID'], errors='coerce')
    df['TRANSFER_DATE'] = pd.to_datetime(df['TRANSFER_DATE'], errors='coerce').dt.date
    success, nchunks, nrows, _ = write_pandas(conn, df, 'PRICE_PAID', schema='RAW')
    print(f"  Loaded {nrows} rows from {os.path.basename(file)}")

cursor.close()
conn.close()
print("Done!")
