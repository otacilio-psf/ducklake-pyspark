import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.config import DuckLakeConnection

def load_sample_data():
    con = DuckLakeConnection.get_connection()
    con.execute("""
        DROP TABLE IF EXISTS nl_train_stations;
        CREATE TABLE nl_train_stations AS
        FROM 'https://blobs.duckdb.org/nl_stations.csv';
    """)

load_sample_data()