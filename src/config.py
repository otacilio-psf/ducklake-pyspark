import os
import duckdb

# Duck Lake backend: either "sqlite" or "postgres"
DUCKLAKE_BACKEND = os.getenv("DUCKLAKE_BACKEND", "sqlite").lower()

DUCKLAKE_DATA_PATH = os.getenv("DUCKLAKE_DATA_PATH", "data/ducklake/")

SQLITE_METADATA_PATH = os.getenv("SQLITE_METADATA_PATH", "data/ducklake_metadata.sqlite")

PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE")


def get_ducklake_uri() -> str:
    """
    Returns the Duck Lake ATTACH URI based on selected backend.
    """
    if DUCKLAKE_BACKEND == "sqlite":
        return f"ducklake:sqlite:{SQLITE_METADATA_PATH}"
    elif DUCKLAKE_BACKEND == "postgres":
        if not all([PGUSER, PGPASSWORD, PGDATABASE]):
            raise ValueError("Missing required PostgreSQL environment variables.")
        return f"ducklake:postgres:dbname={PGDATABASE} host={PGHOST}"
    else:
        raise ValueError(f"Unsupported DUCKLAKE_BACKEND: '{DUCKLAKE_BACKEND}'")


class DuckLakeConnection:
    _instance = None

    def __init__(self, alias: str = "my_ducklake"):
        if DuckLakeConnection._instance is not None:
            raise Exception("Use get_connection() instead")
        self.con = duckdb.connect()
        self._load_extensions()
        self._attach_ducklake(alias)

    def _load_extensions(self):
        self.con.execute("INSTALL ducklake; LOAD ducklake;")
        if DUCKLAKE_BACKEND == "sqlite":
            self.con.execute("INSTALL sqlite; LOAD sqlite;")
        elif DUCKLAKE_BACKEND == "postgres":
            self.con.execute("INSTALL postgres; LOAD postgres;")
        else:
            raise ValueError(f"Unsupported DUCKLAKE_BACKEND: '{DUCKLAKE_BACKEND}'")

    def _attach_ducklake(self, alias):
        uri = get_ducklake_uri()
        self.con.execute(f"""
            ATTACH '{uri}' AS {alias} (DATA_PATH '{DUCKLAKE_DATA_PATH}');
            USE {alias};
        """)

    @staticmethod
    def get_connection():
        if DuckLakeConnection._instance is None:
            DuckLakeConnection._instance = DuckLakeConnection()
        return DuckLakeConnection._instance.con