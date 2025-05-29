import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.config import DuckLakeConnection
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType

def duckdb_to_spark_type(duckdb_type: str):
    t = duckdb_type.lower()
    if t in ("integer", "int", "int4"):
        return IntegerType()
    elif t in ("varchar", "text", "string"):
        return StringType()
    elif t in ("float", "float4", "float8", "double", "real"):
        return FloatType()
    elif t in ("boolean", "bool"):
        return BooleanType()
    else:
        return StringType()


class DuckLakeDataSource(DataSource):
    """
    A DuckLake data source for PySpark.
    Options:
    - table: table name
    """

    @classmethod
    def name(cls):
        return "ducklake"

    def schema(self):
        table_name = self.options.get("table")
        if not table_name:
            raise ValueError("The 'table' option is mandatory and must be provided.")

        con = DuckLakeConnection.get_connection()
        query = f"PRAGMA table_info('{table_name}')"
        columns = con.execute(query).fetchall()

        fields = []
        for col in columns:
            col_name = col[1]
            col_type = col[2]
            spark_type = duckdb_to_spark_type(col_type)
            fields.append(StructField(col_name, spark_type, nullable=True))

        return StructType(fields)

    def reader(self, schema: StructType):
        return DuckLakeDataSourceReader(schema, self.options)

class DuckLakeDataSourceReader(DataSourceReader):

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options

        if not self.options.get("table"):
            raise ValueError("The 'table' option is mandatory and must be provided.")

    def read(self, partition):
        table_name = self.options["table"]
        con = DuckLakeConnection.get_connection()
        query = f"SELECT * FROM {table_name}"
        result = con.execute(query)
        for row in result.fetchall():
            yield tuple(row)
