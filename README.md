# DuckLake + PySpark 4 Demo

This repository demonstrates how to use the new [**DuckLake**](https://ducklake.select/) table format with [**Spark 4 - Python Data Source API**](https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/python_data_source.html) feature.

DuckLake is a newly released table format from DuckDB, designed to compete with Delta Lake and Apache Iceberg. PySpark 4 introduces support for Python-based data sources, which allows Spark to directly integrate with libraries like DuckDB using Python APIs.

In this demo:

* We create a DuckLake table using `duckdb` and `ducklake`
* Then read it using PySpark 4, via a custom Python data source
* Both **SQLite** and **PostgreSQL** metadata backends are supported for the DuckLake catalog

> 💡 Although you could read the Parquet data directly based on the ducklake backend db, this example is designed to **showcase the capabilities of Python data sources in PySpark 4**, which allow direct integration with non-JVM libraries.

---

## 🐍 Build the PySpark 4 Docker Image

```bash
docker build -t ducklake-pyspark .
```

---

## 🔌 Using SQLite as the Catalog Backend

### Set Environment Variables

```bash
export DUCKLAKE_DATA_PATH=data/ducklake/
export DUCKLAKE_BACKEND=sqlite
export SQLITE_METADATA_PATH=data/ducklake_metadata.sqlite
```

### Create DuckLake Table

```bash
uv run src/ducklake_loader.py
```

### Read with PySpark 4

```bash
docker run -it --rm \
    -v $(pwd):/opt/spark/work-dir \
    -e DUCKLAKE_DATA_PATH=data/ducklake/ \
    -e DUCKLAKE_BACKEND=sqlite \
    -e SQLITE_METADATA_PATH=data/ducklake_metadata.sqlite \
    ducklake-pyspark \
    /opt/spark/bin/spark-submit /opt/spark/work-dir/src/spark_read_ducklake.py
```

---

## 🐘 Using PostgreSQL as the Catalog Backend

### Set Environment Variables

```bash
export DUCKLAKE_DATA_PATH=data/ducklake/
export DUCKLAKE_BACKEND=postgres
export PGUSER=ducklake_user
export PGPASSWORD=supersecret
export PGDATABASE=ducklake_catalog
```

### Start PostgreSQL

```bash
docker compose up -d
```

### Create DuckLake Table

```bash
uv run src/ducklake_loader.py
```

### Read with PySpark 4

```bash
docker run -it --rm \
    -v $(pwd):/opt/spark/work-dir \
    --network ducklake-pyspark_ducklake_net \
    -e DUCKLAKE_DATA_PATH=data/ducklake/ \
    -e DUCKLAKE_BACKEND=postgres \
    -e PGUSER=ducklake_user \
    -e PGPASSWORD=supersecret \
    -e PGDATABASE=ducklake_catalog \
    -e PGHOST=postgres \
    ducklake-pyspark \
    /opt/spark/bin/spark-submit /opt/spark/work-dir/src/spark_read_ducklake.py
```

---

## 📝 Notes

* This project uses [uv](https://docs.astral.sh/uv/) to manage Python packages and virtual environments
* The setup and scripts were developed and tested on Linux. You may encounter compatibility issues when running on Windows (e.g., file paths, Docker volume mounts, or environment variable handling).
* DuckLake stores data in Parquet and metadata in either SQLite or PostgreSQL
* This example abstracts the metadata backend selection via environment variables
* The Spark reader is implemented using PySpark’s new Python data source api
* This is a **prototype/demo**—not intended for production use
