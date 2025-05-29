# Build pyspark image

```bash
docker build -t ducklake-pyspark . 
```

# SQLITE

```bash
export DUCKLAKE_DATA_PATH=data/ducklake/
export DUCKLAKE_BACKEND=sqlite
export SQLITE_METADATA_PATH=data/ducklake_metadata.sqlite
```

```bash
uv run src/ducklake_loader.py 
```

```bash
docker run -it --rm \
    -v $(pwd):/opt/spark/work-dir \
    -e DUCKLAKE_DATA_PATH=data/ducklake/ \
    -e DUCKLAKE_BACKEND=sqlite \
    -e SQLITE_METADATA_PATH=data/ducklake_metadata.sqlite \
    ducklake-pyspark \
    /opt/spark/bin/spark-submit /opt/spark/work-dir/src/spark_read_ducklake.py
```

# POSTGRES

```bash
export DUCKLAKE_DATA_PATH=data/ducklake/
export DUCKLAKE_BACKEND=postgres
export PGUSER=ducklake_user
export PGPASSWORD=supersecret
export PGDATABASE=ducklake_catalog
```

```bash
docker compose up -d
```

```bash
uv run src/ducklake_loader.py 
```

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