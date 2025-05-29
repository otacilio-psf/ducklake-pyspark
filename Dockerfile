FROM spark:python3

USER root

RUN pip install --no-cache-dir pyarrow duckdb

RUN mkdir -p /home/spark \
    && chown -R spark:spark /home/spark \
    && chmod 700 /home/spark

USER spark

WORKDIR /opt/spark/work-dir
