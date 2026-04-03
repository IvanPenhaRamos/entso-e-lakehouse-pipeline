FROM apache/superset:4.1.3
ENV TRINO_PYTHON_CLIENT_VERSION=0.337.0
USER superset
RUN pip install --no-cache-dir trino[sqlalchemy]==$TRINO_PYTHON_CLIENT_VERSION psycopg2-binary
