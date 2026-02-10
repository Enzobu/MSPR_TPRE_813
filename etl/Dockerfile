FROM apache/spark-py:v3.4.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    curl \
    libpq-dev \
    wget \
    nano && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark  \
    matplotlib  \
    requests \
    psycopg2-binary \
    python-dotenv \
    country_converter \
    kagglehub \
    pylint

WORKDIR /app

RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    mkdir -p /opt/spark/jars && \
    mv postgresql-42.6.0.jar /opt/spark/jars/

RUN echo "alias ll='ls -al'" >> /root/.bashrc && \
    echo "export PATH=$PATH:/app/bin/" >> /root/.bashrc && \
    echo "export PYTHONPATH=/app" >> /root/.bashrc && \
    echo "SPARK_HOME=/opt/spark/" >> /root/.bashrc

CMD ["tail", "-f", "/dev/null"]