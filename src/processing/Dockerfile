FROM flink:2.0.0

WORKDIR /app

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar

RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

COPY src/processing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt 
COPY src/processing/flink_job.py .

CMD ["python3", "flink_job.py"]