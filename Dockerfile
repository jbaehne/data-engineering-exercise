# in the production case there should be seperate docker images for each process.
# for the execsise, I am putting everything onto one image
FROM apache/spark:3.5.0

ARG ICEBERG_VERSION=1.4.3
ARG SCALA_VERSION=2.12
ARG SPARK_VERSION=3.5

ADD --chown=spark:spark https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar /opt/spark/jars

COPY run.sh /opt/spark/work-dir/
COPY collector/ /opt/spark/work-dir/collector/
COPY processors /opt/spark/work-dir/processors
COPY report /opt/spark/work-dir/report

USER root
RUN pip3 install -r /opt/spark/work-dir/collector/requirements.txt

USER spark

RUN mkdir -p /opt/spark/work-dir/src/fantasy/
RUN mkdir -p /opt/spark/work-dir/reports/