FROM flink:1.15.2

ENV JOB_JAR=flink-*-java-job-*.jar
ENV JOBS_DIR=/opt/flink/jobs/

RUN mkdir -p $JOBS_DIR && \
    chmod 777 $JOBS_DIR
COPY target/$JOB_JAR /opt/flink/jobs/
