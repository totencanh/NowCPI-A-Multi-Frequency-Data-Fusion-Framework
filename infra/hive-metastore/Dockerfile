# Stage 1: download JARs
FROM python:3.11-slim AS downloader
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /deps && \
    curl -fSL "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" -o /deps/mysql-connector-j-8.0.33.jar

# Stage 2: final image
FROM ilum/hive:3.1.3
ENV HIVE_VER=3.1.3
USER root
COPY --from=downloader /deps/mysql-connector-j-8.0.33.jar /opt/hive/lib/
COPY conf/hive-site.xml /opt/hive/conf/hive-site.xml
COPY conf/core-site.xml /opt/hive/conf/core-site.xml
COPY entrypoint.sh /entrypoint.sh
RUN chmod 644 /opt/hive/lib/mysql-connector-j-8.0.33.jar \
    /opt/hive/conf/hive-site.xml \
    /opt/hive/conf/core-site.xml && \
    chmod +x /entrypoint.sh
USER hive
ENTRYPOINT ["/entrypoint.sh"]
