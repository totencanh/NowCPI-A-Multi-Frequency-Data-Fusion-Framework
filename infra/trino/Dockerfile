FROM trinodb/trino:442

# The named trino_data volume is initialized from this directory on first use.
# Set ownership in the image so Trino can write there without a runtime chown.
USER root
RUN mkdir -p /var/trino/data && chown -R trino:trino /var/trino/data
USER trino
