FROM apache/hive:4.0.1

ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/hive/lib/postgresql-42.7.4.jar
COPY entrypoint.sh /opt/hive/custom-entrypoint.sh

USER root
RUN chmod 644 /opt/hive/lib/postgresql-42.7.4.jar \
    && chmod +x /opt/hive/custom-entrypoint.sh
USER hive

ENTRYPOINT ["/opt/hive/custom-entrypoint.sh"]
