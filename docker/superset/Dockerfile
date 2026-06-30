FROM apache/superset:4.1.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir \
    sqlalchemy-trino==0.5.0 \
    trino==0.336.0 \
    psycopg2-binary==2.9.10

COPY init_superset.sh /usr/local/bin/init_superset.sh
COPY patches/apply_onov8_branding.py /tmp/apply_onov8_branding.py
RUN python /tmp/apply_onov8_branding.py \
    && rm /tmp/apply_onov8_branding.py
RUN chmod +x /usr/local/bin/init_superset.sh

USER superset
