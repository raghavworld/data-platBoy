#!/usr/bin/env bash
set -euo pipefail

cat > /opt/hive/conf/core-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>http://minio:9000</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>${MINIO_ROOT_USER}</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>${MINIO_ROOT_PASSWORD}</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
  </property>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
</configuration>
EOF

cp /opt/hive/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

exec /entrypoint.sh
