#!/bin/sh
set -eu

for db in \
  bnpb_shard_1 \
  bnpb_shard_1_r1 \
  bnpb_shard_2 \
  bnpb_shard_2_r1 \
  bnpb_shard_3 \
  bnpb_shard_3_r1
do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE ${db}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${db}')\gexec
EOSQL
done
