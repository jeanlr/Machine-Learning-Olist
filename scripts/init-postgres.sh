#!/bin/bash
set -e

# Criar banco para o Metabase se não existir
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE IF NOT EXISTS metabase;
    GRANT ALL PRIVILEGES ON DATABASE metabase TO $POSTGRES_USER;
    
    CREATE DATABASE IF NOT EXISTS hive_metastore;
    GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO $POSTGRES_USER;
EOSQL