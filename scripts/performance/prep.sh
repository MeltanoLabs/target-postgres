#!/bin/bash
time python 1m_rows_generate.py
time meltano invoke tap-csv > data.singer

# Create initial table in postgres

#Spin up postgres instance
podman run -e POSTGRES_PASSWORD=postgres -p 5432:5432 -h postgres -d postgres

#Vars  We'd definietly want this as a meltano utility, just as POC right now
DB_NAME="postgres"
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_HOST="localhost"
DB_PORT="5432"
export PGPASSWORD=$DB_PASSWORD

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME <<EOF

-- Create the staging table
CREATE TABLE large_data (
    column_1 TEXT PRIMARY KEY,
    column_2 TEXT,
    column_3 TEXT,
    column_4 TEXT,
    column_5 TEXT,
    column_6 TEXT,
    column_7 TEXT,
    column_8 TEXT,
    column_9 TEXT,
    column_10 TEXT
);

EOF
