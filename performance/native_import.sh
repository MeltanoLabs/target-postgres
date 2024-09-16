#!/bin/bash

# Variables
CSV_FILE="data.csv"
DB_NAME="postgres"
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_HOST="localhost"
DB_PORT="5432"

# Export the password to avoid being prompted
export PGPASSWORD=$DB_PASSWORD

# Execute COPY command to import the CSV into PostgreSQL
#psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\COPY large_data FROM '$CSV_FILE' CSV HEADER;"
# Begin transaction
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME <<EOF

-- Create the staging table
DROP TABLE IF EXISTS large_data_staging;
CREATE UNLOGGED TABLE large_data_staging (
    column_1 VARCHAR(255),
    column_2 VARCHAR(255),
    column_3 VARCHAR(255),
    column_4 VARCHAR(255),
    column_5 VARCHAR(255),
    column_6 VARCHAR(255),
    column_7 VARCHAR(255),
    column_8 VARCHAR(255),
    column_9 VARCHAR(255),
    column_10 VARCHAR(255)
);

-- Import data into the staging table
\COPY large_data_staging FROM '$CSV_FILE' CSV HEADER;

-- Upsert data into the main table
INSERT INTO large_data AS target
SELECT * FROM large_data_staging
ON CONFLICT (column_1) DO UPDATE SET
    column_2 = EXCLUDED.column_2,
    column_3 = EXCLUDED.column_3,
    column_4 = EXCLUDED.column_4,
    column_5 = EXCLUDED.column_5,
    column_6 = EXCLUDED.column_6,
    column_7 = EXCLUDED.column_7,
    column_8 = EXCLUDED.column_8,
    column_9 = EXCLUDED.column_9,
    column_10 = EXCLUDED.column_10;

EOF

echo "CSV file has been imported into the database with merge handling."