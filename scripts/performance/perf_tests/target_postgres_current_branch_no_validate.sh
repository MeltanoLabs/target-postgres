#!/bin/bash
export TARGET_POSTGRES_VALIDATE_RECORDS="false"
meltano invoke target-postgres < data.singer
