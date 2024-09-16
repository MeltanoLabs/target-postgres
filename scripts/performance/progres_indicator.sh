#!/bin/bash

TOTAL_LINES=1000000
FILE="data.singer"

while true; do
    CURRENT_LINES=$(wc -l < "$FILE")
    PERCENT=$(( CURRENT_LINES * 100 / TOTAL_LINES ))
    echo -ne "Progress: $PERCENT% ($CURRENT_LINES/$TOTAL_LINES lines)\r"
    if [ "$CURRENT_LINES" -ge "$TOTAL_LINES" ]; then
        echo -e "\nDone!"
        break
    fi
    sleep 1  # Update every 5 seconds
done
