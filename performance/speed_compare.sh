#!/bin/bash

# Check if two arguments are given
if [ $# -ne 2 ]; then
    echo "Usage: $0 script1 script2"
    exit 1
fi

script1="$1"
script2="$2"

# Check if the scripts exist and are executable
if [ ! -x "$script1" ]; then
    echo "Error: $script1 does not exist or is not executable."
    exit 1
fi

if [ ! -x "$script2" ]; then
    echo "Error: $script2 does not exist or is not executable."
    exit 1
fi

# Function to measure execution time
measure_time() {
    local script="$1"
    local start_time end_time duration
    start_time=$(date +%s.%N)
    ./"$script"
    end_time=$(date +%s.%N)
    duration=$(awk "BEGIN {print $end_time - $start_time}")
    echo "$duration"
}

# Measure execution time for script1
echo "Measuring execution time for $script1..."
time1=$(measure_time "$script1")
echo "Execution time for $script1: $time1 seconds"

# Measure execution time for script2
echo "Measuring execution time for $script2..."
time2=$(measure_time "$script2")
echo "Execution time for $script2: $time2 seconds"

# Compare the execution times using awk
comparison=$(awk -v t1="$time1" -v t2="$time2" 'BEGIN { if (t1 < t2) print "script1_faster"; else if (t1 > t2) print "script2_faster"; else print "equal" }')

if [ "$comparison" = "script1_faster" ]; then
    diff=$(awk "BEGIN {print $time2 - $time1}")
    ratio=$(awk "BEGIN {print $time2 / $time1}")
    ratio_formatted=$(printf "%.2f" "$ratio")
    echo "$script1 is faster than $script2 by $diff seconds."
    echo "$script1 is $ratio_formatted times faster than $script2."
elif [ "$comparison" = "script2_faster" ]; then
    diff=$(awk "BEGIN {print $time1 - $time2}")
    ratio=$(awk "BEGIN {print $time1 / $time2}")
    ratio_formatted=$(printf "%.2f" "$ratio")
    echo "$script2 is faster than $script1 by $diff seconds."
    echo "$script2 is $ratio_formatted times faster than $script1."
else
    echo "$script1 and $script2 have the same execution time."
fi