#!/usr/bin/env python3

import sys
import subprocess
import time
import os

def measure_time(script_path):
    # Check if the script exists and is executable
    if not os.path.isfile(script_path):
        print(f"Error: {script_path} does not exist.")
        sys.exit(1)
    if not os.access(script_path, os.X_OK):
        print(f"Error: {script_path} is not executable.")
        sys.exit(1)

    # Measure execution time
    start_time = time.perf_counter()
    try:
        subprocess.run([script_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(f"Error: {script_path} exited with a non-zero status.")
        sys.exit(1)
    end_time = time.perf_counter()
    duration = end_time - start_time
    return duration

def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} script1 script2")
        sys.exit(1)

    script1 = sys.argv[1]
    script2 = sys.argv[2]

    print(f"Measuring execution time for {script1}...")
    time1 = measure_time(script1)
    print(f"Execution time for {script1}: {time1:.6f} seconds\n")

    print(f"Measuring execution time for {script2}...")
    time2 = measure_time(script2)
    print(f"Execution time for {script2}: {time2:.6f} seconds\n")

    # Compare execution times
    if time1 < time2:
        diff = time2 - time1
        ratio = time2 / time1 if time1 != 0 else float('inf')
        print(f"{script1} is faster than {script2} by {diff:.6f} seconds.")
        print(f"{script1} is {ratio:.2f} times faster than {script2}.")
    elif time1 > time2:
        diff = time1 - time2
        ratio = time1 / time2 if time2 != 0 else float('inf')
        print(f"{script2} is faster than {script1} by {diff:.6f} seconds.")
        print(f"{script2} is {ratio:.2f} times faster than {script1}.")
    else:
        print(f"{script1} and {script2} have the same execution time.")

if __name__ == "__main__":
    main()
