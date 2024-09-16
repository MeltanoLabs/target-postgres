#!/usr/bin/env python3

import os
import subprocess
import sys
import time


def measure_time(script_path):
    # Check if the script exists and is executable
    if not os.path.isfile(script_path):
        print(f"Error: {script_path} does not exist.")
        sys.exit(1)
    if not os.access(script_path, os.X_OK):
        print(f"Error: {script_path} is not executable.")
        sys.exit(1)

    # Measure execution time
    print(f"Measuring execution time for {script_path}...")
    start_time = time.perf_counter()
    try:
        subprocess.run([script_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(f"Error: {script_path} exited with a non-zero status.")
        sys.exit(1)
    end_time = time.perf_counter()
    duration = end_time - start_time
    print(f"Execution time for {script_path}: {duration:.6f} seconds\n")
    return duration

def main():
    perf_tests_dir = "./perf_tests"
    scripts = [test for test in os.listdir(perf_tests_dir) if os.path.isfile(os.path.join(perf_tests_dir, test))]
    for script in scripts:
        script_path = os.path.join("./perf_tests", script)
        measure_time(script_path)

if __name__ == "__main__":
    main()
