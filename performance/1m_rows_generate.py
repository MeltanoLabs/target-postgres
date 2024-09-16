import csv
import random
import string

# Number of rows and columns
num_rows = 1_000_000
num_columns = 10

# Generate random data for CSV
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Generate the CSV file
csv_filename = "data.csv"

with open(csv_filename, mode='w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    
    # Write header
    header = [f"column_{i+1}" for i in range(num_columns)]
    writer.writerow(header)
    
    # Write data rows
    for _ in range(num_rows):
        row = [random_string() for _ in range(num_columns)]
        writer.writerow(row)

print(f"CSV file '{csv_filename}' with {num_rows} rows and {num_columns} columns has been generated.")
