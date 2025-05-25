import csv
import os
import glob

def reverse_csv(input_file, output_file):
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = list(csv.reader(infile))
        header = reader[0]
        rows = reader[1:]
        rows.reverse()

    with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)
        writer.writerows(rows)

# Folder containing AIR CSV files
input_folder = 'Dataset'

# Find all matching files
csv_files = glob.glob(os.path.join(input_folder, 'EARTH*.csv'))

# Process each file
for file_path in csv_files:
    filename = os.path.basename(file_path)
    output_file = os.path.join(input_folder, f'reversed_{filename}')
    print(f"Processing {filename} -> reversed_{filename}")
    reverse_csv(file_path, output_file)

print("Done reversing all AIR*.csv files.")
