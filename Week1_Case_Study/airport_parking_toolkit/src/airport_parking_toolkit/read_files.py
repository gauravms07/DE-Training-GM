import csv

input_file = 'datasets\parking_zones.csv'
output_file = 'datasets\parking_zones.txt'

with open(input_file, mode='r', newline='', encoding='utf-8') as infile, \
     open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
    
    reader = csv.reader(infile)
    
    for row in reader:
        # Enclose each value in double quotes
        quoted_row = ['"{}"'.format(value) for value in row]
        # Join with commas and write to file
        outfile.write(','.join(quoted_row) + '\n')

print(f'Output written to {output_file}')
