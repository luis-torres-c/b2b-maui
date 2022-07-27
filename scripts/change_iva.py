import csv
import os
import sys

# True if value * 1.19, False if / 1.19
ADD_IVA = True

storage_path = os.environ.get('STORAGE_PATH', '')
metric_folder = 'metrics'

metrics = []

if '-h' in sys.argv:
    print("")
    print(" - To add IVA to values in metric sales")
    print("python change_iva.py -m sales")
    print("")
    print(" - to remove IVA from values in metrics sales and stocks")
    print("python change_iva.py -SIN_IVA -m sales stocks")
    print("")
    exit()

if '-m' in sys.argv:
    metrics = sys.argv[sys.argv.index('-m') + 1:]
else:
    print("No -m parameter added")
    exit()

if len(metrics) == 0:
    print("No metrics to change")
    exit()

if '-SIN_IVA' in sys.argv:
    ADD_IVA = False

all_processed_files = []
for metric in metrics:
    folder = os.path.join(storage_path, metric_folder, metric)
    for filename in os.listdir(folder):
        filename_path = os.path.join(folder, filename)
        all_processed_files.append(filename_path)
        header = []
        with open(filename_path) as csvfile:
            reader = csv.DictReader(csvfile)
            with open(filename_path + '1', 'w', newline='') as csvwfile:
                get_header = False
                for row in reader:
                    if not get_header:
                        header = list(row.keys())
                        writer = csv.DictWriter(csvwfile, fieldnames=header)
                        writer.writeheader()
                        get_header = True
                    newrow = {}
                    for param in header:
                        if param == 'value':
                            if ADD_IVA:
                                newrow[param] = float(row[param]) * 1.19
                            else:
                                newrow[param] = float(row[param]) / 1.19
                        else:
                            newrow[param] = row[param]
                    writer.writerow(newrow)

for fn_path in all_processed_files:
    os.rename(fn_path + '1', fn_path)
