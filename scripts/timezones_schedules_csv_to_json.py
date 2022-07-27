import argparse
import csv
import json
import os

parser = argparse.ArgumentParser()
parser.add_argument('csv_file_path', type=str)
parser.add_argument('folder_destination', type=str)
args = parser.parse_args()


def run(file_path, destination):
    json_data = []
    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            new_row = {
                'store': row['object_id'],
                'timezone': row['timezone'],
                'schedules': row['schedule'],
            }
            json_data.append(new_row)

        with open(os.path.join(destination, 'timezones_schedules.json'), 'w') as json_file:
            json.dump(json_data, json_file)


if __name__ == '__main__':
    run(args.csv_file_path, args.folder_destination)
