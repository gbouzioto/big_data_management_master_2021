import os
import datetime
import csv
import json


class CSVToJson:
    """Converts csv to json"""

    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path

    def convert_to_json(self, output_path):
        """Converts the csv file to json"""
        result = []
        with open(self.csv_file_path) as csv_f:
            reader = csv.DictReader(csv_f)
            for row in reader:
                # since in mongo db we dont have a schema its ok to omit null values
                # hence we are deleting all the records with null values from the row
                keys_to_remove = []
                for key, value in row.items():
                    if not value:
                        keys_to_remove.append(key)
                for key in keys_to_remove:
                    del row[key]

                # Director
                if director := row.get('director'):
                    # split directors into a list so it can be json serialized
                    row['director'] = director.split(',')
                # Cast
                if cast := row.get('cast'):
                    # split cast into a list o it can be json serialized
                    row['cast'] = cast.split(',')
                # Country
                if country := row.get('country'):
                    # split country into a list o it can be json serialized
                    row['country'] = country.split(',')

                # Date added
                if date_added := row.get('date_added'):
                    # remove trailing and leading spaces
                    date_added = date_added.strip()
                    # convert date in format Y-m-d (example: 2019-08-09)
                    date_added = datetime.datetime.strptime(date_added, '%B %d, %Y').date()
                    row['date_added'] = date_added
                # Listed In
                if listed_in := row.get('listed_in'):
                    # split listed_in into a list o it can be json serialized
                    row['listed_in'] = listed_in.split(',')
                result.append(row)
        # write json file
        with open(output_path, 'w') as json_file:
            json_file.write((json.dumps(result, indent=4, default=str)))


def main():
    # Paths
    data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    filepath = os.path.join(data_path, 'netflix_titles.csv')
    output_path = os.path.join(data_path, 'netflix_titles.json')

    # initialize CSVTJson
    converter = CSVToJson(filepath)
    # convert to json
    converter.convert_to_json(output_path)


if __name__ == '__main__':
    main()
