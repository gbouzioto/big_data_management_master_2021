import argparse
import csv
import os
import time
from collections import defaultdict

ATHLETES_FILENAME = os.path.join('data', 'athlete_events.csv')
NOC_FILENAME = os.path.join('data', 'noc_regions.csv')
OUTPUT_PATH = os.path.join(os.pardir, 'java_app', 'input', 'athlete_events_cleaned.csv')
HELP_TEXT = '''
Options about the script, the view option parses the file and does a report about the NULL values,
while clean parses the data and replaces the team names with their respective region they belong too.
'''


def _parse_user_args():
    """
    Parses the user arguments
    :returns: user arguments
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-a', '--athletes', help="filename containing the atheltes data", default=ATHLETES_FILENAME)
    arg_parser.add_argument('-n', '--noc', help="filename containing NOC regional data", default=NOC_FILENAME)
    arg_parser.add_argument('-o', '--options', help=HELP_TEXT, default="view", choices=["view", "clean"])
    arg_parser.add_argument('-out', '--output', help="output path", default=OUTPUT_PATH)
    return arg_parser.parse_args()


class Athlete:
    """Athlete Class"""

    def __init__(self):
        """Constructor"""
        self.attributes = {}

    @classmethod
    def create(cls, row):
        """Creates an Athlete from a row"""
        athlete = cls()
        athlete.attributes.update(row)
        return athlete

    def get_null_columns(self):
        """checks for null columns and returns their key names in a set"""
        return {key for key in self.attributes.keys() if self.attributes[key] == 'NA'}


class FileParser:
    """FileParser Class"""

    def __init__(self, filepath):
        """Constructor"""
        self._filepath = filepath
        self._null_columns = defaultdict(int)
        self._row_count = 0
        self._unique_teams = set()
        self._unique_countries = set()
        self._noc_mapping = {}
        self._notes_mapping = {}

    def parse_csv(self):
        """Parses the csv file and gathers useful info"""
        with open(self._filepath) as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                self._row_count += 1
                athlete = Athlete.create(row)
                self._unique_teams.add(athlete.attributes['Team'])
                self._unique_countries.add(athlete.attributes['NOC'])
                if null_columns := athlete.get_null_columns():
                    for null_column in null_columns:
                        self._null_columns[null_column] += 1

    def _build_noc_mapping(self, filepath):
        """Builds the NOC to region mapping"""
        with open(filepath) as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                noc = row['NOC']
                self._noc_mapping[noc] = row['region']
                if notes := row.get('notes'):
                    self._notes_mapping[notes] = noc

    def parse_csv_and_clean_data(self, output_path):
        """
        Parses the csv file, calculates the missing values in
        the dataset while creating a new clean csv file
        """
        with open(self._filepath) as f, open(output_path, 'w') as n:
            reader = csv.DictReader(f, delimiter=',')
            writer = csv.DictWriter(n, delimiter=',', fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                noc = row['NOC']
                if not(region := self._noc_mapping.get(noc)):
                    true_noc = self._notes_mapping[noc]
                    region = self._noc_mapping[true_noc]
                self._unique_teams.add(region)
                self._unique_countries.add(noc)
                row['Team'] = region
                writer.writerow(row)

    def export_null_stats(self):
        """Outputs the proportion of null values in the dataset"""
        for column_name, count in self._null_columns.items():
            percentage = round((count / self._row_count) * 100, 2)
            output = f'Column: {column_name} has {percentage}% missing values'
            print(output)
        print(f"Total number of rows: {self._row_count}")

    def export_unique_teams_countries(self):
        """Outputs the unique teams and countries"""
        print(f'Unique number of teams found: {len(self._unique_teams)}')
        print(f'Unique number of countries found: {len(self._unique_countries)}')

    @classmethod
    def create(cls, athletes_file, noc_file):
        """Creates a FileParser object"""
        file_parser = cls(athletes_file)
        file_parser._build_noc_mapping(noc_file)
        return file_parser


def main():
    """main function"""
    args = _parse_user_args()
    parser = FileParser.create(args.athletes, args.noc)
    if args.options == 'view':
        parser.parse_csv()
        parser.export_null_stats()
    else:
        parser.parse_csv_and_clean_data(args.output)
    parser.export_unique_teams_countries()


if '__main__' == __name__:
    start = time.time()
    main()
    end = time.time()
    print(f'\nExecution time in seconds: {round(end - start, 2)}')
