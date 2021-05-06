import csv
import time
from collections import defaultdict

FILENAME = 'athlete_events.csv'


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
        self.null_columns = defaultdict(int)
        self.row_count = 0

    def parse_csv(self):
        """Parses the csv file and calculates the missing values in the dataset"""
        with open(self._filepath) as f:
            reader = csv.DictReader(f, delimiter=',')
            for row in reader:
                self.row_count += 1
                athlete = Athlete.create(row)
                if null_columns := athlete.get_null_columns():
                    for null_column in null_columns:
                        self.null_columns[null_column] += 1

    def export_null_stats(self):
        """Outputs the proportion of null values in the dataset"""
        for column_name, count in self.null_columns.items():
            percentage = round((count / self.row_count) * 100, 2)
            output = f'Column: {column_name} has {percentage}% missing values'
            print(output)


def main():
    """main function"""
    parser = FileParser(FILENAME)
    parser.parse_csv()
    parser.export_null_stats()


if '__main__' == __name__:
    start = time.time()
    main()
    end = time.time()
    print(f'\nExecution time in seconds: {round(end - start, 2)}')
