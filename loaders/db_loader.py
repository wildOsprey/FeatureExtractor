import csv
import sqlite3

from .abstract_loader import AbstractLoader
from utils.decorators import print_time
from utils.normalization import calculate_db_zscore


class DBLoader(AbstractLoader):
    def __init__(self, path, sep, host=':memory:', norm_function='zscore'):
        self.path = path
        self.sep = sep
        self.host = host
        self.norm_function = self._get_norm_function(norm_function)

    @print_time
    def load_data(self):
        self.con = sqlite3.connect(self.host)
        self.cursor = self.con.cursor()
        self.cursor.execute("create table job (id_job INT, features);")

        with open(self.path, 'r') as fin:
            dr = csv.DictReader(fin, delimiter=self.sep)
            to_db = [(i['id_job'], i['features']) for i in dr]

        self.cursor.executemany('''
            insert into job (id_job, features)
            values (?, ?);''', to_db)

    @print_time
    def create_code_features(self, features_code, n_features):
        for i in range(n_features):
            self.cursor.execute(f"alter table job add feature_{features_code}_{i} INT;")
        self.cursor.execute(f"alter table job add max_feature_{features_code}_index INT;")
        self.cursor.execute(f"alter table job add max_feature_{features_code}_abs_mean_diff DOUBLE;")

    @print_time
    def extract_features(self):
        self.cursor.execute("select * from job")
        feature_rows = self.cursor.fetchall()

        created_column_ids = []

        for id_job, row in feature_rows:
            features_data = row.split(",")
            features_code, features = features_data[0], features_data[1:]
            curr_values = []

            if int(features_code) not in created_column_ids:
                self.create_code_features(int(features_code), len(features))
                created_column_ids.append(int(features_code))

            max_index = features.index(max(features))
            features = list(map(int, features))
            abs_max_diff = abs((sum(features) / len(features)) - max(features))

            curr_values = [f"feature_{features_code}_{i}={features[i]}" for i in range(len(features))]

            self.cursor.execute(f'''
                update job
                set {', '.join(curr_values)}
                where id_job={id_job}
                ''')

            self.cursor.execute(f'''
                update job
                set max_feature_{features_code}_index={max_index}, max_feature_{features_code}_abs_mean_diff={abs_max_diff}
                where id_job={id_job}
                ''')

        columns = self.cursor.execute(f'''pragma table_info(job)''').fetchall()

        self.norm_function(columns, self.cursor)

    @print_time
    def export(self, filename):
        columns = self.cursor.execute(f'''pragma table_info(job)''').fetchall()

        columns_to_export = [col[1] for col in columns
            if self._validate_column(col[1])]

        self._export_from_db(columns_to_export, filename)
        self.con.close()

    def _export_from_db(self, columns, filename):
        print(', '.join(columns))
        self.cursor.execute(f"select {', '.join(columns)} from job")
        rows = self.cursor.fetchall()

        with open(filename, "w+") as f:
            writer = csv.writer(f, delimiter=self.sep)
            writer.writerow(columns)
            writer.writerows(rows)

    def _validate_column(self, column):
        return 'stand' in column \
            or 'max' in column \
            or column == 'id_job'

    def _get_norm_function(self, method):
        if method == 'zscore':
            return calculate_db_zscore
        else:
            raise Exception(f'''
            {method} is not supported.
            Please choose from: zscore.
        ''')
