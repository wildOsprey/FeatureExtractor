import pandas as pd

from .abstract_loader import AbstractLoader
from utils.decorators import print_time
from utils.normalization import calculate_paralel_df_zscore_filter


class DFLoader(AbstractLoader):
    def __init__(self, path, sep, norm_function):
        """
        Init data loader.

        Args:
            path: str - Path to the file.
            sep: str – Separator for data in the file.
            norm_function: str – Function name for normalization.
        """
        self.path = path
        self.sep = sep
        self.norm_function = self._get_norm_function(norm_function)

    @print_time
    def load_data(self):
        '''Load data from file.

        Loading data from file with predefined path and separator.
        '''
        self.df = pd.read_csv(
            self.path,
            sep=self.sep,
            dtype={"id_job": int, "features": str}
        )

    @print_time
    def extract_features(self):
        """Extract normalized values.

        Extract normalized values with predefined function
        and max values: max index and abs mean diff.

        Values are processed based on their feature code.
        """
        norm_df = self._split_df()
        feature_sets = set(norm_df['code'].tolist())

        stand_results = []

        max_df = self._process_max_functions(feature_sets, norm_df)

        for features_code in feature_sets:
            data = self._process_feature_set(features_code, norm_df)
            stand_results.append(data)
        stand_results.append(max_df)

        self.processed_df = pd.concat(stand_results, sort=False)

    @print_time
    def export(self, filename):
        """
        Export extracted features to file.

        Args:
            filename: str - Path to the file.
        """
        self.processed_df['id_job'] = self.df['id_job']
        self.processed_df.to_csv(filename, sep=self.sep)

    def _get_max_ind(self, features):
        features = features.loc[features.index.str.startswith('feature_')].tolist()
        return int(features.index(max(features)))

    def _get_abs_mean_diff(self, features):
        features = features.loc[features.index.str.startswith('feature_')].tolist()
        return abs((sum(features) / len(features)) - max(features))

    def _get_norm_function(self, method):
        if method == 'zscore':
            return calculate_paralel_df_zscore_filter
        else:
            raise Exception(f'''
            {method} is not supported.
            Please choose from: zscore.
        ''')

    def _process_max_functions(self, feature_sets, norm_df):
        max_df = pd.DataFrame()

        for features_code in feature_sets:
            max_df[f'max_feature_{features_code}_index'] = norm_df.apply(
                lambda x: self._get_max_ind(x)
                if int(x['code']) == features_code else None,
                axis=1
            )

            max_df[f'max_feature_{features_code}_abs_mean_diff'] = norm_df.apply(
                lambda x: self._get_abs_mean_diff(x)
                if int(x['code']) == features_code else None,
                axis=1
            )
        return max_df

    def _process_feature_set(self, features_code, norm_df):
        data = self.norm_function(norm_df[norm_df['code'] == features_code])
        current_columns = [col for col in data.columns.tolist() if str(col).startswith('feature_')]
        rename_columns  = [f"feature_{features_code}_{int(col.split('_')[1])-1}_stand" for col in current_columns]
        columns_to_rename = dict(zip(current_columns, rename_columns))
        data.rename(columns=columns_to_rename, inplace=True)
        return data

    def _split_df(self):
        norm_df = self.df['features'].str \
            .split(',', expand=True).add_prefix('feature_')

        norm_df.rename(columns={'feature_0': 'code'}, inplace=True)
        norm_df = norm_df.astype(int)
        return norm_df
