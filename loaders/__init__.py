from .db_loader import DBLoader
from .df_loader import DFLoader


def get_loader(**kwargs):
    loader_type = kwargs.pop('processing_method')
    if loader_type == 'db':
        return DBLoader(**kwargs)
    elif loader_type == 'pandas':
        return DFLoader(**kwargs)
    else:
        raise Exception(f'''
            {loader_type} is not supported.
            Please choose from: db, pandas.
        ''')
