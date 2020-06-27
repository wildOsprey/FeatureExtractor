def calculate_df_zscore(df):
    df.columns = [x + "_zscore" for x in df.columns.tolist()]
    df = ((df - df.mean())/df.std(ddof=0))
    return df


def calculate_df_zscore_filter(df):
    cols = list(df.columns)
    cols = [col for col in cols if 'max_feature' not in col]
    for col in cols:
        col_zscore = col + '_zscore'
        df[col] = (df[col] - df[col].mean())/df[col].std(ddof=0)
        df.rename(columns={col: col_zscore}, inplace=True)
    return df


def process_column(df):
    df = (df - df.mean())/df.std(ddof=0)
    return df


def calculate_paralel_df_zscore_filter(df):
    import multiprocessing as mp
    import pandas as pd

    pool = mp.Pool(mp.cpu_count()-3)

    all_cols = list(df.columns)
    cols = [col for col in all_cols if col.startswith('feature_')]
    work_args = [df[col] for col in cols]

    results = pool.map(process_column, work_args)
    data = pd.concat(results, axis=1)
    pool.close()
    pool.join()

    return data


def calculate_db_zscore(columns, cursor):
    for col in columns:
        col = col[1]
        cursor.execute(f"alter table job add {col}_stand INT;")

        cursor.execute(f'''
        update job as j
        set {col}_stand=(
            select
            (t.{col} - tt.col_avr) / tt.col_std as {col}_stand
            from job as t
            cross join (
                select
                    avg({col}) as col_avr,
                    avg({col}*{col}) - avg({col})*avg({col}) as col_std
                from job as tt
            ) as tt
            where t.id_job=j.id_job
        )
        ''')
