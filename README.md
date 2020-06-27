## How to run

To run the script, you should run main.py with the following arguments:

1. path (Required) – Path to the tsv file. Example: ./data/train.tsv
2. processing_method (Optional) – You can either process the file using SQLite db, or using pandas DataFrames.  Options: db – use database, pandas – use dataframe. Default: pandas.
3. sep (Optional) – Separator str in the file. Default: \t.
4. output_file (Optional) – Name or full path for output data. Default: test_proc.tsv.
5. norm_function (Optional) – Function for processing. Default: zscore.
6. host (Optional) – Path to SQLite DB. Example: sqlite:///your_filename.db. By default: :memory:

Example command:
```
python3.7 main.py --path ./data/test.tsv --processing_method pandas
```

```
python3.7 main.py --path ./data/test.tsv --processing_method db --output_file test_db.tsv
```