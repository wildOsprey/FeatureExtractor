import argparse

from loaders import get_loader


def process_args(parser_args):
    args = vars(parser_args)
    if args['processing_method'] == 'pandas':
        del args['host']
    return args


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
            "--path",
            default=None,
            type=str,
            required=True,
            help="The input data dir. Should contain the path to .tsv file.",
        )

    parser.add_argument(
        "--processing_method",
        type=str,
        default='pandas',
        help="Method for loading data. Options: db – use database, pandas – use dataframe."
    )

    parser.add_argument("--sep", type=str, default='\t', help="Separator for file processing.")
    parser.add_argument("--output_file", type=str, default="test_proc.tsv", help="Name or full path for output data.")
    parser.add_argument("--norm_function", type=str, default='zscore', help="Function for processing.")
    parser.add_argument("--host", type=str, default='zscore', help="Optional. Path to SQLite DB. Example: sqlite:///your_filename.db. By default: :memory:")

    args = process_args(parser.parse_args())

    output_file = args.pop('output_file')

    loader = get_loader(**args)
    loader.load_data()
    loader.extract_features()
    loader.export(output_file)



if __name__ == "__main__":
    main()