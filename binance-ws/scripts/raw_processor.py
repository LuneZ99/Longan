import os
import gzip
import pandas as pd


def replace_file_extension(file_path, new_extension):
    if '.' not in file_path:
        return file_path
    last_dot_index = file_path.rfind('.')
    file_name = file_path[:last_dot_index]
    new_file_path = f"{file_name}.{new_extension}"
    return new_file_path


def raw_csv_to_parquet(path):
    print(f"Converting CSV to parquet: {path}")
    df = pd.read_csv(path)
    df.to_parquet(replace_file_extension(path, 'parquet'))


def raw_log_to_gzip(path):
    compressed_file_path = path + '.gz'
    print(f"Compressing log file: {compressed_file_path}")
    with open(path, 'rb') as file_in:
        with gzip.open(compressed_file_path, 'wb') as file_out:
            file_out.writelines(file_in)
    os.remove(path)


def process_folder(raw_folder, log_file='log.txt'):

    need_to_process_days = [
        x.split('.')[-1] for x in os.listdir(raw_folder)
        if "txt" in x and not x.endswith("txt") and not x.endswith('gz')
    ]
    print(f'processing {need_to_process_days}')

    for day in need_to_process_days:

        raw_log_to_gzip(f"{raw_folder}/{log_file}.{day}")

        for dir_path, dir_names, filenames in os.walk(raw_folder):

            if day in dir_path:

                for filename in filenames:
                    file_path = os.path.join(dir_path, filename)
                    raw_csv_to_parquet(file_path)


if __name__ == '__main__':
    process_folder('../raw_folder')
