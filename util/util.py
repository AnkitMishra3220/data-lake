import os
import logging

logging.getLogger().setLevel(logging.INFO)


def get_directory_size(dir_path: str):
    dir_size = 0
    for path, dirs, files in os.walk(dir_path):
        for f in files:
            fp = os.path.join(path, f)
            file_size = os.path.getsize(fp)
            if file_size == 0:
                logging.info('Empty File :', str(f))
            else:
                dir_size += file_size
    return dir_size
