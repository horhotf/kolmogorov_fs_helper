import pyarrow
import psycopg2

from .db import db_status_uid

def calculate_directory_size(fs: pyarrow.fs.HadoopFileSystem, directory_path):
    """Рекурсивно вычисляет общий размер файлов в директории."""
    try:
        file_infos = fs.get_file_info(pyarrow.fs.FileSelector(directory_path, recursive=True))
        return sum(f.size for f in file_infos if f.is_file)
    except Exception as e:
        print(e)


def delete_oldest_folders(fs:pyarrow.fs.HadoopFileSystem, psycopg2_connection_pool: psycopg2.pool.SimpleConnectionPool, directory_path, max_size):
    """Удаляет самые старые папки из директории до тех пор, пока общий размер не окажется меньше max_size."""
    file_infos = fs.get_file_info(pyarrow.fs.FileSelector(directory_path, recursive=False))
    folders = [(f.path, f.mtime) for f in file_infos if f.is_dir]
    folders.sort(key=lambda x: x[1])  # Сортируем по времени изменения (mtime)

    total_size = calculate_directory_size(fs, directory_path)
    for folder, _ in folders:
        if total_size <= max_size:
            break
        # Удаляем папку
        fs.delete_dir(folder)
        db_status_uid(psycopg2_connection_pool, func="DELETED", file = folder)
        total_size = calculate_directory_size(fs, directory_path)