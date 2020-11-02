import logging
import pandas as pd
import os
from pathlib import Path


def read(description, path, file_type='excel', separator=',', skip_rows=0, use_cols=None, sheet_name=0):
    """
    Read file, along with validating provided path.
    :param description: str; File description
    :param path: str; Fully qualified file name to read
    :param file_type: str, default='Excel'; Read type with possible values of 'csv' or 'excel'
    :param separator: str, default=','; Values separator
    :param skip_rows: int, default=0; Number of rows to skip
    :param use_cols: int, default=None; A list of columns to read (all others are discarded)
    :param sheet_name: int or str; default=0; A sheet name or index to read
    :return: pd.DataFrame; Resulted dataframe
    """
    df_target = None
    if validate_path(path):
        if file_type.lower() == 'csv':
            # Read csv based file.
            df_target = pd.read_csv(path, sep=separator, skiprows=skip_rows, usecols=use_cols)
        elif file_type.lower() == 'excel':
            # Read Excel based file.
            if len((pd.ExcelFile(path)).sheet_names) > 1:

                df_target = pd.read_excel(path, sep=separator, skiprows=skip_rows,
                                          usecol=use_cols, sheet_name=sheet_name)

            else:
                df_target = pd.read_excel(path, sep=separator, skiprows=skip_rows, usecol=use_cols)

    logging.info(f'{description} records <{len(df_target.index)}> were read from <{path}>')
    return df_target


def write(df, description, path, file_type, index, separator=',', mode='overwrite'):
    """
    Write file, along with validating provided path.
    :param df: pd.DataFrame; Provided dataframe
    :param description: str; File description
    :param path: str; Fully qualified file name to read
    :param file_type: str, default='Excel'; Read type with possible values of 'csv' or 'excel'
    :param index: bool; Index to write
    :param separator: str, default=','; Values separator
    :param mode: str; mode can be "new" or "overwrite"; default is "overwrite"
    :return: null
    """
    if validate_path(Path(path).parent, isfile=False):
        if file_type.lower() == 'csv':
            if mode == "overwrite":
                df.to_csv(path_or_buf=path, sep=separator, index=index)
                logging.info(
                    f'New {file_type} file generated from DataFrame by overwriting, description: {description}.')
            elif mode == "new":
                new_path = create_new_path(path)
                df.to_csv(path_or_buf=new_path, sep=separator, index=index)
                logging.info(
                    f'New {file_type} file generated from DataFrame by creating a new path, description: {description}.')
            else:
                logging.error(f'Mode can only be "overwrite" or "new". ')

        elif file_type.lower() == 'excel':
            if mode == "overwrite":
                df.to_excel(excel_writer=path, index=False)
                logging.info(
                    f'New {file_type} file generated from DataFrame by overwriting, description: {description}.')
            elif mode == "new":
                new_path = create_new_path(path)
                df.to_excel(excel_writer=new_path, index=False)
                logging.info(
                    f'New {file_type} file generated from DataFrame by creating a new path, description: {description}.')
            else:
                logging.error(f'Mode can only be "overwrite" or "new". ')
        else:
            logging.error(f'File type can only be "csv" or "excel. ')
    else:
        logging.error(f'Path validation failed: <{path}>')


def validate_path(path, isfile=True):
    """
    Validate provided path.
    :param path: Fully qualified file path
    :param isfile: boolean, is a directory or not. True means provided path is a file. False means it is not a file.
    :return: bool; Resulted validation; either true (aka is a file) or raise an exception
    """
    if not isfile:
        if not os.path.isdir(path):
            logging.error(f'Provided directory path is invalid: <{path}>')
            raise FileNotFoundError(f'Provided directory path is invalid: <{path}>')
    else:
        if not os.path.isfile(path):
            logging.error(f'Provided file path is invalid: <{path}>')
            raise FileNotFoundError(f'Provided file path is invalid: <{path}>')
    return True


def create_new_path(path):
    """
    :param path: str; new path needs to be created from this path
    :return: str, a new path name
    """
    try:
        validate_path(path)
        root = os.path.splitext(path)[0]
        ext = os.path.splitext(path)[1]
        counter = 1
        new_path = os.path.join("", "".join([root, "_", str(counter), ext]))
        while os.path.isfile(new_path):
            counter += 1
            new_path = os.path.join("", "".join([root, "_", str(counter), ext]))
        return new_path
    except FileNotFoundError:
        return path