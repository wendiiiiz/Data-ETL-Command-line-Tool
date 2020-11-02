import numpy as np
import pandas as pd
import utils.file_util as fileu
import utils.misc_util as miscu
import datetime


def aggregate_feature(df, config):
    """
    ETL feature to aggregate given dataframe.
    :param df: pd.DataFrame; Provided dataframe
    :param config: dict; Provided feature configuration
    :return: df_target: pd.DataFrame; Resulted dataframe
    Sample:
    "aggregate"
    """
    if config:
        return df.groupby(miscu.eval_elem_mapping(config, "group_by")).agg(miscu.eval_elem_mapping(config, "agg")).reset_index()
    else:
        return df


def apply_dtype_feature(df, config):
    """
    ETL feature to apply data types to dataframe columns and limit columns to ones specified
    :param df: pd.DataFrame; Provided dataframe
    :param config: dict; Provided feature configuration
    :return: df_target: pd.DataFrame; Resulted dataframe
    Sample:
    "apply_dtype": {
        "INSURANCE_CODE": "str",
        "INSURANCE_AMOUNT": "float",
        "CLIENT_TYPE": "int"
    }
    """
    if config and isinstance(config, dict):
        for column_key, type_value in config.items():
            if column_key in df:
                # str type.
                if type_value is str or type_value == 'str':
                    df[column_key] = df[column_key].fillna('')
                    df[column_key] = df[column_key].astype(str)
                # int type.
                elif type_value is int or type_value == 'int':
                    df[column_key] = df[column_key].fillna(0)
                    df[column_key] = df[column_key].astype(int)
                # float type.
                elif type_value is float or type_value == 'float':
                    df[column_key] = df[column_key].fillna(0.0)
                    df[column_key] = df[column_key].astype(float)
                # TODO: Implement datetime.date type
                elif type_value is datetime.date or type_value == "datetime.date":
                    df[column_key] = df[column_key].fillna(datetime.date(1997, 2, 6))
                    df[column_key] = pd.to_datetime(df[column_key])
                    #df[column_key] = df[column_key].astype(datetime.date)
            else:
                raise KeyError(f'Column <{column_key}> is missing from given dataframe')

        # Limit dataframe to specified columns.
        df = df[list(config.keys())]
    return df


def assign_feature(df, config):
    """
    ETL feature to assign new columns to a given dataframe
    :param df: pd.DataFrame; Provided dataframe
    :param config: dict; Provided feature configuration
    :return: df_target: pd.DataFrame; Resulted dataframe
    """
    if not config:
        return df
    else:
        df_target = df
        length = len(df_target.index)
        config_assign = dict()

        # Assign new columns, using static values.
        config_assign_const = miscu.eval_elem_mapping(config, 'col_const')
        if config_assign_const and isinstance(config_assign_const, dict):
            config_assign.update(config_assign_const)

        # Assign new columns, using variable values.
        config_assign_var = miscu.eval_elem_mapping(config, 'col_var')
        if config_assign_var and isinstance(config_assign_var, dict):
            config_assign.update(config_assign_var)

        for col_name, col_value in config_assign.items():
            df_target[col_name] = [col_value] * length

        return df_target


def dupl_feature(df, config):
    """
    ETL feature to duplicate every row with an ability to change particular values.
    :param df: pd.DataFrame; Provided dataframe
    :param config: dict; Provided feature configuration
    :return: df_target: pd.DataFrame; Resulted dataframe
    """
    if not config:
        return df
    else:
        length = len(df.values)
        df_target = pd.DataFrame(np.repeat(df.values, [2] * length, axis=0),
                                 columns=df.columns.values)
        config_assign = miscu.eval_elem_mapping(config, 'col_const')
        for col_name, col_value in config_assign.items():
            df_target.loc[::2, col_name] = col_value

        return df_target


def mapping_feature(df, config):
    """
    ETL feature to merge given dataframe with extracted mapping dataframe
    :param df: pd.DataFrame; Provided dataframe
    :param config: dict; Provided feature configuration
    :return: df_target: pd.DataFrame; Resulted dataframe
    """
    df_mapping = read_feature(config['read'])
    df_target = pd.merge(df, df_mapping, how='left', left_index=True,
                         left_on=miscu.eval_elem_mapping(config, 'left_on'),
                         right_on=miscu.eval_elem_mapping(config, 'right_on'))
    df_target.drop(columns=miscu.eval_elem_mapping(config, 'right_on'), inplace=True)

    return df_target


def read_feature(config):
    """
    ETL feature to read a file, based on provided ETL configuration section
    This is a composite feature, since it can call apply_dtype_feature, if appropriate config section exists
    :param config: dict; Provided configuration mapping
    :return: pd.DataFrame; Resulted dataframe
    """
    df_target = fileu.read(description=miscu.eval_elem_mapping(config, 'description'),
                           path=miscu.eval_elem_mapping(config, 'path'),
                           file_type=miscu.eval_elem_mapping(config, 'file_type', default_value='excel'),
                           separator=miscu.eval_elem_mapping(config, 'separator', default_value=','),
                           skip_rows=miscu.eval_elem_mapping(config, 'skip_rows', default_value=0),
                           use_cols=miscu.eval_elem_mapping(config, 'use_cols'),
                           sheet_name=miscu.eval_elem_mapping(config, 'sheet_name', default_value=0))

    df_target.columns = df_target.columns.str.strip()

    # Call apply_dtype_feature, if appropriate config section exists
    apply_dtype_config = miscu.eval_elem_mapping(config, 'apply_dtype')
    if apply_dtype_config:
        df_target = apply_dtype_feature(df_target, apply_dtype_config)

    return df_target


def rearrange_feature(df, config):
    """
    ETL feature to rename and reorder columns of given dataframe.
    :param df: pd.DataFrame; Provided dataframe
    :param config: dict; Provided feature configuration
    :return: df_target: pd.DataFrame; Resulted dataframe
    """
    if not config:
        return df
    else:
        df_target = df

        # Rename columns.
        config_to_rename = miscu.eval_elem_mapping(config, 'col_rename')
        if config_to_rename and isinstance(config_to_rename, dict):
            df_target.rename(columns=config_to_rename, inplace=True)

        # Reorder columns.
        config_to_reorder = miscu.eval_elem_mapping(config, 'col_reorder')
        if config_to_reorder and isinstance(config_to_reorder, list):
            df_target = df_target.reindex(columns=config_to_reorder)

    return df_target


def write_feature(df, config):
    """
    ETL feature to write a dataset to a file, based on provided ETL configuration section
    :param df: pd.DataFrame; Provided dataframe
    :param config: dict; Provided feature configuration
    :return null
    """
    fileu.write(df=df,
                description=miscu.eval_elem_mapping(config, 'description'),
                path=miscu.eval_elem_mapping(config, 'path'),
                file_type=miscu.eval_elem_mapping(config, 'file_type', default_value='excel'),
                index=miscu.eval_elem_mapping(config, 'index'),
                separator=miscu.eval_elem_mapping(config, 'separator', default_value=','),
                mode="new")