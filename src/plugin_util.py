from datetime import date


def add_datetime_into_comments_plugin(df):
    """
    Add current datetime stamp into COMMENTS dataframe column.
    :param df: pd.DataFrame; Provided dataframe
    :return: null
    """
    if "COMMENTS" in df:
        df["COMMENTS"] = f'{date.today()} ' + df["COMMENTS"]

    return df
