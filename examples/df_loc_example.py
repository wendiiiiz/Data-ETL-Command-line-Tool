import numpy as np
import pandas as pd
import sys


def main(argv):

    try:
        df = pd.DataFrame({'Brand': ['BMW', 'TOYOTA', 'NISSAN', 'RENAULT', 'BMW'],
                           'Year': [2012, 2014, 2011, 2015, 2019],
                           'Driven': [60000, 55000, 30000, 31000, 10000],
                           'City': ['New York', 'Miami', 'Chicago', 'London', 'London'],
                           'Mileage': [28, 27, 25, 21, 15] })

        # Selecting data according to some condition.
        # Construct condition with 'BMW' brand and mileage > 20
        condition_bmw = (df.Brand == 'BMW') & (df.Mileage > 20)
        df_bmw = df.loc[(condition_bmw)]

        # Selecting data according to a range of rows.
        df_range = df.loc[2:len(df.index)]

        # Update the value of any column.
        # Construct condition with 'Year' < 2015
        condition_year = (df.Year < 2015)
        df_year = df.loc[condition_year]
        df.loc[condition_year, 'Mileage'] = 30

        # Selecting rows using integer indices.
        df_iloc_select = df.iloc[[0, 2, 4]]

        # Selecting rows from 1 to 2 and columns from 2 to 3.
        df_iloc_select2 = df.iloc[1:3, 2:4]

        print("Exit")
    except Exception as gen_exc:
        print(gen_exc)


if __name__ == '__main__':
    # Call main process.
    sys.exit(main(sys.argv[1:]))