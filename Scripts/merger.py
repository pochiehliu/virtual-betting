"""
This imports all the individual csv files starting with
a certain prefix, and dumps them to a single CSV file that
is one level up in the supplied directory of the collection
of CSV files to be merged.
"""
import pandas as pd
import os
# directory of files to be merged
LOCATION = 'Data/bask_ref_csvs/'

# prefix of the files you want to merge
PREFIX = ['player', 'game']


def merge(loc, kind):
    """
    Creates a master data frame that merges all the individual CSVs.
    :param loc: the directory of the CSV files to be merged.
    :param kind: either 'player' or 'game', depending on which
                 CSV file types need to be merged.
    :return: master file as a pandas data frame
    """
    file_list = [file for file in os.listdir(loc) if not file.startswith(('.', 'full')) and file.startswith(kind)]
    all_csv = []
    for file in file_list:
        all_csv.append(pd.read_csv(loc + file, index_col='Index', header=0))
    return pd.concat(all_csv, ignore_index=True, sort=False)


def main():
    for kind in PREFIX:
        master = merge(LOCATION, kind)
        master.to_csv(LOCATION.split('/')[0] + '/full_' + kind + '.csv',
                      index_label='Index')


if __name__ == '__main__':
    main()

