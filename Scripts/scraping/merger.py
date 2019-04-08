"""
This imports all the individual csv files starting with
a certain prefix, and dumps them to a single CSV file that
is one level up in the supplied directory of the collection
of CSV files to be merged.

Needs to be run from command line from Betting/ directory and
given argument of either 'full' or 'update'.
"""

import pandas as pd
import os


def merge(loc, prefix):
    """
    Creates a master data frame that merges all the individual CSVs.
    :param loc: the directory of the CSV files to be merged.
    :param prefix: prefix of collection of CSVs that need to be merged
    :return: master file as a pandas data frame
    """
    file_list = [file for file in os.listdir(loc) if not file.startswith('.') and file.startswith(prefix)]
    all_csv = []
    for file in file_list:
        all_csv.append(pd.read_csv(loc + file, index_col='Index', header=0))
    return pd.concat(all_csv, ignore_index=True, sort=False)
