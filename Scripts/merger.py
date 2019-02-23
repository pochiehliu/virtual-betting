"""
This imports all the individual csv files from every month and merges
them to a single 'player' and 'game' csv file.
"""
import pandas as pd
import os


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
    loc = 'Data/bask_ref_csvs/'
    master_player = merge(loc, 'player')
    master_game = merge(loc, 'game')
    master_player.to_csv('Data/full_player.csv', index_label='Index')
    master_game.to_csv('Data/full_game.csv', index_label='Index')


if __name__ == '__main__':
    main()

