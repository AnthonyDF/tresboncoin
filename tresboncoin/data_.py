from tresboncoin.utils import km_per_year
from tresboncoin.utils import set_brand_and_model
from tresboncoin.utils import set_colums
from tresboncoin.parameters import concatenation_map
from tresboncoin.parameters import columns_to_keep
from tresboncoin.concatenate import concat_df
from tresboncoin.cleaning import clean_concatenated_data
from tresboncoin.cleaning import clean_data_before_ml
from tresboncoin.fuzzy_match import fuzzy_match_model, fuzzy_match_brand

from datetime import datetime
import pandas as pd
import numpy as np
import string
import os
from termcolor import colored

from google.cloud import storage

# Raw data file path
raw_data_local = os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_data.csv"
raw_data_gs = "gs://tresboncoin/tresboncoin/data/master/master_data.csv"

# History data file path
history_data_local = os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_with_fuzzy_and_cleaning.csv"
history_data_gs = "gs://tresboncoin/tresboncoin/data/master/master_with_fuzzy_and_cleaning.csv"

# reference database file path
moto_database = os.path.dirname(os.path.abspath(__file__)) + "/data/master_vehicule_list/bikez.csv"


def get_raw_data(read_method='local'):
    """
     returns data from conctenation after scraping and cleaning

    Write:
        local: only local save
        both: local and google storage backup
    """
    if read_method == 'local':
        df = pd.read_csv(raw_data_local)

    elif read_method == 'gs':
        df = pd.read_csv(raw_data_gs)

    return df


def get_history_data(read_method='local'):
    """
     returns history data with cleaning and fuzzy matching

    Write:
        local: only local save
        both: local and google storage backup
    """
    if read_method == 'local':
        df = pd.read_csv(history_data_local)

    elif read_method == 'gs':
        df = pd.read_csv(history_data_gs)

    return df


def get_motorcycle_db():
    """
    returns motorcycle database
    """
    return pd.read_csv(moto_database)


def get_new_data(master_data, history_data):
    """
    function to return new rows in a dataframe compared to the history
    """
    history_data['checker'] = 1
    new_data = master_data.merge(history_data[['uniq_id', 'checker']], how='left', left_on='uniq_id',
                                 right_on='uniq_id')
    new_data = new_data[new_data.checker.isnull()]
    new_data.drop(columns=['checker'], inplace=True)
    return new_data


def append(new_data_matched, history_data, write_method='local'):
    new_history = history_data.append(new_data_matched)

    if write_method == 'local':
        # saving to local directory
        new_history.to_csv(history_data_local, index=False)
        print(colored(
            "Fuzzy matched dataset saved as master/master_with_fuzzy_and_cleaning.csv. Shape: " + str(
                new_history.shape),
            "green"))
    else:
        # saving to local directory
        new_history.to_csv(history_data_local, index=False)
        print(colored(
            "Fuzzy matched dataset saved as master/master_with_fuzzy_and_cleaning.csv. Shape: " + str(
                new_history.shape),
            "green"))

        # saving to Google Cloud Storage as a backup
        client = storage.Client()
        bucket = client.bucket("tresboncoin")
        blob = bucket.blob("tresboncoin/data/master/master_with_fuzzy_and_cleaning.csv")
        blob.upload_from_filename(history_data_local)
        print(colored(
            "master_with_fuzzy_and_cleaning.csv backup is on Google Cloud Storage. Shape: " + str(new_history.shape),
            "green"))

    return new_history


if __name__ == '__main__':
    # settings
    read_method = 'gs'  # or 'gs' or 'local'
    write_method = 'both'  # or 'both' or 'local'
    fuzzy_match_method = 'new'  # 'new' or 'all'

    # concatenate scraping outputs
    concat_df(read_method=read_method, write_method=write_method)

    # clean raw data after concatenation
    clean_concatenated_data(read_method=read_method, write_method=write_method)

    # fuzzy matching brand and model
    if fuzzy_match_method == 'all':
        new_data = get_raw_data(read_method=read_method)
        print("New data to be matched. Shape:" + str(new_data.shape))
        print('Fuzzy match in progress, wait...')
        new_data_matched_brand = fuzzy_match_brand(new_data, get_motorcycle_db())
        new_data_matched = fuzzy_match_model(new_data_matched_brand, get_motorcycle_db())
        print("Fuzzy match completed. Shape:" + str(new_data_matched.shape))
        # saving
        new_history = new_data_matched

        if write_method == 'local':
            # saving to local directory
            new_history.to_csv(history_data_local, index=False)
            print(colored(
                "Fuzzy matched dataset saved as master/master_with_fuzzy_and_cleaning.csv. Shape: " + str(
                    new_history.shape),
                "green"))
        else:
            # saving to local directory
            new_history.to_csv(history_data_local, index=False)
            print(colored(
                "Fuzzy matched dataset saved as master/master_with_fuzzy_and_cleaning.csv. Shape: " + str(
                    new_history.shape),
                "green"))

            # saving to Google Cloud Storage as a backup
            client = storage.Client()
            bucket = client.bucket("tresboncoin")
            blob = bucket.blob("tresboncoin/data/master/master_with_fuzzy_and_cleaning.csv")
            blob.upload_from_filename(history_data_local)
            print(colored("master_with_fuzzy_and_cleaning.csv backup is on Google Cloud Storage. Shape: " + str(
                new_history.shape), "green"))

    elif fuzzy_match_method == 'new':
        new_data = get_new_data(get_raw_data(read_method=read_method), get_history_data(read_method=read_method))

        print("New data to be matched. Shape:" + str(new_data.shape))
        if not new_data.empty:
            print('Fuzzy match in progress, wait...')
            new_data_matched_brand = fuzzy_match_brand(new_data, get_motorcycle_db())
            new_data_matched = fuzzy_match_model(new_data_matched_brand, get_motorcycle_db())
            print("Fuzzy match completed. Shape:" + str(new_data_matched.shape))
            history = append(new_data_matched, get_history_data(read_method=read_method))
            print("New dataframe available. Shape:" + str(history.shape))
        else:
            print('No new data to match')
    df_train = get_history_data(read_method=read_method)
    df_train = clean_data_before_ml(df_train)
    print("Train dataframe loaded. Shape:" + str(df_train.shape))