from tresboncoin.utils import km_per_year
from tresboncoin.utils import set_brand_and_model
from tresboncoin.utils import set_colums
from tresboncoin.utils import remove_punctuations
from tresboncoin.parameters import concatenation_map
from tresboncoin.parameters import columns_to_keep
from tresboncoin.concatenate import concat_df
from tresboncoin.concatenate import concat_df

from datetime import datetime
import pandas as pd
import numpy as np
import os
from termcolor import colored

from google.cloud import storage

# Raw data file path
raw_data_local = os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_data.csv"
raw_data_gs = "gs://tresboncoin/tresboncoin/data/master/master_data.csv"

# History data file path
history_data_local = os.path.dirname(os.path.abspath(__file__)) + "/data/master/master_with_fuzzy_and_cleaning.csv"
history_data_gs = "gs://tresboncoin/tresboncoin/data/master/master_with_fuzzy_and_cleaning.csv"


def clean_concatenated_data(write_method='local', read_method='local'):
    """
    Clean the dataframe after concatenation

    Write:
        local: only local save
        both: local and google storage backup
    Read:
        local: read csv file from local
        gs: read csv file from google storage
    """
    if read_method == 'local':
        df = pd.read_csv(raw_data_local)
    elif read_method == 'gs':
        df = pd.read_csv(raw_data_gs)

    print("Data size before cleaning: " + str(df.shape))

    # return clean dataframe
    # brand
    print('- count of rows with empty brand: ', df[df["brand"].isnull()].shape[0])
    df = df[~df["brand"].isnull()]

    # model
    print('- count of rows with empty model: ', df[df["model"].isnull()].shape[0])
    df = df[~df["model"].isnull()]

    # engine size
    print('- count of rows with empty engine_size: ', df[df["engine_size"].isnull()].shape[0])
    df = df[~df["engine_size"].isnull()]
    print('- count of rows with engine_size out of range',
          df[~((df["engine_size"] >= 49) & (df["engine_size"] < 2100))].isnull().shape[0])
    df = df[(df["engine_size"] >= 49) & (df["engine_size"] < 2100)]

    # bike year
    print('- count of rows bike_year not a number: ', df[df["bike_year"] == "['']"].shape[0])
    df = df[df["bike_year"] != "['']"]
    print('- count of rows with empty bike_year: ', df[df["bike_year"].isnull()].shape[0])
    df = df[~df["bike_year"].isnull()]
    df["bike_year"] = df["bike_year"].apply(lambda x: int(float(x)))
    df["bike_year"] = df["bike_year"].astype(int)
    df = df[(df["bike_year"] >= 1985) & (df["bike_year"] <= datetime.now().year)]

    # mileage
    print('- count of rows with mileage out of range: ',
          df[~((df["mileage"] >= 100) & (df["mileage"] <= 150000))].isnull().shape[0])
    df = df[((df["mileage"] >= 100) & (df["mileage"] <= 150000))]

    # price
    print('- count of rows with price out of range: ',
          df[~((df["price"] >= 100) & (df["price"] < 40000))].isnull().shape[0])
    df = df[(df["price"] >= 100) & (df["price"] < 40000)]

    # Clean same bike with multiple prices (keep lowest price)
    shape_before_duplicates = df.shape[0]
    df.sort_values('price', ascending=False, inplace=True)
    df.drop_duplicates(subset=['uniq_id'], keep='first', inplace=True)
    df.drop_duplicates(subset=['brand', 'model', 'bike_year', 'mileage'], keep='first', inplace=True)
    print('- count of duplicates removed: ', shape_before_duplicates - df.shape[0])

    print("Data size after cleaning: " + str(df.shape))

    # lower and remove spaces
    df.brand = df.brand.str.lower()
    df.model = df.model.str.lower()
    df.bike_type = df.bike_type.str.lower()
    # remove punctuation
    df.brand = df.brand.apply(remove_punctuations)
    df.model = df.model.apply(remove_punctuations)

    if write_method == 'local':
        # Saving to local directory
        df.to_csv(raw_data_local, index=False)
        print(colored("Concat dataset saved as master/master_data.csv. Shape: " + str(df.shape), "green"))

    elif write_method == 'both':
        # Saving to local directory
        df.to_csv(raw_data_local, index=False)
        print(colored("Concat dataset saved as master/master_data.csv. Shape: " + str(df.shape), "green"))
        # Saving to Google Cloud Storage as a backup
        client = storage.Client()
        bucket = client.bucket("tresboncoin")
        blob = bucket.blob("tresboncoin/data/master/master_data.csv")
        blob.upload_from_filename(raw_data_local)
        print(colored("master_data.csv backup is on Google Cloud Storage. Shape: " + str(df.shape), "green"))


def clean_data_before_ml(df):
    """
    return clean dataframe before machine leanring
    """

    # data size before
    print("Data size before cleaning: " + str(df.shape))

    # cleaning
    df = df.drop_duplicates()
    df = df[(df["bike_year"] >= (-df["bike_year"].std() * 3 + df["bike_year"].mean())) & (df["bike_year"] <= 2022)]
    df = df[(df["mileage"] >= 1000) & (df["mileage"] <= (df["mileage"].mean() + 3 * df["mileage"].std()))]
    df = df[(df["price"] >= 1000) & (df["price"] < (df["price"].mean() + 4 * df["price"].std()))]
    df = df[(df["engine_size"] >= 49) & (df["engine_size"] < (df["engine_size"].mean() + 3 * df["engine_size"].std()))]
    df = df[~df["category_db"].isnull()]
    df = df[df["brand_db"].isin(list(pd.DataFrame(df["brand_db"].value_counts())[0:50].index))]

    # feature engineering
    df['km/year'] = df.apply(lambda x: km_per_year(x['mileage'], x['bike_year']), axis=1)

    # remove duplicates
    df.drop_duplicates(subset=['model_db', 'brand_db', 'price', 'mileage', 'bike_year'], inplace=True)

    df = df[['brand_db', 'bike_year', 'mileage', 'engine_size', 'km/year', "price", "category_db"]]
    df.dropna(inplace=True)

    # data size after
    print("Data size after cleaning: " + str(df.shape))

    return df


if __name__ == '__main__':
    # concatenate scraping outputs
    concat_df(read_method='local', write_method='local')
    # concatenate scraping outputs
    clean_concatenated_data(read_method='local', write_method='local')
