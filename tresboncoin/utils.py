# imports
import os
import joblib
import numpy as np
import pandas as pd
from termcolor import colored
from datetime import datetime
from tresboncoin.parameters import df_ids
from google.cloud import storage
import joblib
from io import BytesIO
import subprocess

PATH_TO_LOCAL_MODEL = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/models/"


def get_model_cloud_storage():

    """ loading joblib file from google cloud storage """

    if os.path.isfile(os.path.join(PATH_TO_LOCAL_MODEL, "local_model.joblib")) is False:

        storage_client = storage.Client()
        bucket_name="tresboncoin"
        model_bucket='model.joblib'

        bucket = storage_client.get_bucket(bucket_name)

        #select bucket file
        blob = bucket.blob(model_bucket)

        #download blob into an in-memory file object
        model_file = BytesIO()
        blob.download_to_filename("local_model")

        # moving joblib into container localy
        subprocess.run(["mv", "local_model", "models/local_model.joblib"])

    #load into joblib
    return joblib.load(os.path.join(PATH_TO_LOCAL_MODEL, "local_model.joblib"))


def custom_rmse(y_true, y_pred):
    return np.sqrt(np.mean(np.square(y_true - y_pred)))


def km_per_year(km, bike_year):
    if (datetime.now().year - bike_year) == 0:
        return km
    return km / (datetime.now().year - bike_year)


def set_brand_and_model(df, feature_name, r=None):

    # init var
    brand_list = [np.nan] * df.shape[0]
    model_list = [np.nan] * df.shape[0]

    # set brands to lower
    df[feature_name] = df[feature_name].apply(lambda x: x.lower())

    # find brand
    for k in range(df.shape[0]):
        val = df[feature_name].iloc[k]
        for brand in r:
            if val.find(brand) >= 0:
                brand_list[k] = brand.strip()
                model_list[k] = val.replace(brand.strip(), "").strip()
    df["Brand"] = pd.Series(brand_list)
    df["Model"] = pd.Series(model_list)

    return df


def set_colums(df_, dict_, sitename_):
    site_id = df_ids[sitename_]
    old_columns = list(dict_.iloc[site_id])
    new_columns = list(dict_.iloc[site_id].index)

    fd_new_columns = []

    for k in df_.columns:
        if k in old_columns:
            line_id = old_columns.index(k)
            fd_new_columns.append(new_columns[line_id])
        else:
            fd_new_columns.append(k)
    return fd_new_columns


if __name__ == "__main__":

    model_ = get_model(model="model")

    print("\nModel loaded: " + colored(str(model_.best_estimator_[-1]).split("(")[0], "cyan"))

    for k, v in model_.best_params_.items():
        print(k, colored(v, "green"))
    print("\n")

