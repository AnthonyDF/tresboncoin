# imports
import os
import joblib
import numpy as np
import pandas as pd
from termcolor import colored
from datetime import datetime
from tresboncoin.parameters import df_ids
import pytz


PATH_TO_LOCAL_MODEL = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/models/"
PATH_TO_PARAM_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "parameterlog.csv")


def get_model():

    """ loading joblib file """
    return joblib.load(os.path.join(PATH_TO_LOCAL_MODEL, "model.joblib"))


def get_last_time_modified(file_path):
    """ return last time a file was modified """

    # file timestamp
    file_timestamp = datetime.fromtimestamp(os.path.getmtime(file_path))

    # set timezones
    machine_TZ = pytz.timezone("UTC")
    france_TZ = pytz.timezone("Europe/Paris")

    return machine_TZ.localize(file_timestamp).astimezone(france_TZ).strftime("%d/%m/%Y - %Hh%M")


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


def log_best_params(model):

    # loading parameters log file
    logfile = pd.read_csv(PATH_TO_PARAM_LOG)

    # parameters
    params = dict()

    # save the date
    params["date"] = [get_last_time_modified(PATH_TO_LOCAL_MODEL + "model.joblib")]

    # save estimator
    params["estimator"] = [str(model.estimator["model"])]

    # save parameters
    for k, v in model.best_params_.items():
        params[k] = [v]

    # save score
    params["rmse"] = [model.best_score_]

    # concat
    newlog = pd.concat([logfile, pd.DataFrame(params)], axis=0)

    # saving to logfile
    newlog.to_csv(PATH_TO_PARAM_LOG, index=False)
    return


if __name__ == "__main__":

    model_ = get_model(model="model")

    print("\nModel loaded: " + colored(str(model_.best_estimator_[-1]).split("(")[0], "cyan"))

    for k, v in model_.best_params_.items():
        print(k, colored(v, "green"))
    print("\n")

