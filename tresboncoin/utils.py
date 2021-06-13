# imports
import os
import joblib
import numpy as np
from termcolor import colored
from datetime import datetime

PATH_TO_LOCAL_MODEL = os.path.dirname(os.path.abspath(__file__)) + "/models/"


def get_model(model):
    return joblib.load(os.path.join(PATH_TO_LOCAL_MODEL, model + ".joblib"))


def custom_rmse(y_true, y_pred):
    return np.sqrt(np.mean(np.square(y_true - y_pred)))


def km_per_year(km, bike_year):
    if (datetime.now().year - bike_year) == 0:
        return km
    return km / (datetime.now().year - bike_year)


if __name__ == "__main__":

    model_ = get_model(model="model")

    print("\nModel loaded: " + colored(str(model_.best_estimator_[-1]).split("(")[0], "cyan"))

    for k, v in model_.best_params_.items():
        print(k, colored(v, "green"))
    print("\n")

