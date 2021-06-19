# sklearn
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import RobustScaler
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.neighbors import KNeighborsRegressor


# others
from scipy import stats
import numpy as np
import pandas as pd

# MLFLOW PARAMETERS
MLFLOW_URI = "https://mlflow.lewagon.co/"
CUSTOMURI = ""
myname = "VictorBnnt"
EXPERIMENT_NAME = f"[FR] [Paris] [{myname}] TresBonCoin"

######################################################
# Random Forest Classifier model
######################################################
grid_ETR = {'model__max_depth': stats.randint(1, 1000),
            'model__min_samples_split': [2, 4, 6, 8, 10],
            "preprocessor__scaler": [StandardScaler(), RobustScaler(), MinMaxScaler()]
            }
params_ETR = {"random_grid_search": grid_ETR,
              "model": ExtraTreesRegressor()}
######################################################

######################################################
# KNeighborsRegressor Classifier model
######################################################
grid_KNR = {'model__weights': ['distance', 'uniform'],
            'model__algorithm': ['ball_tree', 'kd_tree'],
            'model__leaf_size': np.linspace(25, 40, 15),
            'model__p': [1, 2],
            "preprocessor__scaler": [StandardScaler(), RobustScaler(), MinMaxScaler()]
            }
params_KNR = {"random_grid_search": grid_KNR,
              "model": KNeighborsRegressor()}
######################################################








######################################################
# Parameters for datasets concatenation
######################################################
concatenation_map = pd.DataFrame(dict({"site_name": ["motoplanete", "moto-occasion", "autoscout24", "fulloccaz", "moto-selection", "autoscout24_BE"],
                                       "url": ["url", "url", "url", "url", "url", "url"],
                                       "uniq_id": ["uniq_id", "uniq_id", "uniq_id", "uniq_id", "uniq_id", "uniq_id"],
                                       "brand": ["Brand", "bike_brand", "Brand", "Brand", "bike_brand", "marque"],
                                       "bike_year": ["vehicle release date", "bike_year", "annee", "vehicle release date", "bike_year", "annee"],
                                       "mileage": ["mileage", "bike_km", "mileage", "mileage", "bike_km", "mileage"],
                                       "price": ["price", "price", "price", "price", "price", "price"],
                                       "bike_type": ["vehicle type", "bike_type", "carrosserie", "vehicle type", "bike_type", "carrosserie"],
                                       "model": ["Model", "bike_model", "Model", "Model", "bike_model", "model"],
                                       "engine_size": ["engine capacity [CC]", "bike_size", "cylindree", "engine capacity [CC]", "bike_size", "cylindree"],
                                       "date_scrapped": ["date_scrapped", "scrap_date", "date_scrapped", "date_scrapped", "scrap_date", "date_scrapped"]}))
#
df_ids = dict({"motoplanete": 0,
               "moto-occasion": 1,
               "autoscout24": 2,
               "fulloccaz": 3,
               "moto-selection": 4,
               "autoscout24_de": 5})
#
columns_to_keep = ["url", "uniq_id", "brand", "bike_year", "mileage", "bike_type", "price", "model", "engine_size", "date_scrapped"]
